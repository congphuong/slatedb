use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;
use slatedb::admin::{Admin, CloneSourceSpec};
use slatedb::config::{
    CloseOptions, DbReaderOptions, FlushOptions, FlushType, GarbageCollectorDirectoryOptions,
    GarbageCollectorOptions, Settings, WalFollowMode,
};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStore;
use slatedb::wal::{
    FlushResultFuture, WalAdmin, WalError, WalEvent, WalFileRange, WalGc, WalIterator, WalObserver,
    WalReader, WalRows, WalStatus, WalStatusListener, WalWriter, WriterInit, WriterInitResult,
    WriterManifest,
};
use slatedb::{Db, DbReader, DbReaderMode, GarbageCollectorBuilder, RowEntry, VersionedManifest};
use slatedb_common::SystemClock;

/// A deliberately small WAL implementation used to exercise the public pluggable-WAL API.
/// Every call to `WalWriter::append` inserts one write batch under a new WAL file ID.
#[derive(Clone, Default)]
struct BTreeMapWal {
    files: Arc<Mutex<BTreeMap<u64, Vec<RowEntry>>>>,
}

impl BTreeMapWal {
    fn file_ids(&self) -> Vec<u64> {
        self.files.lock().keys().copied().collect()
    }

    fn snapshot(&self) -> BTreeMap<u64, Vec<RowEntry>> {
        self.files.lock().clone()
    }

    fn open_status(&self) -> WalStatus {
        let files = self.files.lock();
        let last_flushed_wal_id = files.last_key_value().map(|(id, _)| *id).unwrap_or(0);
        let last_flushed_seq = files
            .last_key_value()
            .and_then(|(_, batch)| batch.iter().map(|row| row.seq).max());
        WalStatus {
            closed_reason: None,
            estimated_bytes: 0,
            last_flushed_wal_id,
            last_flushed_seq,
            buffered_wal_entries_count: 0,
        }
    }
}

struct BTreeMapWalIterator {
    batches: VecDeque<(u64, Vec<RowEntry>)>,
}

#[async_trait]
impl WalIterator for BTreeMapWalIterator {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        Ok(self.batches.pop_front().map(|(wal_file_id, rows)| WalRows {
            rows,
            last_consumed_wal_file_id: wal_file_id,
        }))
    }
}

#[async_trait]
impl WalReader for BTreeMapWal {
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError> {
        let WalFileRange(start, end) = wal_file_id_range;
        let batches = self
            .files
            .lock()
            .range((start, end))
            .map(|(id, batch)| (*id, batch.clone()))
            .collect();
        Ok(Box::new(BTreeMapWalIterator { batches }))
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        Ok(self
            .files
            .lock()
            .range((Bound::Excluded(replay_after_wal_id), Bound::Unbounded))
            .next_back()
            .map(|(id, _)| *id)
            .unwrap_or(replay_after_wal_id))
    }
}

struct ObserverState {
    status: WalStatus,
    listeners: Vec<WalStatusListener>,
}

#[derive(Clone)]
struct BTreeMapWalObserver {
    state: Arc<Mutex<ObserverState>>,
}

impl BTreeMapWalObserver {
    fn status(&self) -> Result<WalStatus, WalStatus> {
        let status = self.state.lock().status.clone();
        if status.closed_reason.is_some() {
            Err(status)
        } else {
            Ok(status)
        }
    }
}

impl WalObserver for BTreeMapWalObserver {
    fn status(&self) -> Result<WalStatus, WalStatus> {
        self.status()
    }

    fn subscribe(&self, listener: WalStatusListener) -> Result<(), WalError> {
        self.state.lock().listeners.push(listener);
        Ok(())
    }
}

struct BTreeMapWalWriter {
    wal: BTreeMapWal,
    observer: BTreeMapWalObserver,
}

impl BTreeMapWalWriter {
    fn new(wal: BTreeMapWal) -> Self {
        let observer = BTreeMapWalObserver {
            state: Arc::new(Mutex::new(ObserverState {
                status: wal.open_status(),
                listeners: Vec::new(),
            })),
        };
        Self { wal, observer }
    }

    fn publish(&self, event: WalEvent) {
        let listeners = self.observer.state.lock().listeners.clone();
        for listener in listeners {
            listener(event.clone());
        }
    }
}

#[async_trait]
impl WalWriter for BTreeMapWalWriter {
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError> {
        if self.observer.state.lock().status.closed_reason.is_some() {
            return Err(WalError::Closed);
        }

        let wal_file_id = {
            let mut files = self.wal.files.lock();
            let wal_file_id = match files.last_key_value() {
                Some((last_id, _)) => last_id.checked_add(1).ok_or_else(|| {
                    WalError::InternalError(Arc::new(std::io::Error::other("WAL file ID overflow")))
                })?,
                None => 1,
            };
            files.insert(wal_file_id, write_batch.to_vec());
            wal_file_id
        };

        let status = {
            let mut state = self.observer.state.lock();
            state.status.last_flushed_wal_id = wal_file_id;
            state.status.last_flushed_seq = write_batch.iter().map(|row| row.seq).max();
            state.status.clone()
        };
        self.publish(WalEvent::WalFlushed(status));
        Ok(())
    }

    async fn flush(&mut self) -> Result<FlushResultFuture, WalError> {
        Ok(Box::pin(async { Ok(()) }))
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        Box::new(self.observer.clone())
    }

    fn status(&self) -> Result<WalStatus, WalStatus> {
        self.observer.status()
    }

    async fn close(&mut self) -> Result<(), WalError> {
        let status = {
            let mut state = self.observer.state.lock();
            if state.status.closed_reason.is_some() {
                return Ok(());
            }
            state.status.closed_reason = Some(WalError::Closed);
            state.status.clone()
        };
        self.publish(WalEvent::WalClosed(status));
        Ok(())
    }
}

#[async_trait]
impl WriterInit for BTreeMapWal {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        let replay_after_wal_id = manifest.replay_after_wal_id();
        let start = replay_after_wal_id.checked_add(1).ok_or_else(|| {
            WalError::InternalError(Arc::new(std::io::Error::other("WAL replay range overflow")))
        })?;
        let end = self
            .last_wal_file_id(replay_after_wal_id)
            .await?
            .checked_add(1)
            .ok_or_else(|| {
                WalError::InternalError(Arc::new(std::io::Error::other(
                    "WAL replay range overflow",
                )))
            })?;
        let replay_iterator = self.iterator((start..end.max(start)).into()).await?;

        Ok(WriterInitResult {
            replay_iterator,
            wal_writer: Box::new(BTreeMapWalWriter::new(self.clone())),
        })
    }
}

fn range_contains(range: &WalFileRange, wal_file_id: u64) -> bool {
    let starts_before = match range.0 {
        Bound::Included(start) => wal_file_id >= start,
        Bound::Excluded(start) => wal_file_id > start,
        Bound::Unbounded => true,
    };
    let ends_after = match range.1 {
        Bound::Included(end) => wal_file_id <= end,
        Bound::Excluded(end) => wal_file_id < end,
        Bound::Unbounded => true,
    };
    starts_before && ends_after
}

#[async_trait]
impl WalGc for BTreeMapWal {
    async fn collect(
        &self,
        referenced_ranges: Vec<WalFileRange>,
        _min_age: Duration,
        dry_run: bool,
    ) -> Result<(), WalError> {
        if !dry_run {
            self.files.lock().retain(|wal_file_id, _| {
                referenced_ranges
                    .iter()
                    .any(|range| range_contains(range, *wal_file_id))
            });
        }
        Ok(())
    }
}

/// Supplies a separate `BTreeMapWal` for each database path so clone administration can copy
/// the source WAL into the clone's WAL namespace.
#[derive(Clone, Default)]
struct BTreeMapWalAdmin {
    wals: Arc<Mutex<BTreeMap<String, BTreeMapWal>>>,
}

impl BTreeMapWalAdmin {
    fn wal(&self, path: &Path) -> BTreeMapWal {
        self.wals
            .lock()
            .entry(path.to_string())
            .or_default()
            .clone()
    }
}

#[async_trait]
impl WalAdmin for BTreeMapWalAdmin {
    fn garbage_collector(&self, path: &Path) -> Arc<dyn WalGc> {
        Arc::new(self.wal(path))
    }

    async fn delete_wal(&self, path: &Path, dry_run: bool) -> Result<Vec<String>, WalError> {
        let key = path.to_string();
        let exists = self.wals.lock().contains_key(&key);
        if exists && !dry_run {
            self.wals.lock().remove(&key);
        }
        Ok(exists
            .then(|| format!("btree-map-wal:{key}"))
            .into_iter()
            .collect())
    }

    async fn is_empty(
        &self,
        path: &Path,
        _replay_after_wal_id: u64,
        _wal_id_last_seen: u64,
    ) -> Result<bool, WalError> {
        Ok(self.wal(path).files.lock().values().all(Vec::is_empty))
    }

    async fn clone_wal(
        &self,
        from_path: &Path,
        from_manifest: VersionedManifest,
        to_path: &Path,
    ) -> Result<(u64, u64), WalError> {
        let replay_after_wal_id = from_manifest.replay_after_wal_id();
        let copied = self
            .wal(from_path)
            .files
            .lock()
            .range((Bound::Excluded(replay_after_wal_id), Bound::Unbounded))
            .map(|(id, batch)| (*id, batch.clone()))
            .collect::<BTreeMap<_, _>>();
        let last_wal_file_id = copied
            .last_key_value()
            .map(|(id, _)| *id)
            .unwrap_or(replay_after_wal_id);
        *self.wal(to_path).files.lock() = copied;
        Ok((replay_after_wal_id, last_wal_file_id))
    }
}

fn test_settings() -> Settings {
    Settings {
        flush_interval: None,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    }
}

async fn open_db(path: Path, object_store: Arc<dyn ObjectStore>, wal: BTreeMapWal) -> Db {
    Db::builder(path, object_store)
        .with_settings(test_settings())
        .with_wal_writer(Box::new(wal))
        .build()
        .await
        .expect("failed to open database with custom WAL")
}

async fn close_without_memtable_flush(db: &Db) {
    db.close_with_options(CloseOptions::default().with_flush_type(None))
        .await
        .expect("failed to close database")
}

#[tokio::test]
async fn custom_wal_basic_write_and_recovery() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/basic-recovery");
    let wal = BTreeMapWal::default();

    let db = open_db(path.clone(), Arc::clone(&object_store), wal.clone()).await;
    db.put(b"key-1", b"value-1").await.expect("put failed");
    db.put(b"key-2", b"value-2").await.expect("put failed");

    assert_eq!(wal.file_ids(), vec![1, 2]);
    assert!(wal.snapshot().values().all(|batch| batch.len() == 1));
    close_without_memtable_flush(&db).await;

    let recovered = open_db(path, object_store, wal).await;
    assert_eq!(
        recovered
            .get(b"key-1")
            .await
            .expect("get failed")
            .as_deref(),
        Some(b"value-1".as_slice())
    );
    assert_eq!(
        recovered
            .get(b"key-2")
            .await
            .expect("get failed")
            .as_deref(),
        Some(b"value-2".as_slice())
    );
    close_without_memtable_flush(&recovered).await;
}

#[tokio::test]
async fn custom_wal_db_reader_serves_wal_data() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/db-reader");
    let wal = BTreeMapWal::default();
    let db = open_db(path.clone(), Arc::clone(&object_store), wal.clone()).await;
    db.put(b"reader-key", b"reader-value")
        .await
        .expect("put failed");

    let reader = DbReader::builder(path, object_store)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(DbReaderOptions {
            manifest_poll_interval: Duration::from_secs(60),
            ..DbReaderOptions::default()
        })
        .with_wal_reader(Arc::new(wal))
        .build()
        .await
        .expect("failed to open database reader");
    assert_eq!(
        reader
            .get(b"reader-key")
            .await
            .expect("reader get failed")
            .as_deref(),
        Some(b"reader-value".as_slice())
    );

    reader.close().await.expect("failed to close reader");
    close_without_memtable_flush(&db).await;
}

#[tokio::test]
async fn custom_wal_garbage_collects_unused_ranges() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/gc");
    let wal = BTreeMapWal::default();
    let db = open_db(path.clone(), Arc::clone(&object_store), wal.clone()).await;

    for id in 1..=3 {
        db.put(
            format!("key-{id}").as_bytes(),
            format!("value-{id}").as_bytes(),
        )
        .await
        .expect("put failed");
    }
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("memtable flush failed");
    db.put(b"key-4", b"value-4").await.expect("put failed");
    close_without_memtable_flush(&db).await;
    assert_eq!(wal.file_ids(), vec![1, 2, 3, 4]);

    let gc = GarbageCollectorBuilder::new(path, object_store)
        .with_wal_gc(Arc::new(wal.clone()))
        .with_options(GarbageCollectorOptions {
            manifest_options: None,
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: None,
                min_age: Duration::ZERO,
                dry_run: false,
            }),
            wal_fence_options: None,
            compacted_options: None,
            compactions_options: None,
            detach_options: None,
            ..GarbageCollectorOptions::default()
        })
        .build();
    gc.run_gc_once().await;

    // The latest manifest references the replay boundary (3) and everything after it.
    assert_eq!(wal.file_ids(), vec![3, 4]);
}

#[tokio::test]
async fn custom_wal_clone_has_the_same_data() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let source_path = Path::from("/custom-wal/clone-source");
    let clone_path = Path::from("/custom-wal/clone-destination");
    let wal_admin = BTreeMapWalAdmin::default();
    let source_wal = wal_admin.wal(&source_path);

    let source_db = open_db(
        source_path.clone(),
        Arc::clone(&object_store),
        source_wal.clone(),
    )
    .await;
    source_db
        .put(b"clone-key-1", b"clone-value-1")
        .await
        .expect("put failed");
    source_db
        .put(b"clone-key-2", b"clone-value-2")
        .await
        .expect("put failed");
    close_without_memtable_flush(&source_db).await;

    Admin::builder(clone_path.clone(), Arc::clone(&object_store))
        .with_wal_admin(Arc::new(wal_admin.clone()))
        .build()
        .create_clone_builder_from_source(CloneSourceSpec::new(source_path))
        .build()
        .await
        .expect("failed to create clone");

    let clone_wal = wal_admin.wal(&clone_path);
    assert_eq!(clone_wal.snapshot(), source_wal.snapshot());
    let clone_db = open_db(clone_path, object_store, clone_wal).await;
    assert_eq!(
        clone_db
            .get(b"clone-key-1")
            .await
            .expect("get failed")
            .as_deref(),
        Some(b"clone-value-1".as_slice())
    );
    assert_eq!(
        clone_db
            .get(b"clone-key-2")
            .await
            .expect("get failed")
            .as_deref(),
        Some(b"clone-value-2".as_slice())
    );
    close_without_memtable_flush(&clone_db).await;
}

// ---------------------------------------------------------------------------
// WAL name in the manifest
// ---------------------------------------------------------------------------

const WAL_NAME: &str = "btree-map-wal/1";

/// A `BTreeMapWal` that reports a WAL name. It also wakes iterators with no end when a new
/// file arrives, and reports truncation when files before the cursor are gone.
#[derive(Clone)]
struct NamedWal {
    wal: BTreeMapWal,
    appended: Arc<tokio::sync::Notify>,
    /// While set, streaming iterators return nothing.
    paused: Arc<std::sync::atomic::AtomicBool>,
}

impl NamedWal {
    fn new(wal: BTreeMapWal) -> Self {
        Self {
            wal,
            appended: Arc::new(tokio::sync::Notify::new()),
            paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn set_paused(&self, paused: bool) {
        self.paused
            .store(paused, std::sync::atomic::Ordering::SeqCst);
        self.appended.notify_waiters();
    }
}

/// Follows the files of a `NamedWal` from `next_id`, with no end.
struct StreamingIterator {
    wal: NamedWal,
    next_id: u64,
}

#[async_trait]
impl WalIterator for StreamingIterator {
    async fn next(&mut self) -> Result<Option<WalRows>, WalError> {
        loop {
            let notified = self.wal.appended.notified();
            if !self.wal.paused.load(std::sync::atomic::Ordering::SeqCst) {
                let files = self.wal.wal.files.lock();
                if let Some((first, _)) = files.first_key_value() {
                    if *first > self.next_id {
                        return Err(WalError::WalTruncated(self.next_id));
                    }
                }
                if let Some(rows) = files.get(&self.next_id) {
                    let rows = WalRows {
                        rows: rows.clone(),
                        last_consumed_wal_file_id: self.next_id,
                    };
                    self.next_id += 1;
                    return Ok(Some(rows));
                }
            }
            notified.await;
        }
    }
}

#[async_trait]
impl WalReader for NamedWal {
    async fn iterator(
        &self,
        wal_file_id_range: WalFileRange,
    ) -> Result<Box<dyn WalIterator>, WalError> {
        match wal_file_id_range {
            WalFileRange(Bound::Included(start), Bound::Unbounded) => {
                Ok(Box::new(StreamingIterator {
                    wal: self.clone(),
                    next_id: start,
                }))
            }
            range => self.wal.iterator(range).await,
        }
    }

    async fn last_wal_file_id(&self, replay_after_wal_id: u64) -> Result<u64, WalError> {
        self.wal.last_wal_file_id(replay_after_wal_id).await
    }

    fn wal_name(&self) -> Option<&str> {
        Some(WAL_NAME)
    }
}

/// Wraps the writer of a `BTreeMapWal` so that each append wakes the streaming iterators.
struct NotifyingWriter {
    inner: Box<dyn WalWriter>,
    appended: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl WalWriter for NotifyingWriter {
    async fn append(&mut self, write_batch: &[RowEntry]) -> Result<(), WalError> {
        self.inner.append(write_batch).await?;
        self.appended.notify_waiters();
        Ok(())
    }

    async fn flush(&mut self) -> Result<FlushResultFuture, WalError> {
        self.inner.flush().await
    }

    fn observer(&self) -> Box<dyn WalObserver> {
        self.inner.observer()
    }

    fn status(&self) -> Result<WalStatus, WalStatus> {
        self.inner.status()
    }

    async fn close(&mut self) -> Result<(), WalError> {
        self.inner.close().await
    }
}

#[async_trait]
impl WriterInit for NamedWal {
    async fn fence_and_init(
        &self,
        manifest: &mut WriterManifest,
    ) -> Result<WriterInitResult, WalError> {
        let result = self.wal.fence_and_init(manifest).await?;
        Ok(WriterInitResult {
            replay_iterator: result.replay_iterator,
            wal_writer: Box::new(NotifyingWriter {
                inner: result.wal_writer,
                appended: Arc::clone(&self.appended),
            }),
        })
    }

    fn wal_name(&self) -> Option<&str> {
        Some(WAL_NAME)
    }
}

/// A `BTreeMapWalAdmin` that reports a WAL name and checks only the requested range.
#[derive(Clone, Default)]
struct NamedWalAdmin {
    inner: BTreeMapWalAdmin,
}

#[async_trait]
impl WalAdmin for NamedWalAdmin {
    fn garbage_collector(&self, path: &Path) -> Arc<dyn WalGc> {
        self.inner.garbage_collector(path)
    }

    async fn delete_wal(&self, path: &Path, dry_run: bool) -> Result<Vec<String>, WalError> {
        self.inner.delete_wal(path, dry_run).await
    }

    async fn is_empty(
        &self,
        path: &Path,
        replay_after_wal_id: u64,
        _wal_id_last_seen: u64,
    ) -> Result<bool, WalError> {
        Ok(self
            .inner
            .wal(path)
            .files
            .lock()
            .range((Bound::Excluded(replay_after_wal_id), Bound::Unbounded))
            .all(|(_, batch)| batch.is_empty()))
    }

    async fn clone_wal(
        &self,
        from_path: &Path,
        from_manifest: VersionedManifest,
        to_path: &Path,
    ) -> Result<(u64, u64), WalError> {
        self.inner
            .clone_wal(from_path, from_manifest, to_path)
            .await
    }

    fn wal_name(&self) -> Option<&str> {
        Some(WAL_NAME)
    }
}

async fn open_named_db(path: Path, object_store: Arc<dyn ObjectStore>, wal: NamedWal) -> Db {
    Db::builder(path, object_store)
        .with_settings(test_settings())
        .with_wal_writer(Box::new(wal))
        .build()
        .await
        .expect("failed to open database with named WAL")
}

fn admin(path: Path, object_store: Arc<dyn ObjectStore>, wal_admin: Arc<dyn WalAdmin>) -> Admin {
    Admin::builder(path, object_store)
        .with_wal_admin(wal_admin)
        .build()
}

#[tokio::test]
async fn custom_wal_name_is_stamped_on_create() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/name-stamp");
    let db = open_named_db(
        path.clone(),
        Arc::clone(&object_store),
        NamedWal::new(BTreeMapWal::default()),
    )
    .await;
    close_without_memtable_flush(&db).await;

    let manifest = admin(path, object_store, Arc::new(NamedWalAdmin::default()))
        .read_manifest(None)
        .await
        .expect("read manifest failed")
        .expect("manifest missing");
    assert_eq!(manifest.wal_name(), Some(WAL_NAME));
}

#[tokio::test]
async fn custom_wal_open_with_another_wal_fails() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Created with a named WAL, opened with the native WAL.
    let named_path = Path::from("/custom-wal/name-mismatch-named");
    let wal = NamedWal::new(BTreeMapWal::default());
    let db = open_named_db(named_path.clone(), Arc::clone(&object_store), wal.clone()).await;
    db.put(b"key", b"value").await.expect("put failed");
    close_without_memtable_flush(&db).await;
    let err = Db::builder(named_path.clone(), Arc::clone(&object_store))
        .with_settings(test_settings())
        .build()
        .await
        .err()
        .expect("open with the native WAL must fail");
    assert_eq!(err.kind(), slatedb::ErrorKind::Invalid);
    // The right WAL still opens the database and sees the row.
    let db = open_named_db(named_path, Arc::clone(&object_store), wal).await;
    assert_eq!(
        db.get(b"key").await.expect("get failed").as_deref(),
        Some(b"value".as_slice())
    );
    close_without_memtable_flush(&db).await;

    // Created with the native WAL, opened with a named WAL.
    let native_path = Path::from("/custom-wal/name-mismatch-native");
    let db = Db::builder(native_path.clone(), Arc::clone(&object_store))
        .with_settings(test_settings())
        .build()
        .await
        .expect("open native failed");
    db.close().await.expect("close failed");
    let err = Db::builder(native_path, object_store)
        .with_settings(test_settings())
        .with_wal_writer(Box::new(NamedWal::new(BTreeMapWal::default())))
        .build()
        .await
        .err()
        .expect("open with a named WAL must fail");
    assert_eq!(err.kind(), slatedb::ErrorKind::Invalid);
}

#[tokio::test]
async fn custom_wal_reader_rejects_another_wal() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/name-reader");
    let wal = NamedWal::new(BTreeMapWal::default());
    let db = open_named_db(path.clone(), Arc::clone(&object_store), wal.clone()).await;
    db.put(b"key", b"value").await.expect("put failed");

    let err = DbReader::builder(path.clone(), Arc::clone(&object_store))
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_wal_reader(Arc::new(wal.wal.clone()))
        .build()
        .await
        .err()
        .expect("a reader with an unnamed WAL must fail");
    assert_eq!(err.kind(), slatedb::ErrorKind::Invalid);

    let reader = DbReader::builder(path, object_store)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_wal_reader(Arc::new(wal))
        .build()
        .await
        .expect("a reader with the right WAL must open");
    assert_eq!(
        reader.get(b"key").await.expect("get failed").as_deref(),
        Some(b"value".as_slice())
    );
    reader.close().await.expect("close reader failed");
    close_without_memtable_flush(&db).await;
}

#[tokio::test]
async fn custom_wal_set_wal_name_requires_empty_wal() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/set-name");
    let wal_admin = NamedWalAdmin::default();
    let wal = NamedWal::new(wal_admin.inner.wal(&path));
    let admin = admin(
        path.clone(),
        Arc::clone(&object_store),
        Arc::new(wal_admin.clone()),
    );

    let db = open_named_db(path.clone(), Arc::clone(&object_store), wal.clone()).await;
    db.put(b"key", b"value").await.expect("put failed");
    close_without_memtable_flush(&db).await;
    // The row is only in the WAL, so the WAL cannot change.
    let err = admin
        .set_wal_name(None)
        .await
        .expect_err("a WAL with rows must not change");
    assert_eq!(err.kind(), slatedb::ErrorKind::Invalid);

    // Flush the row to L0. Then the WAL is empty after the replay point.
    let db = open_named_db(path.clone(), Arc::clone(&object_store), wal).await;
    db.close().await.expect("close failed");
    admin
        .set_wal_name(None)
        .await
        .expect("an empty WAL can change");

    let db = Db::builder(path, object_store)
        .with_settings(test_settings())
        .build()
        .await
        .expect("the native WAL opens the database now");
    assert_eq!(
        db.get(b"key").await.expect("get failed").as_deref(),
        Some(b"value".as_slice())
    );
    db.close().await.expect("close failed");
}

#[tokio::test]
async fn custom_wal_clone_carries_wal_name() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let source_path = Path::from("/custom-wal/name-clone-source");
    let wal_admin = NamedWalAdmin::default();
    let source_wal = NamedWal::new(wal_admin.inner.wal(&source_path));
    let db = open_named_db(source_path.clone(), Arc::clone(&object_store), source_wal).await;
    db.put(b"key", b"value").await.expect("put failed");
    close_without_memtable_flush(&db).await;

    // A clone admin with a different WAL refuses the source.
    let err = Admin::builder(
        Path::from("/custom-wal/name-clone-native"),
        Arc::clone(&object_store),
    )
    .build()
    .create_clone_builder_from_source(CloneSourceSpec::new(source_path.clone()))
    .build()
    .await
    .expect_err("a clone with another WAL must fail");
    assert_eq!(err.kind(), slatedb::ErrorKind::Invalid);

    let clone_path = Path::from("/custom-wal/name-clone-destination");
    admin(
        clone_path.clone(),
        Arc::clone(&object_store),
        Arc::new(wal_admin.clone()),
    )
    .create_clone_builder_from_source(CloneSourceSpec::new(source_path))
    .build()
    .await
    .expect("clone failed");
    let manifest = admin(
        clone_path.clone(),
        Arc::clone(&object_store),
        Arc::new(wal_admin.clone()),
    )
    .read_manifest(None)
    .await
    .expect("read manifest failed")
    .expect("manifest missing");
    assert_eq!(manifest.wal_name(), Some(WAL_NAME));

    let clone_db = open_named_db(
        clone_path.clone(),
        object_store,
        NamedWal::new(wal_admin.inner.wal(&clone_path)),
    )
    .await;
    assert_eq!(
        clone_db.get(b"key").await.expect("get failed").as_deref(),
        Some(b"value".as_slice())
    );
    close_without_memtable_flush(&clone_db).await;
}

// ---------------------------------------------------------------------------
// Garbage collection of WAL storage that several databases share
// ---------------------------------------------------------------------------

/// Records the ranges that `WalGc::collect` receives.
#[derive(Default)]
struct RecordingWalGc {
    calls: Mutex<Vec<Vec<WalFileRange>>>,
}

#[async_trait]
impl WalGc for RecordingWalGc {
    async fn collect(
        &self,
        referenced_ranges: Vec<WalFileRange>,
        _min_age: Duration,
        _dry_run: bool,
    ) -> Result<(), WalError> {
        self.calls.lock().push(referenced_ranges);
        Ok(())
    }
}

fn wal_gc_options() -> GarbageCollectorOptions {
    GarbageCollectorOptions {
        manifest_options: None,
        wal_options: Some(GarbageCollectorDirectoryOptions {
            interval: None,
            min_age: Duration::ZERO,
            dry_run: false,
        }),
        wal_fence_options: None,
        compacted_options: None,
        compactions_options: None,
        detach_options: None,
        ..GarbageCollectorOptions::default()
    }
}

#[tokio::test]
async fn admin_referenced_wal_ranges_matches_gc_task() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/referenced-ranges");
    let wal = BTreeMapWal::default();
    let db = open_db(path.clone(), Arc::clone(&object_store), wal).await;
    db.put(b"key-1", b"value-1").await.expect("put failed");
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("memtable flush failed");
    // A checkpoint references the WAL range of its manifest.
    let admin = Admin::builder(path.clone(), Arc::clone(&object_store)).build();
    admin
        .create_detached_checkpoint(&Default::default())
        .await
        .expect("checkpoint failed");
    db.put(b"key-2", b"value-2").await.expect("put failed");
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("memtable flush failed");
    db.put(b"key-3", b"value-3").await.expect("put failed");
    close_without_memtable_flush(&db).await;

    let recorder = Arc::new(RecordingWalGc::default());
    GarbageCollectorBuilder::new(path, object_store)
        .with_wal_gc(recorder.clone())
        .with_options(wal_gc_options())
        .build()
        .run_gc_once()
        .await;
    let from_gc = recorder
        .calls
        .lock()
        .last()
        .cloned()
        .expect("collect was not called");
    let from_admin = admin
        .referenced_wal_ranges()
        .await
        .expect("referenced_wal_ranges failed");
    assert_eq!(from_admin, from_gc);
    assert!(from_admin.len() >= 2, "{from_admin:?}");
}

#[tokio::test]
async fn shared_wal_gc_waits_for_all_dbs() {
    // Two databases. Each shared object holds one WAL file of each database: object k holds
    // file k of database A and file k of database B. An object can go only when neither
    // database references its file.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let paths = [
        Path::from("/custom-wal/shared-a"),
        Path::from("/custom-wal/shared-b"),
    ];
    let mut dbs = Vec::new();
    for path in &paths {
        let db = open_db(
            path.clone(),
            Arc::clone(&object_store),
            BTreeMapWal::default(),
        )
        .await;
        for id in 1..=3 {
            db.put(format!("key-{id}").as_bytes(), b"value")
                .await
                .expect("put failed");
        }
        dbs.push(db);
    }
    // Only database A moves its data to L0.
    dbs[0]
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect("memtable flush failed");
    for db in &dbs {
        close_without_memtable_flush(db).await;
    }

    let deletable = |references: &[Vec<WalFileRange>], object: u64| {
        references
            .iter()
            .all(|ranges| !ranges.iter().any(|range| range_contains(range, object)))
    };
    let mut references = Vec::new();
    for path in &paths {
        references.push(
            Admin::builder(path.clone(), Arc::clone(&object_store))
                .build()
                .referenced_wal_ranges()
                .await
                .expect("referenced_wal_ranges failed"),
        );
    }
    // Database A alone would free objects 1 and 2. Database B still references them.
    assert!(deletable(&references[..1], 1));
    assert!(!deletable(&references, 1));
    assert!(!deletable(&references, 2));
}

// ---------------------------------------------------------------------------
// DbReader that streams the WAL
// ---------------------------------------------------------------------------

async fn eventually_get(reader: &DbReader, key: &[u8]) -> Option<Vec<u8>> {
    for _ in 0..200 {
        if let Some(value) = reader.get(key).await.expect("get failed") {
            return Some(value.to_vec());
        }
        slatedb_common::DefaultSystemClock::new()
            .sleep(Duration::from_millis(25))
            .await;
    }
    None
}

fn stream_reader_options() -> DbReaderOptions {
    DbReaderOptions {
        // Long enough that the test can only pass through the stream.
        manifest_poll_interval: Duration::from_secs(3600),
        wal_follow: WalFollowMode::Stream,
        ..DbReaderOptions::default()
    }
}

#[tokio::test]
async fn custom_wal_reader_stream_sees_write_without_poll() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/stream");
    let wal = NamedWal::new(BTreeMapWal::default());
    let db = open_named_db(path.clone(), Arc::clone(&object_store), wal.clone()).await;
    db.put(b"before", b"1").await.expect("put failed");

    let reader = DbReader::builder(path, object_store)
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(stream_reader_options())
        .with_wal_reader(Arc::new(wal))
        .build()
        .await
        .expect("open reader failed");
    assert_eq!(
        reader.get(b"before").await.expect("get failed").as_deref(),
        Some(b"1".as_slice())
    );

    db.put(b"after", b"2").await.expect("put failed");
    assert_eq!(
        eventually_get(&reader, b"after").await.as_deref(),
        Some(b"2".as_slice())
    );

    reader.close().await.expect("close reader failed");
    close_without_memtable_flush(&db).await;
}

#[tokio::test]
async fn custom_wal_reader_stream_refreshes_manifest_on_truncation() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/custom-wal/stream-truncated");
    let wal = NamedWal::new(BTreeMapWal::default());
    let db = open_named_db(path.clone(), Arc::clone(&object_store), wal.clone()).await;
    db.put(b"first", b"1").await.expect("put failed");

    let reader = DbReader::builder(path.clone(), Arc::clone(&object_store))
        .with_reader_mode(DbReaderMode::FollowLatest)
        .with_options(stream_reader_options())
        .with_wal_reader(Arc::new(wal.clone()))
        .build()
        .await
        .expect("open reader failed");

    assert_eq!(
        eventually_get(&reader, b"first").await.as_deref(),
        Some(b"1".as_slice())
    );

    // Hold the stream back while the writer moves "second" to L0. Then delete the WAL file of
    // "second", so that the stream finds a gap when it continues.
    wal.set_paused(true);
    db.put(b"second", b"2").await.expect("put failed");
    db.flush_with_options(FlushOptions {
        flush_type: FlushType::MemTable,
    })
    .await
    .expect("memtable flush failed");
    db.put(b"third", b"3").await.expect("put failed");
    let replay_after = Admin::builder(path, object_store)
        .build()
        .read_manifest(None)
        .await
        .expect("read manifest failed")
        .expect("manifest missing")
        .replay_after_wal_id();
    wal.wal.files.lock().retain(|id, _| *id > replay_after);
    wal.set_paused(false);

    // The rows of "second" are only in L0 now. The reader sees them only if it loads the
    // latest manifest, which a one-hour poll does not do in this test.
    assert_eq!(
        eventually_get(&reader, b"third").await.as_deref(),
        Some(b"3".as_slice())
    );
    assert_eq!(
        reader.get(b"second").await.expect("get failed").as_deref(),
        Some(b"2".as_slice())
    );

    reader.close().await.expect("close reader failed");
    close_without_memtable_flush(&db).await;
}
