//! In-process wake-up signals between the writer, the compaction coordinator,
//! and the embedded compaction worker.
//!
//! Without these signals, each component finds work only when its poll ticker
//! fires. The coordinator sees a new L0 SST only on its next
//! [`CompactorOptions::poll_interval`](crate::config::CompactorOptions::poll_interval)
//! tick. The worker sees a new job only on its next
//! [`CompactionWorkerOptions::compactions_poll_interval`](crate::config::CompactionWorkerOptions::compactions_poll_interval)
//! tick. The writer sees a compacted manifest only on its next
//! [`Settings::manifest_poll_interval`](crate::config::Settings::manifest_poll_interval)
//! tick. If the writer stalls because L0 is full, the stall lasts for the sum of
//! these waits.
//!
//! When the components run in the same process, they raise a [`Signal`] after
//! each change that another component must react to:
//!
//! | Signal | Raised by | After | Wakes |
//! |---|---|---|---|
//! | [`CompactionSignals::l0_flushed`] | writer | it adds L0 SSTs to the manifest | coordinator |
//! | [`CompactionSignals::compactions_scheduled`] | coordinator | it marks a job `Scheduled` | worker |
//! | [`CompactionSignals::compaction_finished`] | worker | it writes a `Compacted` job | coordinator |
//! | [`CompactionSignals::manifest_compacted`] | coordinator | it writes a manifest that changes the LSM tree | writer |
//!
//! The poll tickers stay in place. They find changes that no signal reports, for
//! example a compaction that a remote process submits. A deployment that uses
//! the signals can set long poll intervals, so that an idle database sends
//! almost no requests to the object store.
//!
//! A signal holds at most one pending wake-up. If a component raises a signal
//! many times before the listener runs, the listener handles one event. A
//! listener always reads the current state from the object store, so one event
//! is enough.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Notify;

use crate::dispatcher::Notifier;

/// A wake-up signal that collapses repeated raises into one pending event.
#[derive(Clone, Debug, Default)]
pub(crate) struct Signal {
    notify: Arc<Notify>,
}

impl Signal {
    /// Wakes the listener. If the listener is busy, it wakes once more after it
    /// finishes its current work.
    pub(crate) fn raise(&self) {
        self.notify.notify_one();
    }

    /// Returns a [`Notifier`] that produces `message` each time the signal is
    /// raised. Register the notifier with
    /// [`MessageHandler::notifiers`](crate::dispatcher::MessageHandler::notifiers).
    pub(crate) fn notifier<T: Send + 'static>(&self, message: fn() -> T) -> Box<dyn Notifier<T>> {
        Box::new(SignalNotifier {
            notify: self.notify.clone(),
            message,
        })
    }
}

struct SignalNotifier<T> {
    notify: Arc<Notify>,
    message: fn() -> T,
}

#[async_trait]
impl<T: Send + 'static> Notifier<T> for SignalNotifier<T> {
    async fn notify(&mut self) -> T {
        self.notify.notified().await;
        (self.message)()
    }
}

/// The set of signals that connect a writer, a coordinator, and an embedded
/// worker in one process. See the [module documentation](self).
#[derive(Clone, Debug, Default)]
pub(crate) struct CompactionSignals {
    /// The writer added L0 SSTs to the manifest.
    pub(crate) l0_flushed: Signal,
    /// The coordinator marked one or more compactions `Scheduled`.
    pub(crate) compactions_scheduled: Signal,
    /// The worker wrote a `Compacted` result to `.compactions`.
    pub(crate) compaction_finished: Signal,
    /// The coordinator wrote a manifest that changes the LSM tree.
    pub(crate) manifest_compacted: Signal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        CompactionWorkerOptions, CompactorOptions, FlushOptions, FlushType, Settings,
    };
    use crate::Db;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult,
    };
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    /// A poll interval that no test reaches. Any progress a test sees must
    /// come from a signal, or from the first tick that each ticker fires at
    /// startup.
    const NEVER: Duration = Duration::from_secs(3600);

    /// Counts every request that reaches the inner store.
    #[derive(Debug)]
    struct CountingObjectStore {
        inner: Arc<dyn ObjectStore>,
        requests: AtomicUsize,
    }

    impl CountingObjectStore {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(InMemory::new()),
                requests: AtomicUsize::new(0),
            })
        }

        fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }

        fn count(&self) {
            self.requests.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl fmt::Display for CountingObjectStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "CountingObjectStore({})", self.inner)
        }
    }

    #[async_trait]
    impl ObjectStore for CountingObjectStore {
        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.count();
            self.inner.get_opts(location, options).await
        }

        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.count();
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.count();
            self.inner.put_multipart_opts(location, opts).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.count();
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.count();
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.count();
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.count();
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.count();
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Settings with an embedded compactor and worker whose polls never fire
    /// after startup, and an L0 limit of `l0_max_ssts`.
    fn settings_without_polls(l0_max_ssts: usize) -> Settings {
        Settings {
            flush_interval: Some(Duration::from_millis(10)),
            manifest_poll_interval: NEVER,
            l0_max_ssts,
            l0_max_ssts_per_key: l0_max_ssts,
            garbage_collector_options: None,
            compactor_options: Some(CompactorOptions {
                poll_interval: NEVER,
                worker: Some(CompactionWorkerOptions {
                    compactions_poll_interval: NEVER,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    async fn open_db(store: Arc<CountingObjectStore>, settings: Settings) -> Db {
        Db::builder("/tmp/test_compaction_signals", store)
            .with_settings(settings)
            .build()
            .await
            .unwrap()
    }

    /// Writes one key and flushes the memtable, which adds one L0 SST.
    async fn write_l0(db: &Db, i: usize) {
        db.put(format!("key{i:04}").as_bytes(), &[b'v'; 64])
            .await
            .unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
    }

    fn l0_and_sorted_runs(db: &Db) -> (usize, usize) {
        let state = db.inner.state.read().state();
        let tree = &state.core().tree;
        (tree.l0.len(), tree.compacted.len())
    }

    async fn wait_for_sorted_run(db: &Db) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while l0_and_sorted_runs(db).1 == 0 {
            assert!(
                Instant::now() < deadline,
                "no compaction reached the writer's manifest [state={:?}]",
                l0_and_sorted_runs(db)
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_compact_new_l0_without_a_poll() {
        let db = open_db(CountingObjectStore::new(), settings_without_polls(8)).await;
        // The scheduler compacts L0 once it holds `min_compaction_sources` (4)
        // SSTs.
        for i in 0..4 {
            write_l0(&db, i).await;
        }

        wait_for_sorted_run(&db).await;

        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_resume_l0_flush_stalled_at_the_limit_without_a_poll() {
        let db = open_db(CountingObjectStore::new(), settings_without_polls(4)).await;

        // With an L0 limit of 4, each flush after the fourth must wait until a
        // compaction frees an L0 slot. Without signals, the writer waits for
        // the next poll, which is an hour away.
        let flushes = tokio::time::timeout(Duration::from_secs(30), async {
            for i in 0..16 {
                write_l0(&db, i).await;
            }
        })
        .await;

        assert!(
            flushes.is_ok(),
            "a flush stalled at the L0 limit did not resume [state={:?}]",
            l0_and_sorted_runs(&db)
        );
        db.close().await.unwrap();
    }

    /// Waits until `.compactions` holds a `Scheduled` compaction and returns it.
    async fn wait_for_scheduled_compaction(
        compactions_store: Arc<crate::compactions_store::CompactionsStore>,
    ) -> crate::compactor_state::Compaction {
        use crate::compactions_store::StoredCompactions;
        use crate::compactor_state::CompactionStatus;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(stored) = StoredCompactions::try_load(compactions_store.clone())
                .await
                .unwrap()
            {
                let scheduled = stored
                    .prepare_dirty()
                    .unwrap()
                    .value
                    .iter_with_status(&[CompactionStatus::Scheduled])
                    .next()
                    .cloned();
                if let Some(compaction) = scheduled {
                    return compaction;
                }
            }
            assert!(Instant::now() < deadline, "no compaction was scheduled");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_reclaim_the_job_of_a_dead_worker_without_a_poll() {
        use crate::compactions_store::{CompactionsStore, StoredCompactions};
        use crate::compactor_state::{CompactionStatus, WorkerSpec};

        let store = CountingObjectStore::new();
        // A coordinator without a worker schedules an L0 compaction that nobody
        // runs.
        let mut settings = settings_without_polls(4);
        if let Some(compactor) = settings.compactor_options.as_mut() {
            compactor.worker = None;
        }
        let db = open_db(store.clone(), settings).await;
        for i in 0..4 {
            write_l0(&db, i).await;
        }
        let compactions_store = Arc::new(CompactionsStore::new(
            &Path::from("/tmp/test_compaction_signals"),
            store.clone(),
        ));
        let compaction = wait_for_scheduled_compaction(compactions_store.clone()).await;
        db.close().await.unwrap();

        // A worker claims the job and dies: the job stays `Running`, and its
        // heartbeat is fresh when the database opens again.
        let mut stored = StoredCompactions::try_load(compactions_store)
            .await
            .unwrap()
            .expect("the compactions file exists");
        let mut dirty = stored.prepare_dirty().unwrap();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        dirty.value.insert(
            compaction
                .with_status(CompactionStatus::Running)
                .with_worker(Some(WorkerSpec::new("dead-worker".to_string(), now_ms))),
        );
        stored.update(dirty).await.unwrap();

        // Open again with an embedded worker. L0 is at the limit, and the job
        // of the dead worker blocks every other L0 compaction. The coordinator
        // must reclaim the job when its heartbeat times out, without a poll.
        let mut settings = settings_without_polls(4);
        if let Some(compactor) = settings.compactor_options.as_mut() {
            compactor.worker_heartbeat_timeout = Duration::from_secs(1);
        }
        let db = open_db(store, settings).await;
        let flush = tokio::time::timeout(Duration::from_secs(15), write_l0(&db, 4)).await;

        assert!(
            flush.is_ok(),
            "a flush stalled behind the job of a dead worker did not resume [state={:?}]",
            l0_and_sorted_runs(&db)
        );
        db.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_send_no_requests_when_idle_after_a_compaction() {
        let store = CountingObjectStore::new();
        let db = open_db(store.clone(), settings_without_polls(8)).await;
        for i in 0..4 {
            write_l0(&db, i).await;
        }
        wait_for_sorted_run(&db).await;
        // Let the writer, the coordinator and the worker finish the work that
        // the compaction started. The signals must not wake each other in a
        // loop.
        tokio::time::sleep(Duration::from_secs(2)).await;

        let before = store.requests();
        tokio::time::sleep(Duration::from_secs(3)).await;
        let idle = store.requests() - before;

        assert_eq!(idle, 0, "an idle database sent {idle} requests");
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_collapse_raises_into_one_event() {
        let signal = Signal::default();
        let mut notifier = signal.notifier(|| 7u32);

        signal.raise();
        signal.raise();
        signal.raise();

        assert_eq!(notifier.notify().await, 7);
        let second = tokio::time::timeout(Duration::from_millis(50), notifier.notify()).await;
        assert!(second.is_err(), "three raises must produce one event");
    }

    #[tokio::test]
    async fn should_wake_a_waiting_listener() {
        let signal = Signal::default();
        let mut notifier = signal.notifier(|| ());
        let raiser = signal.clone();
        let task = tokio::spawn(async move { notifier.notify().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        raiser.raise();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("listener must wake")
            .expect("task must not panic");
    }
}
