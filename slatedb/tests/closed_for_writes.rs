//! `Admin::close_for_writes` and `Admin::reopen_for_writes`.

use std::sync::Arc;

use slatedb::admin::{Admin, CloneSourceSpec};
use slatedb::config::{FlushOptions, FlushType};
use slatedb::object_store::memory::InMemory;
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStore;
use slatedb::{CloseReason, Db, DbReader, ErrorKind};

async fn open(path: &Path, store: &Arc<dyn ObjectStore>) -> Result<Db, slatedb::Error> {
    Db::builder(path.clone(), Arc::clone(store)).build().await
}

fn admin(path: &Path, store: &Arc<dyn ObjectStore>) -> Admin {
    Admin::builder(path.clone(), Arc::clone(store)).build()
}

async fn closed_flag(path: &Path, store: &Arc<dyn ObjectStore>) -> bool {
    admin(path, store)
        .read_manifest(None)
        .await
        .expect("read manifest failed")
        .expect("the database has a manifest")
        .closed_for_writes()
}

#[tokio::test]
async fn closed_database_refuses_writers_until_reopened() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/closed/refuse");
    let db = open(&path, &store).await.expect("open failed");
    db.put(b"key", b"value").await.expect("put failed");
    db.close().await.expect("close failed");

    let admin = admin(&path, &store);
    admin
        .close_for_writes()
        .await
        .expect("close for writes failed");
    // Idempotent.
    admin.close_for_writes().await.expect("second close failed");
    assert!(closed_flag(&path, &store).await);

    let err = open(&path, &store)
        .await
        .err()
        .expect("a writer must not open a closed database");
    assert_eq!(err.kind(), ErrorKind::Closed(CloseReason::Fenced));

    admin.reopen_for_writes().await.expect("reopen failed");
    admin
        .reopen_for_writes()
        .await
        .expect("second reopen failed");
    assert!(!closed_flag(&path, &store).await);
    let db = open(&path, &store).await.expect("reopened database opens");
    assert_eq!(
        db.get(b"key").await.expect("get failed").as_deref(),
        Some(b"value".as_slice())
    );
    db.close().await.expect("close failed");
}

#[tokio::test]
async fn close_fences_a_writer_that_is_open() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/closed/fence");
    let db = open(&path, &store).await.expect("open failed");
    db.put(b"before", b"1").await.expect("put failed");

    admin(&path, &store)
        .close_for_writes()
        .await
        .expect("close for writes failed");

    // The next manifest update of the old writer fails: the epoch moved.
    db.put(b"after", b"2").await.ok();
    let err = db
        .flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .expect_err("the open writer must be fenced");
    assert_eq!(err.kind(), ErrorKind::Closed(CloseReason::Fenced));
    // The flag is still there: the fenced writer did not clear it.
    assert!(closed_flag(&path, &store).await);
}

#[tokio::test]
async fn closed_database_still_reads_and_clones() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/closed/source");
    let db = open(&path, &store).await.expect("open failed");
    db.put(b"key", b"value").await.expect("put failed");
    db.close().await.expect("close failed");
    admin(&path, &store)
        .close_for_writes()
        .await
        .expect("close for writes failed");

    let reader = DbReader::builder(path.clone(), Arc::clone(&store))
        .build()
        .await
        .expect("a reader opens a closed database");
    assert_eq!(
        reader.get(b"key").await.expect("get failed").as_deref(),
        Some(b"value".as_slice())
    );
    reader.close().await.expect("close reader failed");

    let clone_path = Path::from("/closed/clone");
    admin(&clone_path, &store)
        .create_clone_builder_from_source(CloneSourceSpec::new(path.clone()))
        .build()
        .await
        .expect("a closed database can be a clone source");
    assert!(
        !closed_flag(&clone_path, &store).await,
        "the clone must not inherit the flag"
    );
    let clone = open(&clone_path, &store)
        .await
        .expect("a writer opens the clone");
    clone.put(b"new", b"row").await.expect("put failed");
    assert_eq!(
        clone.get(b"key").await.expect("get failed").as_deref(),
        Some(b"value".as_slice())
    );
    clone.close().await.expect("close failed");
}

#[tokio::test]
async fn close_at_epoch_refuses_when_another_writer_opened() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/closed/epoch");
    let db = open(&path, &store).await.expect("open failed");
    db.put(b"key", b"value").await.expect("put failed");
    db.close().await.expect("close failed");
    let epoch = admin(&path, &store)
        .read_manifest(None)
        .await
        .expect("read manifest failed")
        .expect("the database has a manifest")
        .writer_epoch();

    // Another writer opens and closes: the epoch moves on.
    let other = open(&path, &store).await.expect("second open failed");
    other.close().await.expect("close failed");
    let err = admin(&path, &store)
        .close_for_writes_at_epoch(epoch)
        .await
        .expect_err("an old epoch must not close the database");
    assert_eq!(err.kind(), ErrorKind::Closed(CloseReason::Fenced));
    assert!(!closed_flag(&path, &store).await);

    let epoch = admin(&path, &store)
        .read_manifest(None)
        .await
        .expect("read manifest failed")
        .expect("the database has a manifest")
        .writer_epoch();
    let admin = admin(&path, &store);
    admin
        .close_for_writes_at_epoch(epoch)
        .await
        .expect("the current epoch closes the database");
    // A retry succeeds.
    admin
        .close_for_writes_at_epoch(epoch)
        .await
        .expect("a retry succeeds");
    assert!(closed_flag(&path, &store).await);
    assert!(open(&path, &store).await.is_err());
}

async fn writer_epoch(path: &Path, store: &Arc<dyn ObjectStore>) -> u64 {
    admin(path, store)
        .read_manifest(None)
        .await
        .expect("read manifest failed")
        .expect("the database has a manifest")
        .writer_epoch()
}

#[tokio::test]
async fn reopen_at_epoch_keeps_a_newer_close() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("/closed/reopen-epoch");
    let db = open(&path, &store).await.expect("open failed");
    db.close().await.expect("close failed");
    let admin = admin(&path, &store);

    // First close. A slow caller reads its epoch now, and removes it later.
    let epoch = writer_epoch(&path, &store).await;
    admin
        .close_for_writes_at_epoch(epoch)
        .await
        .expect("first close failed");
    let first_close = writer_epoch(&path, &store).await;
    admin
        .reopen_for_writes_at_epoch(first_close)
        .await
        .expect("reopen of the first close failed");
    assert!(!closed_flag(&path, &store).await);

    // A writer opens, and a second close follows.
    let db = open(&path, &store).await.expect("open failed");
    db.close().await.expect("close failed");
    let epoch = writer_epoch(&path, &store).await;
    admin
        .close_for_writes_at_epoch(epoch)
        .await
        .expect("second close failed");

    // The slow caller retries its reopen: the second close stays.
    let err = admin
        .reopen_for_writes_at_epoch(first_close)
        .await
        .expect_err("a reopen for an older close must not remove a newer close");
    assert_eq!(err.kind(), ErrorKind::Closed(CloseReason::Fenced));
    assert!(closed_flag(&path, &store).await);
    assert!(
        open(&path, &store).await.is_err(),
        "the database must stay closed"
    );

    // The reopen for the second close works, and a retry of it succeeds.
    let second_close = writer_epoch(&path, &store).await;
    admin
        .reopen_for_writes_at_epoch(second_close)
        .await
        .expect("reopen of the second close failed");
    admin
        .reopen_for_writes_at_epoch(second_close)
        .await
        .expect("a retry of the reopen must succeed");
    assert!(!closed_flag(&path, &store).await);
}
