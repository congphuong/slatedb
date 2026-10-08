use crate::bytes_range::BytesRange;
use crate::checkpoint::Checkpoint;
use crate::config::CheckpointOptions;

use crate::db::builder::CloneSourceSpec;
use crate::error::SlateDBError;
use crate::error::SlateDBError::CheckpointMissing;
use crate::manifest::store::{ManifestStore, StoredManifest};
use crate::manifest::{Manifest, ProjectionConfig, VersionedManifest};
use crate::utils::IdGenerator;
use crate::wal::WalAdmin;
use bytes::Bytes;
use fail_parallel::{fail_point, FailPointRegistry};
use object_store::path::Path;
use object_store::ObjectStore;
use slatedb_common::clock::SystemClock;
use slatedb_common::DbRand;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// User-supplied predicate deciding whether a segment is included in the
/// clone. Receives the segment's prefix (the unsegmented tree participates as
/// the empty prefix). Returning `false` drops the segment entirely.
pub(crate) type SegmentFilterFn = Arc<dyn Fn(&[u8]) -> bool + Send + Sync>;

/// User-supplied projector returning the effective range for a segment. The
/// returned range's bounded ends must fall within `[prefix, prefix++)`;
/// `Unbounded` ends resolve to the segment edges. Empty ranges surface to the
/// caller as `SlateDBError::InvalidProjection`.
pub(crate) type SegmentProjectionFn =
    Arc<dyn Fn(&[u8]) -> Result<BytesRange, SlateDBError> + Send + Sync>;

struct CopyWalParams {
    from_path: Path,
    from_manifest: VersionedManifest,
    to_path: Path,
}

struct CreateCloneManifestResult {
    clone_manifest: StoredManifest,
    copy_wal_params: Option<CopyWalParams>,
}

pub(crate) async fn create_clone<P: Into<Path>, R: RangeBounds<Bytes> + Clone>(
    clone_sources: Vec<CloneSourceSpec<R>>,
    clone_path: P,
    object_store: Arc<dyn ObjectStore>,
    wal_admin: Arc<dyn WalAdmin>,
    fp_registry: Arc<FailPointRegistry>,
    system_clock: Arc<dyn SystemClock>,
    rand: Arc<DbRand>,
    projection_ranges: Option<Vec<R>>,
    segment_filter: Option<SegmentFilterFn>,
    segment_projection: Option<SegmentProjectionFn>,
) -> Result<(), SlateDBError> {
    let clone_path = clone_path.into();

    validate_clone_source_specs(&clone_sources, &clone_path)?;

    let CreateCloneManifestResult {
        mut clone_manifest,
        copy_wal_params,
    } = create_clone_manifest(
        clone_path.clone(),
        clone_sources,
        object_store,
        system_clock.clone(),
        rand,
        fp_registry.clone(),
        projection_ranges,
        segment_filter,
        segment_projection,
        wal_admin.as_ref(),
    )
    .await?;

    if !clone_manifest.db_state().initialized {
        let (replay_after_wal_id, wal_id_last_seen) = match copy_wal_params {
            Some(params) => copy_wal(wal_admin.as_ref(), params).await?,
            None => (0, 0),
        };
        let next_wal_sst_id = wal_id_last_seen
            .checked_add(1)
            .ok_or(SlateDBError::InvalidDBState)?;

        let mut dirty = clone_manifest.prepare_dirty()?;
        dirty.value.core.replay_after_wal_id = replay_after_wal_id;
        dirty.value.core.next_wal_sst_id = next_wal_sst_id;
        dirty.value.core.initialized = true;
        clone_manifest.update(dirty).await?;
    }

    Ok(())
}

async fn create_clone_manifest<R: RangeBounds<Bytes> + Clone>(
    clone_path: Path,
    source_specs: Vec<CloneSourceSpec<R>>,
    object_store: Arc<dyn ObjectStore>,
    system_clock: Arc<dyn SystemClock>,
    rand: Arc<DbRand>,
    #[allow(unused)] fp_registry: Arc<FailPointRegistry>,
    projection_ranges: Option<Vec<R>>,
    segment_filter: Option<SegmentFilterFn>,
    segment_projection: Option<SegmentProjectionFn>,
    wal_admin: &dyn WalAdmin,
) -> Result<CreateCloneManifestResult, SlateDBError> {
    let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));

    let (clone_manifest, copy_wal_params) =
        match StoredManifest::try_load(clone_manifest_store.clone(), system_clock.clone()).await? {
            Some(initialized_clone_manifest)
                if initialized_clone_manifest.db_state().initialized =>
            {
                for source_spec in &source_specs {
                    validate_attached_to_external_db(
                        source_spec.path.to_string(),
                        source_spec.checkpoint,
                        &initialized_clone_manifest,
                    )?;
                    validate_external_dbs_contain_final_checkpoint(
                        Arc::new(ManifestStore::new(&source_spec.path, object_store.clone())),
                        source_spec.path.to_string(),
                        &initialized_clone_manifest,
                        object_store.clone(),
                    )
                    .await?;
                }
                return Ok(CreateCloneManifestResult {
                    clone_manifest: initialized_clone_manifest,
                    copy_wal_params: None,
                });
            }
            Some(uninitialized_clone_manifest) => {
                for source_spec in &source_specs {
                    validate_attached_to_external_db(
                        source_spec.path.to_string(),
                        source_spec.checkpoint,
                        &uninitialized_clone_manifest,
                    )?;
                }
                let copy_wal_params = match &source_specs[..] {
                    [source_spec] => {
                        let source = rebuild_source(
                            source_spec,
                            &uninitialized_clone_manifest,
                            &object_store,
                            &system_clock,
                            &rand,
                            &projection_ranges,
                            segment_filter.as_ref(),
                            segment_projection.as_ref(),
                        )
                        .await?;
                        Some(copy_wal_params_for_source(&source, &clone_path))
                    }
                    _ => None,
                };
                (uninitialized_clone_manifest, copy_wal_params)
            }
            None => {
                let sources = build_sources(
                    &source_specs,
                    &object_store,
                    &system_clock,
                    &rand,
                    &projection_ranges,
                    segment_filter.as_ref(),
                    segment_projection.as_ref(),
                )
                .await?;
                let copy_wal_params = match &sources[..] {
                    [source] => Some(copy_wal_params_for_source(source, &clone_path)),
                    _ => None,
                };

                let projection_requested = projection_ranges.is_some()
                    || segment_filter.is_some()
                    || segment_projection.is_some()
                    || source_specs.iter().any(|s| s.projection_ranges.is_some());

                let mut manifest: Manifest = match &sources[..] {
                    [single_source] => {
                        // WAL SSTs are copied to the clone verbatim and replayed in full
                        // when the clone is opened, so entries outside the projected
                        // range would leak into the clone. So we reject projections if
                        // there are non-fence WALs to copy.
                        if projection_requested {
                            validate_no_data_wal(&sources, wal_admin).await?;
                        }
                        Manifest::cloned(
                            &single_source.manifest,
                            single_source.path.to_string(),
                            single_source.checkpoint.id,
                            rand.clone(),
                        )
                    }
                    [..] => {
                        validate_no_data_wal(&sources, wal_admin).await?;
                        Manifest::cloned_from_union(sources, rand.clone())?
                    }
                };
                manifest.core.initialized = false;

                (
                    StoredManifest::store_uninitialized_clone(
                        clone_manifest_store,
                        manifest,
                        system_clock.clone(),
                    )
                    .await?,
                    copy_wal_params,
                )
            }
        };

    fail_point!(fp_registry, "create-clone-manifest-io-error", |_| Err(
        SlateDBError::from(std::io::Error::other("oops"))
    ));

    // Ensure all external databases contain the final checkpoint.
    for external_db in &clone_manifest.manifest().external_dbs {
        let Some(final_checkpoint_id) = external_db.final_checkpoint_id else {
            // If the final checkpoint id is not set, we can skip this check
            continue;
        };
        let external_db_manifest_store = source_specs
            .iter()
            .find(|p| p.path.to_string() == external_db.path)
            .map(|p| Arc::new(ManifestStore::new(&p.path, object_store.clone())))
            .unwrap_or_else(|| {
                Arc::new(ManifestStore::new(
                    &external_db.path.clone().into(),
                    object_store.clone(),
                ))
            });

        let mut external_db_manifest =
            load_initialized_manifest(external_db_manifest_store, system_clock.clone()).await?;

        if external_db_manifest
            .db_state()
            .find_checkpoint(final_checkpoint_id)
            .is_none()
        {
            external_db_manifest
                .write_checkpoint(
                    final_checkpoint_id,
                    &CheckpointOptions {
                        lifetime: None,
                        source: Some(external_db.source_checkpoint_id),
                        name: None,
                    },
                )
                .await?;
        }
    }

    Ok(CreateCloneManifestResult {
        clone_manifest,
        copy_wal_params,
    })
}

/// Turn user-supplied projection ranges into a range set (see
/// [`BytesRange::normalize_set`]). Errors with
/// `SlateDBError::InvalidProjection` when the list is empty or when one of the
/// ranges is empty.
fn to_range_set<T: RangeBounds<Bytes>>(ranges: &[T]) -> Result<Vec<BytesRange>, SlateDBError> {
    let invalid = |reason: &str| SlateDBError::InvalidProjection {
        prefix: Bytes::new(),
        reason: reason.into(),
    };
    if ranges.is_empty() {
        return Err(invalid("projection has no ranges"));
    }
    let ranges = ranges
        .iter()
        .map(|range| {
            BytesRange::try_new(range.start_bound().cloned(), range.end_bound().cloned())
                .ok_or_else(|| invalid("empty range"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BytesRange::normalize_set(ranges))
}

#[derive(Clone)]
pub(crate) struct CloneSource {
    pub path: Path,
    pub manifest: Manifest,
    pub checkpoint: Checkpoint,
}

impl CloneSource {
    fn versioned_manifest(&self) -> VersionedManifest {
        VersionedManifest::from_manifest(self.checkpoint.manifest_id, self.manifest.clone())
    }
}

/// Builds a list of clone sources from the provided specifications. For each source spec, a
/// manifest at the specified checkpoint is loaded (if the checkpoint is not specified then it is
/// created). Additionally, if any of `projection_ranges`, `segment_filter`, or
/// `segment_projection` are specified then they are applied to the returned
/// manifests using `Manifest::projected`.
async fn build_sources<R: RangeBounds<Bytes> + Clone>(
    source_specs: &Vec<CloneSourceSpec<R>>,
    object_store: &Arc<dyn ObjectStore>,
    system_clock: &Arc<dyn SystemClock>,
    rand: &Arc<DbRand>,
    projection_ranges: &Option<Vec<R>>,
    segment_filter: Option<&SegmentFilterFn>,
    segment_projection: Option<&SegmentProjectionFn>,
) -> Result<Vec<CloneSource>, SlateDBError> {
    let mut result: Vec<CloneSource> = vec![];
    for source in source_specs {
        result.push(
            build_source(
                source,
                source.checkpoint,
                object_store,
                system_clock,
                rand,
                projection_ranges,
                segment_filter,
                segment_projection,
            )
            .await?,
        );
    }
    Ok(result)
}

async fn build_source<R: RangeBounds<Bytes> + Clone>(
    source: &CloneSourceSpec<R>,
    checkpoint_id: Option<Uuid>,
    object_store: &Arc<dyn ObjectStore>,
    system_clock: &Arc<dyn SystemClock>,
    rand: &Arc<DbRand>,
    projection_ranges: &Option<Vec<R>>,
    segment_filter: Option<&SegmentFilterFn>,
    segment_projection: Option<&SegmentProjectionFn>,
) -> Result<CloneSource, SlateDBError> {
    let manifest_store = Arc::new(ManifestStore::new(&source.path, object_store.clone()));
    let mut latest_manifest =
        load_initialized_manifest(manifest_store.clone(), system_clock.clone()).await?;
    let checkpoint =
        get_or_create_parent_checkpoint(&mut latest_manifest, checkpoint_id, rand.clone()).await?;
    let mut manifest_at_checkpoint = manifest_store.read_manifest(checkpoint.manifest_id).await?;

    let source_ranges = source
        .projection_ranges
        .as_deref()
        .map(to_range_set)
        .transpose()?;
    let global_ranges = projection_ranges.as_deref().map(to_range_set).transpose()?;
    let ranges: Option<Vec<BytesRange>> = match (source_ranges, global_ranges) {
        // An empty intersection is kept as an empty set: the source then
        // contributes no keys.
        (Some(l), Some(r)) => Some(BytesRange::intersect_sets(&l, &r)),
        (Some(l), None) => Some(l),
        (None, Some(r)) => Some(r),
        (None, None) => None,
    };

    let config = ProjectionConfig {
        global_ranges: ranges,
        segment_filter: segment_filter.cloned(),
        segment_projection: segment_projection.cloned(),
    };
    manifest_at_checkpoint = if config.is_noop() {
        manifest_at_checkpoint
    } else {
        Manifest::projected(&manifest_at_checkpoint, &config, rand)?
    };

    Ok(CloneSource {
        path: source.path.clone(),
        manifest: manifest_at_checkpoint,
        checkpoint,
    })
}

fn copy_wal_params_for_source(source: &CloneSource, to_path: &Path) -> CopyWalParams {
    CopyWalParams {
        from_path: source.path.clone(),
        from_manifest: source.versioned_manifest(),
        to_path: to_path.clone(),
    }
}

async fn rebuild_source<R: RangeBounds<Bytes> + Clone>(
    source_spec: &CloneSourceSpec<R>,
    clone_manifest: &StoredManifest,
    object_store: &Arc<dyn ObjectStore>,
    system_clock: &Arc<dyn SystemClock>,
    rand: &Arc<DbRand>,
    projection_ranges: &Option<Vec<R>>,
    segment_filter: Option<&SegmentFilterFn>,
    segment_projection: Option<&SegmentProjectionFn>,
) -> Result<CloneSource, SlateDBError> {
    // `Manifest::cloned` appends the direct parent after inherited external DBs. Search in reverse
    // so a parent that also appears in its own ancestry still resolves to the direct source.
    let source_path = source_spec.path.to_string();
    let external_db = clone_manifest
        .manifest()
        .external_dbs
        .iter()
        .rev()
        .find(|external_db| external_db.path == source_path)
        .ok_or(SlateDBError::CloneExternalDbMissing)?;
    let manifest_store = Arc::new(ManifestStore::new(&source_spec.path, object_store.clone()));
    let latest_manifest = load_initialized_manifest(manifest_store, system_clock.clone()).await?;
    let checkpoint_id = external_db
        .final_checkpoint_id
        .filter(|checkpoint_id| {
            latest_manifest
                .db_state()
                .find_checkpoint(*checkpoint_id)
                .is_some()
        })
        .unwrap_or(external_db.source_checkpoint_id);
    build_source(
        source_spec,
        Some(checkpoint_id),
        object_store,
        system_clock,
        rand,
        projection_ranges,
        segment_filter,
        segment_projection,
    )
    .await
}

// Get a checkpoint and the corresponding manifest that will be used as the source
// for the clone's initial state.
//
// If `parent_checkpoint_id` is `None`, then create an ephemeral checkpoint from
// the latest state.  Making it ephemeral ensures that it will
// get cleaned up if the clone operation fails.
async fn get_or_create_parent_checkpoint(
    manifest: &mut StoredManifest,
    maybe_checkpoint_id: Option<Uuid>,
    rand: Arc<DbRand>,
) -> Result<Checkpoint, SlateDBError> {
    let checkpoint = match maybe_checkpoint_id {
        Some(checkpoint_id) => match manifest.db_state().find_checkpoint(checkpoint_id) {
            Some(found_checkpoint) => found_checkpoint.clone(),
            None => return Err(CheckpointMissing(checkpoint_id)),
        },
        None => {
            let checkpoint_id = rand.rng().gen_uuid();
            manifest
                .write_checkpoint(
                    checkpoint_id,
                    &CheckpointOptions {
                        lifetime: Some(Duration::from_secs(300)),
                        source: None,
                        name: None,
                    },
                )
                .await?
        }
    };
    Ok(checkpoint)
}

fn validate_clone_source_specs<R: RangeBounds<Bytes> + Clone>(
    specs: &[CloneSourceSpec<R>],
    clone_path: &Path,
) -> Result<(), SlateDBError> {
    if specs.is_empty() {
        return Err(SlateDBError::InvalidUnionSetEmpty());
    }

    let mut seen_paths = std::collections::HashSet::new();
    for source in specs {
        if clone_path == &source.path {
            return Err(SlateDBError::IdenticalClonePaths(clone_path.clone()));
        }
        if !seen_paths.insert(source.path.to_string()) {
            return Err(SlateDBError::DuplicatedCloneSourcePath(source.path.clone()));
        }
    }
    Ok(())
}

async fn validate_no_data_wal(
    sources: &[CloneSource],
    wal_admin: &dyn WalAdmin,
) -> Result<(), SlateDBError> {
    let mut parents_with_wal = vec![];
    for source in sources {
        let replay_after_wal_id = source.manifest.core.replay_after_wal_id;
        let wal_id_last_seen = source
            .manifest
            .core
            .next_wal_sst_id
            .checked_sub(1)
            .ok_or(SlateDBError::InvalidDBState)?;
        if !wal_admin
            .is_empty(&source.path, replay_after_wal_id, wal_id_last_seen)
            .await?
        {
            parents_with_wal.push(source.path.clone());
        }
    }
    if !parents_with_wal.is_empty() {
        return Err(SlateDBError::InvalidCloneSourceWithWal {
            paths: parents_with_wal,
        });
    }
    Ok(())
}

// Validate that the manifest is attached to an external database at specific checkpoint.
fn validate_attached_to_external_db(
    path: String,
    checkpoint_id: Option<Uuid>,
    clone_manifest: &StoredManifest,
) -> Result<(), SlateDBError> {
    let external_dbs = &clone_manifest.manifest().external_dbs;
    if external_dbs.is_empty() {
        return Err(SlateDBError::CloneExternalDbMissing);
    }
    if !external_dbs.iter().any(|external_db| {
        path == external_db.path
            && checkpoint_id
                .map(|id| id == external_db.source_checkpoint_id)
                .unwrap_or(true)
    }) {
        return Err(SlateDBError::CloneIncorrectExternalDbCheckpoint {
            path,
            checkpoint_id,
        });
    };
    Ok(())
}

async fn validate_external_dbs_contain_final_checkpoint(
    parent_manifest_store: Arc<ManifestStore>,
    parent_path: String,
    clone_manifest: &StoredManifest,
    object_store: Arc<dyn ObjectStore>,
) -> Result<(), SlateDBError> {
    // Validate external dbs all contain the final checkpoint
    for external_db in &clone_manifest.manifest().external_dbs {
        let Some(final_checkpoint_id) = external_db.final_checkpoint_id else {
            // If the final checkpoint id is not set, we can skip this check
            continue;
        };
        let external_manifest_store = if external_db.path == parent_path {
            parent_manifest_store.clone()
        } else {
            Arc::new(ManifestStore::new(
                &external_db.path.clone().into(),
                object_store.clone(),
            ))
        };
        let external_manifest = external_manifest_store
            .read_latest_manifest()
            .await?
            .manifest;
        if external_manifest
            .core
            .find_checkpoint(final_checkpoint_id)
            .is_none()
        {
            return Err(SlateDBError::CloneIncorrectFinalCheckpoint {
                path: external_db.path.clone(),
                checkpoint_id: final_checkpoint_id,
            });
        }
    }

    Ok(())
}

async fn load_initialized_manifest(
    manifest_store: Arc<ManifestStore>,
    system_clock: Arc<dyn SystemClock>,
) -> Result<StoredManifest, SlateDBError> {
    let Some(manifest) =
        StoredManifest::try_load(manifest_store.clone(), system_clock.clone()).await?
    else {
        return Err(SlateDBError::LatestTransactionalObjectVersionMissing);
    };

    if !manifest.db_state().initialized {
        return Err(SlateDBError::InvalidDBState);
    }

    Ok(manifest)
}

async fn copy_wal(
    wal_admin: &dyn WalAdmin,
    params: CopyWalParams,
) -> Result<(u64, u64), SlateDBError> {
    let CopyWalParams {
        from_path,
        from_manifest,
        to_path,
    } = params;
    wal_admin
        .clone_wal(&from_path, from_manifest, &to_path)
        .await
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::{SegmentFilterFn, SegmentProjectionFn};
    use crate::compactor::CompactorBuilder;
    use crate::config::{
        CheckpointOptions, CheckpointScope, CompactionWorkerOptions, CompactorOptions,
        FlushOptions, FlushType, PutOptions, Settings, WriteOptions,
    };
    use crate::db::builder::CloneSourceSpec;
    use crate::db::Db;
    use crate::db_reader::DbReader;
    use crate::error::SlateDBError;
    use crate::iter::IterationOrder;
    use crate::manifest::store::{ManifestStore, StoredManifest};
    use crate::manifest::Manifest;
    use crate::manifest::{ManifestCore, VersionedManifest};
    use crate::paths::PathResolver;
    use crate::proptest_util::{rng, sample};
    use crate::test_utils;
    use crate::utils::IdGenerator;
    use crate::wal::slatedb::admin::SlateDbWalAdmin;
    use crate::wal::slatedb::store::WalFileId;
    use crate::wal::{WalAdmin, WalError, WalFileRange, WalGc};
    use async_trait::async_trait;
    use bytes::Bytes;
    use fail_parallel::FailPointRegistry;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::Error as ObjectStoreError;
    use object_store::ObjectStore;
    use slatedb_common::clock::DefaultSystemClock;
    use slatedb_common::DbRand;
    use slatedb_common::SystemClock;
    use slatedb_txn_obj::TransactionalObject;
    use std::collections::BTreeMap;
    use std::ops::Bound;
    use std::ops::RangeBounds;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use uuid::Uuid;

    struct RemappingWalAdmin {
        replay_range: (u64, u64),
        expected_manifest_id: Option<u64>,
    }

    struct NoopWalGc;

    #[async_trait]
    impl WalGc for NoopWalGc {
        async fn collect(
            &self,
            _referenced_ranges: Vec<WalFileRange>,
            _min_age: Duration,
            _dry_run: bool,
        ) -> Result<(), WalError> {
            Ok(())
        }
    }

    #[async_trait]
    impl WalAdmin for RemappingWalAdmin {
        fn garbage_collector(&self, _path: &Path) -> Arc<dyn WalGc> {
            Arc::new(NoopWalGc)
        }

        async fn delete_wal(&self, _path: &Path, _dry_run: bool) -> Result<Vec<String>, WalError> {
            Ok(vec![])
        }

        async fn is_empty(
            &self,
            _path: &Path,
            _replay_after_wal_id: u64,
            _wal_id_last_seen: u64,
        ) -> Result<bool, WalError> {
            Ok(true)
        }

        async fn clone_wal(
            &self,
            _from_path: &Path,
            from_manifest: VersionedManifest,
            _to_path: &Path,
        ) -> Result<(u64, u64), WalError> {
            if let Some(expected_manifest_id) = self.expected_manifest_id {
                assert_eq!(from_manifest.id(), expected_manifest_id);
            }
            Ok(self.replay_range)
        }
    }

    async fn create_native_clone<P: Into<Path>, R: RangeBounds<Bytes> + Clone>(
        clone_sources: Vec<CloneSourceSpec<R>>,
        clone_path: P,
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Arc<dyn ObjectStore>,
        fp_registry: Arc<FailPointRegistry>,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
        projection_ranges: Option<Vec<R>>,
        segment_filter: Option<SegmentFilterFn>,
        segment_projection: Option<SegmentProjectionFn>,
    ) -> Result<(), SlateDBError> {
        let wal_admin = Arc::new(SlateDbWalAdmin::new(wal_object_store, fp_registry.clone()));
        crate::clone::create_clone(
            clone_sources,
            clone_path,
            object_store,
            wal_admin,
            fp_registry,
            system_clock,
            rand,
            projection_ranges,
            segment_filter,
            segment_projection,
        )
        .await
    }

    // helper method for tests that creates CloneSourceSpec
    async fn create_clone<P: Into<Path>>(
        clone_path: P,
        parent_path: P,
        object_store: Arc<dyn ObjectStore>,
        wal_object_store: Arc<dyn ObjectStore>,
        parent_checkpoint: Option<Uuid>,
        fp_registry: Arc<FailPointRegistry>,
        system_clock: Arc<dyn SystemClock>,
        rand: Arc<DbRand>,
    ) -> Result<(), SlateDBError> {
        let source: CloneSourceSpec = match parent_checkpoint {
            Some(cp) => CloneSourceSpec::with_checkpoint(parent_path, cp),
            None => CloneSourceSpec::new(parent_path),
        };
        create_native_clone(
            vec![source],
            clone_path,
            object_store,
            wal_object_store,
            fp_registry,
            system_clock,
            rand,
            None,
            None,
            None,
        )
        .await
    }

    #[tokio::test]
    async fn should_stamp_wal_range_returned_by_wal_admin() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_remapped_wal");
        let clone_path = Path::from("/tmp/test_clone_remapped_wal");
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        let mut parent_manifest = StoredManifest::create_new_db(
            Arc::new(ManifestStore::new(&parent_path, object_store.clone())),
            ManifestCore::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();
        let checkpoint = parent_manifest
            .write_checkpoint(Uuid::new_v4(), &CheckpointOptions::default())
            .await
            .unwrap();

        let wal_admin = RemappingWalAdmin {
            replay_range: (41, 46),
            expected_manifest_id: Some(checkpoint.manifest_id),
        };
        let source: CloneSourceSpec = CloneSourceSpec::with_checkpoint(parent_path, checkpoint.id);
        crate::clone::create_clone(
            vec![source],
            clone_path.clone(),
            object_store.clone(),
            Arc::new(wal_admin),
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            Arc::new(DbRand::default()),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let manifest = StoredManifest::load(
            Arc::new(ManifestStore::new(&clone_path, object_store)),
            system_clock,
        )
        .await
        .unwrap();
        assert!(manifest.db_state().initialized);
        assert_eq!(manifest.db_state().replay_after_wal_id, 41);
        assert_eq!(manifest.db_state().next_wal_sst_id, 47);
    }

    #[tokio::test]
    async fn should_reset_wal_range_when_clone_does_not_copy_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_paths = [
            Path::from("/tmp/test_parent_no_wal_a"),
            Path::from("/tmp/test_parent_no_wal_b"),
        ];
        let clone_path = Path::from("/tmp/test_clone_no_wal");
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());

        for parent_path in &parent_paths {
            StoredManifest::create_new_db(
                Arc::new(ManifestStore::new(parent_path, object_store.clone())),
                ManifestCore::new(),
                system_clock.clone(),
            )
            .await
            .unwrap();
        }

        crate::clone::create_clone(
            parent_paths.into_iter().map(CloneSourceSpec::new).collect(),
            clone_path.clone(),
            object_store.clone(),
            Arc::new(RemappingWalAdmin {
                replay_range: (41, 47),
                expected_manifest_id: None,
            }),
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            Arc::new(DbRand::default()),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let manifest = StoredManifest::load(
            Arc::new(ManifestStore::new(&clone_path, object_store)),
            system_clock,
        )
        .await
        .unwrap();
        assert!(manifest.db_state().initialized);
        assert_eq!(manifest.db_state().replay_after_wal_id, 0);
        assert_eq!(manifest.db_state().next_wal_sst_id, 1);
    }

    #[tokio::test]
    async fn should_clone_latest_state_if_no_checkpoint_provided() {
        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 5000, 10);

        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        let parent_db = Db::open(parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &table, false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap();

        let clone_db = Db::open(clone_path.clone(), object_store.clone())
            .await
            .unwrap();
        let mut db_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&table, .., IterationOrder::Ascending, &mut db_iter)
            .await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_read_clone_with_db_reader() {
        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 5000, 10);

        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");

        let parent_db = Db::open(parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &table, false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        // Flush the memtable so the parent's data lives in L0 SSTs, which the
        // clone references as external SSTs instead of replaying WALs.
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap();

        // Sanity check that reads must resolve parent-resident SSTs.
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        let clone_manifest = clone_manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .manifest;
        assert!(!clone_manifest.external_ssts().is_empty());

        let reader = DbReader::builder(clone_path.clone(), object_store.clone())
            .build()
            .await
            .unwrap();
        let mut db_iter = reader.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&table, .., IterationOrder::Ascending, &mut db_iter)
            .await;
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_read_clone_with_db_reader_from_checkpoint_with_pruned_external_ssts() {
        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 5000, 10);

        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand = Arc::new(DbRand::default());

        let parent_db = Db::open(parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &table, false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap();

        // Pin a checkpoint to the clone's current manifest, which references
        // the parent's SSTs externally.
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        let mut clone_sm = StoredManifest::load(clone_manifest_store.clone(), system_clock.clone())
            .await
            .unwrap();
        let checkpoint_id = rand.rng().gen_uuid();
        clone_sm
            .write_checkpoint(checkpoint_id, &CheckpointOptions::default())
            .await
            .unwrap();

        // Simulate a post-checkpoint compaction that re-localized all external
        // SSTs and pruned their ids from the latest manifest. The checkpoint's
        // manifest still references them.
        clone_sm
            .maybe_apply_update(|sr| {
                let mut dirty = sr.prepare_dirty()?;
                dirty
                    .value
                    .external_dbs
                    .iter_mut()
                    .for_each(|external_db| external_db.sst_ids.clear());
                Ok(Some(dirty))
            })
            .await
            .unwrap();
        let latest_manifest = clone_manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .manifest;
        assert!(latest_manifest.external_ssts().is_empty());

        // A reader pinned to the checkpoint must resolve the external SSTs
        // referenced by the checkpoint's manifest.
        let reader = DbReader::builder(clone_path.clone(), object_store.clone())
            .with_reader_mode(crate::DbReaderMode::Checkpoint(checkpoint_id))
            .build()
            .await
            .unwrap();
        let mut db_iter = reader.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&table, .., IterationOrder::Ascending, &mut db_iter)
            .await;
        reader.close().await.unwrap();
    }

    // A clone lists every ancestor it inherits. Once compaction has re-localized all of an
    // ancestor's SSTs, the detach collector releases the parent's pin on that ancestor. A
    // clone of a checkpoint taken before the release reads nothing from the ancestor, so it
    // must not need the pin.
    #[tokio::test]
    async fn should_clone_checkpoint_whose_emptied_ancestor_was_detached() {
        let mut rng = rng::new_test_rng(None);
        let table = sample::table(&mut rng, 1000, 10);

        let object_store = Arc::new(InMemory::new());
        let grandparent_path = Path::from("/tmp/test_grandparent");
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let rand = Arc::new(DbRand::default());

        // The grandparent owns no SSTs, so the parent's entry for it is empty from the start,
        // as it is once compaction has re-localized everything the parent borrowed.
        Db::open(grandparent_path.clone(), object_store.clone())
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        create_clone(
            parent_path.clone(),
            grandparent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap();

        // The parent's own collector would detach the empty entry as soon as it runs; the
        // test releases the pin itself below, after the checkpoint that still lists it.
        let parent_db = Db::builder(parent_path.clone(), object_store.clone())
            .with_settings(Settings {
                garbage_collector_options: None,
                ..Settings::default()
            })
            .build()
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &table, false)
            .await
            .unwrap();
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        let checkpoint = parent_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        // Release the parent's pin on the grandparent, as the detach collector does.
        let checkpointed = ManifestStore::new(&parent_path, object_store.clone())
            .read_manifest(checkpoint.manifest_id)
            .await
            .unwrap();
        let ancestor = checkpointed
            .external_dbs
            .iter()
            .find(|e| e.path == grandparent_path.to_string())
            .expect("the checkpoint lists the grandparent");
        assert!(ancestor.sst_ids.is_empty());
        let mut grandparent_manifest = StoredManifest::load(
            Arc::new(ManifestStore::new(&grandparent_path, object_store.clone())),
            system_clock.clone(),
        )
        .await
        .unwrap();
        grandparent_manifest
            .delete_checkpoint(ancestor.final_checkpoint_id.unwrap())
            .await
            .unwrap();

        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint.id),
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap();

        let clone_db = Db::open(clone_path.clone(), object_store.clone())
            .await
            .unwrap();
        let mut db_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&table, .., IterationOrder::Ascending, &mut db_iter)
            .await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_clone_from_checkpoint_wal_enabled() {
        should_clone_from_checkpoint(Settings::default()).await
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_clone_from_checkpoint_wal_disabled() {
        should_clone_from_checkpoint(Settings {
            wal_enabled: false,
            ..Settings::default()
        })
        .await
    }

    async fn should_clone_from_checkpoint(db_opts: Settings) {
        let mut rng = rng::new_test_rng(None);
        let checkpoint_table = sample::table(&mut rng, 5000, 10);
        let post_checkpoint_table = sample::table(&mut rng, 1000, 10);

        let object_store = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";

        let parent_db = Db::builder(parent_path, object_store.clone())
            .with_settings(db_opts.clone())
            .build()
            .await
            .unwrap();
        test_utils::seed_database(&parent_db, &checkpoint_table, false)
            .await
            .unwrap();
        let checkpoint = parent_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await
            .unwrap();

        // Add some more data so that we can be sure that the clone was created
        // from the checkpoint and not the latest state.
        test_utils::seed_database(&parent_db, &post_checkpoint_table, false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint.id),
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap();

        let clone_db = Db::builder(clone_path, object_store.clone())
            .with_settings(db_opts)
            .build()
            .await
            .unwrap();
        let mut db_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(
            &checkpoint_table,
            ..,
            IterationOrder::Ascending,
            &mut db_iter,
        )
        .await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_fail_retry_if_uninitialized_checkpoint_is_invalid() {
        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        // Create the parent with empty state
        let parent_db = Db::open(parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        // Create an uninitialized manifest with an invalid checkpoint id
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        let non_existent_source_checkpoint_id = Uuid::new_v4();
        StoredManifest::store_uninitialized_clone(
            clone_manifest_store,
            Manifest::cloned(
                &Manifest::initial(ManifestCore::new()),
                parent_path.to_string(),
                non_existent_source_checkpoint_id,
                rand.clone(),
            ),
            system_clock.clone(),
        )
        .await
        .unwrap();

        // Cloning should reset the checkpoint to a newly generated id
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(err, SlateDBError::CheckpointMissing(id) if id == non_existent_source_checkpoint_id)
        );
    }

    #[tokio::test]
    async fn should_fail_retry_if_uninitialized_checkpoint_differs_from_provided() {
        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        // Create the parent with empty state
        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, object_store.clone()));
        let mut parent_sm = StoredManifest::create_new_db(
            parent_manifest_store,
            ManifestCore::new(),
            system_clock.clone(),
        )
        .await
        .unwrap();
        let uuid_1 = rand.rng().gen_uuid();
        let checkpoint_1 = parent_sm
            .write_checkpoint(uuid_1, &CheckpointOptions::default())
            .await
            .unwrap();
        let uuid_2 = rand.rng().gen_uuid();
        let checkpoint_2 = parent_sm
            .write_checkpoint(uuid_2, &CheckpointOptions::default())
            .await
            .unwrap();

        // Create an uninitialized manifest referring to the first checkpoint
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        StoredManifest::store_uninitialized_clone(
            clone_manifest_store,
            Manifest::cloned(
                &Manifest::initial(ManifestCore::new()),
                parent_path.to_string(),
                checkpoint_1.id,
                rand.clone(),
            ),
            system_clock.clone(),
        )
        .await
        .unwrap();

        // Cloning with the second checkpoint should fail
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint_2.id),
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            SlateDBError::CloneIncorrectExternalDbCheckpoint { .. }
        ));
    }

    #[tokio::test]
    async fn should_fail_retry_if_parent_path_is_different() {
        let object_store = Arc::new(InMemory::new());
        let original_parent_path = Path::from("/tmp/test_parent");
        let updated_parent_path = Path::from("/tmp/test_parent/new");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        // Setup an uninitialized manifest pointing to a different parent
        let parent_manifest = Manifest::initial(ManifestCore::new());
        let clone_manifest_store = Arc::new(ManifestStore::new(&clone_path, object_store.clone()));
        StoredManifest::store_uninitialized_clone(
            clone_manifest_store,
            Manifest::cloned(
                &parent_manifest,
                original_parent_path.to_string(),
                Uuid::new_v4(),
                rand.clone(),
            ),
            system_clock.clone(),
        )
        .await
        .unwrap();

        // Initialize the parent at the updated path
        let parent_db = Db::open(updated_parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        // The clone should fail because of inconsistent parent information
        let err = create_clone(
            clone_path.clone(),
            updated_parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();

        assert!(matches!(
            err,
            SlateDBError::CloneIncorrectExternalDbCheckpoint { .. }
        ));
    }

    #[tokio::test]
    async fn clone_retry_should_be_idempotent_after_success() -> Result<(), SlateDBError> {
        let object_store = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let parent_db = Db::open(parent_path, object_store.clone()).await.unwrap();
        parent_db.close().await.unwrap();

        create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap();

        let clone_manifest_store =
            ManifestStore::new(&Path::from(clone_path), object_store.clone());
        let manifest_id = clone_manifest_store
            .read_latest_manifest()
            .await
            .unwrap()
            .id;

        create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            rand.clone(),
        )
        .await?;

        assert_eq!(
            manifest_id,
            clone_manifest_store
                .read_latest_manifest()
                .await
                .unwrap()
                .id
        );

        Ok(())
    }

    #[tokio::test]
    async fn should_retry_clone_after_io_error_copying_wals() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let parent_db = Db::builder(parent_path.clone(), object_store.clone())
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();
        let mut rng = rng::new_test_rng(None);
        test_utils::seed_database(&parent_db, &sample::table(&mut rng, 100, 10), false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();

        test_utils::seed_database(&parent_db, &sample::table(&mut rng, 100, 10), false)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        // Block L0 uploads so the data remains in the WAL after close.
        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();
        // expect to fail since l0 flush is blocked
        assert!(parent_db.close().await.is_err());
        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "write-compacted-sst-io-error",
            "off",
        )
        .unwrap();

        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "copy-wal-ssts-io-error",
            "1*off->return",
        )
        .unwrap();

        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::clone(&fp_registry),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SlateDBError::WalUnavailable(_)));

        fail_parallel::cfg(Arc::clone(&fp_registry), "copy-wal-ssts-io-error", "off").unwrap();
        create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::clone(&fp_registry),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn should_fail_retry_if_source_checkpoint_is_missing() -> Result<(), crate::Error> {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent");
        let clone_path = Path::from("/tmp/test_clone");
        let rand = Arc::new(DbRand::default());
        let system_clock = Arc::new(DefaultSystemClock::new());

        let parent_db = Db::open(parent_path.clone(), object_store.clone()).await?;
        let mut rng = rng::new_test_rng(None);
        test_utils::seed_database(&parent_db, &sample::table(&mut rng, 100, 10), false).await?;
        let checkpoint = parent_db
            .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
            .await?;
        parent_db.close().await?;

        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "create-clone-manifest-io-error",
            "return",
        )
        .unwrap();

        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint.id),
            Arc::clone(&fp_registry),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SlateDBError::IoError(_)));

        fail_parallel::cfg(
            Arc::clone(&fp_registry),
            "create-clone-manifest-io-error",
            "off",
        )
        .unwrap();

        // Delete the checkpoint from the parent database
        let parent_manifest_store =
            Arc::new(ManifestStore::new(&parent_path, object_store.clone()));
        let mut parent_manifest =
            StoredManifest::load(parent_manifest_store, system_clock.clone()).await?;
        parent_manifest.delete_checkpoint(checkpoint.id).await?;

        // Attempting to clone with a missing checkpoint should fail
        let err = create_clone(
            clone_path.clone(),
            parent_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Some(checkpoint.id),
            Arc::clone(&fp_registry),
            system_clock.clone(),
            rand.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, SlateDBError::CheckpointMissing(id) if id == checkpoint.id));

        Ok(())
    }

    #[tokio::test]
    async fn clone_should_succeed_when_wal_object_store_is_provided() {
        let object_store = Arc::new(InMemory::new());
        let wal_object_store = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";

        let parent_db = Db::builder(parent_path, object_store.clone())
            .with_wal_object_store(wal_object_store.clone())
            .build()
            .await
            .unwrap();
        let write_options = WriteOptions {
            ..Default::default()
        };
        let put_options = PutOptions::default();
        let l0_and_wal_data = [
            (b"l0-key-1".as_slice(), b"l0-value-1".as_slice()),
            (b"l0-key-2".as_slice(), b"l0-value-2".as_slice()),
        ];
        let wal_only_data = [
            (b"wal-only-key-1".as_slice(), b"wal-only-value-1".as_slice()),
            (b"wal-only-key-2".as_slice(), b"wal-only-value-2".as_slice()),
        ];
        for &(key, value) in &l0_and_wal_data {
            parent_db
                .put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
        parent_db.flush().await.unwrap();
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        for &(key, value) in &wal_only_data {
            parent_db
                .put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
        parent_db.flush().await.unwrap();
        let manifest = parent_db.manifest();
        assert!(
            !manifest.manifest.core.tree.l0.is_empty(),
            "expected cloned state to include L0 data"
        );
        assert!(
            manifest.manifest.core.replay_after_wal_id + 1 < manifest.manifest.core.next_wal_sst_id,
            "expected cloned state to retain WAL-only SSTs"
        );
        parent_db.close().await.unwrap();

        create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            wal_object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap();

        let clone_db = Db::builder(clone_path, object_store.clone())
            .with_wal_object_store(wal_object_store.clone())
            .build()
            .await
            .unwrap();
        for &(key, value) in &l0_and_wal_data {
            assert_eq!(
                clone_db.get(key).await.unwrap(),
                Some(Bytes::copy_from_slice(value))
            );
        }
        for &(key, value) in &wal_only_data {
            assert_eq!(
                clone_db.get(key).await.unwrap(),
                Some(Bytes::copy_from_slice(value))
            );
        }
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn clone_should_fail_when_wal_store_is_not_provided() {
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store = Arc::new(InMemory::new());
        let wal_object_store = Arc::new(InMemory::new());
        let parent_path = "/tmp/test_parent";
        let clone_path = "/tmp/test_clone";

        let parent_db = Db::builder(parent_path, object_store.clone())
            .with_wal_object_store(wal_object_store.clone())
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();
        let write_options = WriteOptions {
            ..Default::default()
        };
        let put_options = PutOptions::default();
        let l0_and_wal_data = [
            (b"l0-key-1".as_slice(), b"l0-value-1".as_slice()),
            (b"l0-key-2".as_slice(), b"l0-value-2".as_slice()),
        ];
        let wal_only_data = [
            (b"wal-only-key-1".as_slice(), b"wal-only-value-1".as_slice()),
            (b"wal-only-key-2".as_slice(), b"wal-only-value-2".as_slice()),
        ];
        for &(key, value) in &l0_and_wal_data {
            parent_db
                .put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
        parent_db.flush().await.unwrap();
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        for &(key, value) in &wal_only_data {
            parent_db
                .put_with_options(key, value, &put_options, &write_options)
                .await
                .unwrap();
        }
        parent_db.flush().await.unwrap();
        let manifest = parent_db.manifest();
        assert!(
            !manifest.manifest.core.tree.l0.is_empty(),
            "expected cloned state to include L0 data"
        );
        assert!(
            manifest.manifest.core.replay_after_wal_id + 1 < manifest.manifest.core.next_wal_sst_id,
            "expected cloned state to retain WAL-only SSTs"
        );
        let expected_missing_wal_path = PathResolver::from_root(Path::from(parent_path))
            .wal_sst_path(&WalFileId::from(
                manifest.manifest.core.replay_after_wal_id + 1,
            ))
            .to_string();
        // Block L0 uploads so the WAL-only data stays in the WAL.
        fail_parallel::cfg(
            fp_registry.clone(),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();
        // expect to fail since l0 upload is blocked
        assert!(parent_db.close().await.is_err());
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "off").unwrap();

        // Pass main store as WAL store — WAL SSTs won't be found there
        let err = create_clone(
            clone_path,
            parent_path,
            object_store.clone(),
            object_store.clone(),
            None,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            SlateDBError::WalUnavailable(ref source)
                if matches!(
                    source.downcast_ref::<ObjectStoreError>(),
                    Some(ObjectStoreError::NotFound { path, .. })
                        if path == &expected_missing_wal_path
                )
        ));
    }

    #[tokio::test]
    async fn should_disallow_projected_clone_when_source_has_data_wal() {
        // Data that only lives in the parent's WAL at the checkpoint is copied
        // to the clone verbatim and replayed in full on first open, so a
        // projection cannot be applied to it. Cloning with a projection must
        // fail while the source still has data in its WAL, and succeed once
        // that data has been flushed into L0.
        let fp_registry = Arc::new(FailPointRegistry::new());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_wal_projection");
        let clone_path = Path::from("/tmp/test_clone_wal_projection");

        let parent_db = Db::builder(parent_path.clone(), object_store.clone())
            .with_fp_registry(fp_registry.clone())
            .build()
            .await
            .unwrap();
        let write_options = WriteOptions::default();
        let put_options = PutOptions::default();

        // Keys inside and outside the projection range [aaa, bbb), flushed
        // through to L0 ...
        parent_db
            .put_with_options(b"aaa-l0", b"v1", &put_options, &write_options)
            .await
            .unwrap();
        parent_db
            .put_with_options(b"zzz-l0", b"v2", &put_options, &write_options)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();

        // ... and the same shape of data made durable only in the WAL.
        parent_db
            .put_with_options(b"aaa-wal", b"v3", &put_options, &write_options)
            .await
            .unwrap();
        parent_db
            .put_with_options(b"zzz-wal", b"v4", &put_options, &write_options)
            .await
            .unwrap();
        parent_db.flush().await.unwrap();

        let manifest = parent_db.manifest();
        assert!(
            !manifest.manifest.core.tree.l0.is_empty(),
            "expected parent state to include L0 data"
        );
        assert!(
            manifest.manifest.core.replay_after_wal_id + 1 < manifest.manifest.core.next_wal_sst_id,
            "expected parent state to retain WAL-only SSTs"
        );

        // Block L0 uploads so the WAL-only data stays in the WAL.
        fail_parallel::cfg(
            fp_registry.clone(),
            "write-compacted-sst-io-error",
            "return",
        )
        .unwrap();
        // expect to fail since l0 upload is blocked
        assert!(parent_db.close().await.is_err());
        fail_parallel::cfg(fp_registry.clone(), "write-compacted-sst-io-error", "off").unwrap();

        // Cloning with a projection that keeps only keys in [aaa, bbb) must
        // be rejected while the WAL-only data is still in the WAL.
        let range = (
            Bound::Included(Bytes::from_static(b"aaa")),
            Bound::Excluded(Bytes::from_static(b"bbb")),
        );
        let err = create_native_clone(
            vec![CloneSourceSpec::new(parent_path.clone())],
            clone_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
            Some(vec![range.clone()]),
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            SlateDBError::InvalidCloneSourceWithWal { ref paths }
                if paths == &vec![parent_path.clone()]
        ));

        // Reopen the parent so the WAL tail is replayed, flush it into L0,
        // and close cleanly. With no data WALs left to copy the projected
        // clone is allowed.
        let parent_db = Db::open(parent_path.clone(), object_store.clone())
            .await
            .unwrap();
        parent_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        parent_db.close().await.unwrap();

        create_native_clone(
            vec![CloneSourceSpec::new(parent_path.clone())],
            clone_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
            Some(vec![range]),
            None,
            None,
        )
        .await
        .unwrap();

        let clone_db = Db::open(clone_path.clone(), object_store.clone())
            .await
            .unwrap();

        // L0 data respects the projection.
        assert_eq!(
            clone_db.get(b"aaa-l0").await.unwrap(),
            Some(Bytes::from_static(b"v1"))
        );
        assert_eq!(
            clone_db.get(b"zzz-l0").await.unwrap(),
            None,
            "L0 entry outside the projection range must not be visible in the clone"
        );

        // The formerly WAL-only data was flushed into L0 before the retry,
        // so it must respect the projection too.
        assert_eq!(
            clone_db.get(b"aaa-wal").await.unwrap(),
            Some(Bytes::from_static(b"v3"))
        );
        assert_eq!(
            clone_db.get(b"zzz-wal").await.unwrap(),
            None,
            "entry outside the projection range must not be visible in the clone"
        );
        clone_db.close().await.unwrap();
    }

    fn segmented_table() -> BTreeMap<Bytes, Bytes> {
        BTreeMap::from([
            (Bytes::from_static(b"aaa-001"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"aaa-003"), Bytes::from_static(b"v2")),
            (Bytes::from_static(b"bbb-001"), Bytes::from_static(b"v3")),
            (Bytes::from_static(b"bbb-002"), Bytes::from_static(b"v4")),
            (Bytes::from_static(b"ddd-001"), Bytes::from_static(b"v5")),
            (Bytes::from_static(b"ddd-004"), Bytes::from_static(b"v6")),
        ])
    }

    #[cfg(feature = "wal_disable")]
    fn wal_disabled_settings() -> Settings {
        Settings {
            wal_enabled: false,
            ..Settings::default()
        }
    }

    async fn build_segmented_parent(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        extractor: Arc<dyn crate::prefix_extractor::PrefixExtractor>,
        settings: Settings,
        table: &BTreeMap<Bytes, Bytes>,
    ) {
        #[cfg(feature = "wal_disable")]
        let wal_enabled = settings.wal_enabled;
        #[cfg(not(feature = "wal_disable"))]
        let wal_enabled = true;
        let db = Db::builder(path.clone(), object_store)
            .with_settings(settings)
            .with_segment_extractor(extractor)
            .build()
            .await
            .unwrap();
        // Do not await the returned handle here: with wal_enabled=false, the
        // memtable flush is gated on the explicit call below.
        test_utils::seed_database(&db, table, false).await.unwrap();
        if wal_enabled {
            // Flush the WAL before the memtable so that `replay_after_wal_id`
            // covers every data WAL; projected clones of this parent would
            // otherwise be rejected.
            db.flush().await.unwrap();
        }
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    async fn open_segmented_clone(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        extractor: Arc<dyn crate::prefix_extractor::PrefixExtractor>,
        settings: Settings,
    ) -> Db {
        Db::builder(path.clone(), object_store)
            .with_settings(settings)
            .with_segment_extractor(extractor)
            .build()
            .await
            .unwrap()
    }

    async fn run_segmented_clone<R: RangeBounds<Bytes> + Clone>(
        sources: Vec<CloneSourceSpec<R>>,
        clone_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        projection: Option<R>,
    ) {
        create_native_clone(
            sources,
            clone_path.clone(),
            object_store.clone(),
            object_store,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
            projection.map(|range| vec![range]),
            None,
            None,
        )
        .await
        .unwrap();
    }

    async fn assert_clone_segments(
        clone_path: &Path,
        object_store: Arc<dyn ObjectStore>,
        expected_prefixes: &[&[u8]],
    ) {
        let store = ManifestStore::new(clone_path, object_store);
        let stored = store.read_latest_manifest().await.unwrap();
        assert_eq!(
            stored.manifest.core.segment_extractor_name.as_deref(),
            Some("fixed-3")
        );
        let actual: Vec<Bytes> = stored
            .manifest
            .core
            .segments
            .iter()
            .map(|s| s.prefix.clone())
            .collect();
        let want: Vec<Bytes> = expected_prefixes
            .iter()
            .map(|b| Bytes::copy_from_slice(b))
            .collect();
        assert_eq!(actual, want);
    }

    async fn assert_segment_prefix_scan(
        db: &Db,
        expected: &BTreeMap<Bytes, Bytes>,
        prefix_lo: &'static [u8],
        prefix_hi: &'static [u8],
    ) {
        let mut iter = db.scan_prefix(prefix_lo, ..).await.unwrap();
        test_utils::assert_ranged_db_scan(
            expected,
            Bytes::from_static(prefix_lo)..Bytes::from_static(prefix_hi),
            IterationOrder::Ascending,
            &mut iter,
        )
        .await;
    }

    #[tokio::test]
    async fn should_filter_segments_via_clone_builder() {
        // Drop the `bbb` segment using filter_segments; verify only `aaa` and
        // `ddd` remain in the clone.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_seg_filter");
        let clone_path = Path::from("/tmp/test_clone_seg_filter");
        let extractor = Arc::new(test_utils::FixedThreeBytePrefixExtractor);
        let table = segmented_table();

        build_segmented_parent(
            &parent_path,
            object_store.clone(),
            extractor.clone(),
            Settings::default(),
            &table,
        )
        .await;

        crate::db::builder::CloneBuilder::new(
            clone_path.clone(),
            CloneSourceSpec::new(parent_path.clone()),
            object_store.clone(),
        )
        .with_wal_object_store(object_store.clone())
        .with_segment_filter(|prefix| prefix != b"bbb")
        .build()
        .await
        .unwrap();

        assert_clone_segments(&clone_path, object_store.clone(), &[b"aaa", b"ddd"]).await;
    }

    #[tokio::test]
    async fn should_apply_segment_projection_via_clone_builder() {
        // Narrow the `aaa` segment to only keys >= "aaa-002" via
        // with_segment_projection; other segments retain full ranges.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_seg_proj");
        let clone_path = Path::from("/tmp/test_clone_seg_proj");
        let extractor = Arc::new(test_utils::FixedThreeBytePrefixExtractor);
        let table = segmented_table();

        build_segmented_parent(
            &parent_path,
            object_store.clone(),
            extractor.clone(),
            Settings::default(),
            &table,
        )
        .await;

        crate::db::builder::CloneBuilder::new(
            clone_path.clone(),
            CloneSourceSpec::new(parent_path.clone()),
            object_store.clone(),
        )
        .with_wal_object_store(object_store.clone())
        .with_segment_projection(|prefix| {
            if prefix == b"aaa" {
                let mut start = prefix.to_vec();
                start.extend_from_slice(b"-002");
                (Bound::Included(Bytes::from(start)), Bound::Unbounded)
            } else {
                (Bound::Unbounded, Bound::Unbounded)
            }
        })
        .build()
        .await
        .unwrap();

        let clone_db = open_segmented_clone(
            &clone_path,
            object_store.clone(),
            extractor,
            Settings::default(),
        )
        .await;
        // aaa-001 was filtered out by the projection; aaa-003 remains. Other
        // segments are untouched.
        let mut expected = table.clone();
        expected.remove(&Bytes::from_static(b"aaa-001"));
        let mut full_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&expected, .., IterationOrder::Ascending, &mut full_iter)
            .await;
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_clone_segmented_db() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_seg_clone");
        let clone_path = Path::from("/tmp/test_clone_seg_clone");
        let extractor = Arc::new(test_utils::FixedThreeBytePrefixExtractor);
        let table = segmented_table();

        build_segmented_parent(
            &parent_path,
            object_store.clone(),
            extractor.clone(),
            Settings::default(),
            &table,
        )
        .await;

        run_segmented_clone(
            vec![CloneSourceSpec::new(parent_path.clone())],
            &clone_path,
            object_store.clone(),
            None,
        )
        .await;

        assert_clone_segments(&clone_path, object_store.clone(), &[b"aaa", b"bbb", b"ddd"]).await;

        let clone_db = open_segmented_clone(
            &clone_path,
            object_store.clone(),
            extractor,
            Settings::default(),
        )
        .await;
        let mut full_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&table, .., IterationOrder::Ascending, &mut full_iter)
            .await;
        assert_segment_prefix_scan(&clone_db, &table, b"bbb", b"bbc").await;
        let mut cross_iter = clone_db
            .scan(b"aaa".to_vec()..=b"ddd-999".to_vec())
            .await
            .unwrap();
        test_utils::assert_ranged_db_scan(
            &table,
            Bytes::from_static(b"aaa")..=Bytes::from_static(b"ddd-999"),
            IterationOrder::Ascending,
            &mut cross_iter,
        )
        .await;
        clone_db.close().await.unwrap();
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_union_segmented_dbs() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path_a = Path::from("/tmp/test_parent_seg_union_a");
        let parent_path_b = Path::from("/tmp/test_parent_seg_union_b");
        let clone_path = Path::from("/tmp/test_clone_seg_union");
        let extractor = Arc::new(test_utils::FixedThreeBytePrefixExtractor);
        let settings = wal_disabled_settings();

        let table_a = BTreeMap::from([
            (Bytes::from_static(b"aaa-001"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"bbb-001"), Bytes::from_static(b"v2")),
            (Bytes::from_static(b"bbb-002"), Bytes::from_static(b"v3")),
        ]);
        let table_b = BTreeMap::from([
            (Bytes::from_static(b"ddd-001"), Bytes::from_static(b"v4")),
            (Bytes::from_static(b"eee-001"), Bytes::from_static(b"v5")),
            (Bytes::from_static(b"eee-002"), Bytes::from_static(b"v6")),
        ]);

        build_segmented_parent(
            &parent_path_a,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table_a,
        )
        .await;
        build_segmented_parent(
            &parent_path_b,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table_b,
        )
        .await;

        run_segmented_clone(
            vec![
                CloneSourceSpec::new(parent_path_a.clone()),
                CloneSourceSpec::new(parent_path_b.clone()),
            ],
            &clone_path,
            object_store.clone(),
            None,
        )
        .await;

        assert_clone_segments(
            &clone_path,
            object_store.clone(),
            &[b"aaa", b"bbb", b"ddd", b"eee"],
        )
        .await;
        let store = ManifestStore::new(&clone_path, object_store.clone());
        let stored = store.read_latest_manifest().await.unwrap();
        assert_eq!(stored.manifest.external_dbs.len(), 2);

        let mut expected: BTreeMap<Bytes, Bytes> = table_a.clone();
        expected.extend(table_b.clone());
        let clone_db =
            open_segmented_clone(&clone_path, object_store.clone(), extractor, settings).await;
        let mut full_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&expected, .., IterationOrder::Ascending, &mut full_iter)
            .await;
        assert_segment_prefix_scan(&clone_db, &expected, b"bbb", b"bbc").await;
        assert_segment_prefix_scan(&clone_db, &expected, b"eee", b"eef").await;
        clone_db.close().await.unwrap();
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_union_segmented_shards_that_each_span_every_segment() {
        // Rescale-down of a store keyed `data/{tenant}/…` and
        // `idx/{tenant}/…`, sharded by tenant. Each shard holds part of both
        // segments, so the shards' overall key ranges overlap —
        // `data/metro…` sorts below `idx/bronx…` — while neither segment
        // does. One union call must merge them; no per-source projection and
        // no staged re-slicing clones are needed.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path_a = Path::from("/tmp/test_parent_seg_interleaved_a");
        let parent_path_b = Path::from("/tmp/test_parent_seg_interleaved_b");
        let clone_path = Path::from("/tmp/test_clone_seg_interleaved");
        let extractor = Arc::new(test_utils::DataIdxPrefixExtractor);
        let settings = wal_disabled_settings();

        fn shard(tenants: [&str; 2]) -> BTreeMap<Bytes, Bytes> {
            let mut table = BTreeMap::new();
            for tenant in tenants {
                table.insert(
                    Bytes::from(format!("data/{}/animal/lion-1", tenant)),
                    Bytes::from(format!("{} lion", tenant)),
                );
                table.insert(
                    Bytes::from(format!("idx/{}/owner/alice/lion-1", tenant)),
                    Bytes::new(),
                );
            }
            table
        }
        let table_a = shard(["bronx", "lincoln"]);
        let table_b = shard(["metro", "oakland"]);

        build_segmented_parent(
            &parent_path_a,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table_a,
        )
        .await;
        build_segmented_parent(
            &parent_path_b,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table_b,
        )
        .await;

        run_segmented_clone(
            vec![
                CloneSourceSpec::new(parent_path_a.clone()),
                CloneSourceSpec::new(parent_path_b.clone()),
            ],
            &clone_path,
            object_store.clone(),
            None,
        )
        .await;

        // Both shards contribute an L0 SST to each of the two segments.
        let store = ManifestStore::new(&clone_path, object_store.clone());
        let stored = store.read_latest_manifest().await.unwrap();
        assert_eq!(
            stored.manifest.core.segment_extractor_name.as_deref(),
            Some("data-idx")
        );
        let segments: Vec<(Bytes, usize)> = stored
            .manifest
            .core
            .segments
            .iter()
            .map(|s| (s.prefix.clone(), s.tree.l0.len()))
            .collect();
        assert_eq!(
            segments,
            vec![
                (Bytes::from_static(b"data"), 2),
                (Bytes::from_static(b"idx"), 2)
            ]
        );
        assert_eq!(stored.manifest.external_dbs.len(), 2);

        let mut expected: BTreeMap<Bytes, Bytes> = table_a.clone();
        expected.extend(table_b.clone());

        let clone_db =
            open_segmented_clone(&clone_path, object_store.clone(), extractor, settings).await;
        let mut full_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&expected, .., IterationOrder::Ascending, &mut full_iter)
            .await;
        // Each segment routes reads across both shards' contributions.
        assert_segment_prefix_scan(&clone_db, &expected, b"data", b"datb").await;
        assert_segment_prefix_scan(&clone_db, &expected, b"idx", b"idy").await;
        for (key, value) in &expected {
            assert_eq!(
                clone_db.get(key).await.unwrap().as_ref(),
                Some(value),
                "key={:?}",
                key
            );
        }
        clone_db.close().await.unwrap();
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_union_projected_segmented_dbs() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path_a = Path::from("/tmp/test_parent_seg_proj_union_a");
        let parent_path_b = Path::from("/tmp/test_parent_seg_proj_union_b");
        let clone_path = Path::from("/tmp/test_clone_seg_proj_union");
        let extractor = Arc::new(test_utils::FixedThreeBytePrefixExtractor);
        let settings = wal_disabled_settings();

        // Parents have overlapping key spaces; per-source projection carves
        // out disjoint slices so the union is exactly each parent's slice.
        let table_a = BTreeMap::from([
            (Bytes::from_static(b"aaa-001"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"bbb-001"), Bytes::from_static(b"v2")),
            (Bytes::from_static(b"bbb-002"), Bytes::from_static(b"v3")),
            (
                Bytes::from_static(b"ccc-001"),
                Bytes::from_static(b"vA-ccc"),
            ),
            (
                Bytes::from_static(b"ddd-001"),
                Bytes::from_static(b"vA-ddd"),
            ),
        ]);
        let table_b = BTreeMap::from([
            (
                Bytes::from_static(b"aaa-001"),
                Bytes::from_static(b"vB-aaa"),
            ),
            (
                Bytes::from_static(b"bbb-001"),
                Bytes::from_static(b"vB-bbb"),
            ),
            (Bytes::from_static(b"ccc-001"), Bytes::from_static(b"v4")),
            (Bytes::from_static(b"ddd-001"), Bytes::from_static(b"v5")),
            (Bytes::from_static(b"eee-001"), Bytes::from_static(b"v6")),
        ]);

        build_segmented_parent(
            &parent_path_a,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table_a,
        )
        .await;
        build_segmented_parent(
            &parent_path_b,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table_b,
        )
        .await;

        let range_a = (
            Bound::Included(Bytes::from_static(b"aaa")),
            Bound::Excluded(Bytes::from_static(b"ccc")),
        );
        let range_b = (
            Bound::Included(Bytes::from_static(b"ccc")),
            Bound::Unbounded,
        );

        run_segmented_clone(
            vec![
                CloneSourceSpec::new(parent_path_a.clone()).with_projection_range(range_a.clone()),
                CloneSourceSpec::new(parent_path_b.clone()).with_projection_range(range_b.clone()),
            ],
            &clone_path,
            object_store.clone(),
            None,
        )
        .await;

        assert_clone_segments(
            &clone_path,
            object_store.clone(),
            &[b"aaa", b"bbb", b"ccc", b"ddd", b"eee"],
        )
        .await;
        let store = ManifestStore::new(&clone_path, object_store.clone());
        let stored = store.read_latest_manifest().await.unwrap();
        assert_eq!(stored.manifest.external_dbs.len(), 2);

        let mut expected: BTreeMap<Bytes, Bytes> = BTreeMap::new();
        expected.extend(
            table_a
                .iter()
                .filter(|(k, _)| range_a.contains(*k))
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        expected.extend(
            table_b
                .iter()
                .filter(|(k, _)| range_b.contains(*k))
                .map(|(k, v)| (k.clone(), v.clone())),
        );

        let clone_db =
            open_segmented_clone(&clone_path, object_store.clone(), extractor, settings).await;
        let mut full_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&expected, .., IterationOrder::Ascending, &mut full_iter)
            .await;
        assert_segment_prefix_scan(&clone_db, &expected, b"bbb", b"bbc").await;
        assert_segment_prefix_scan(&clone_db, &expected, b"ddd", b"dde").await;
        clone_db.close().await.unwrap();
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_union_projected_segmented_dbs_with_shared_segment() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path_a = Path::from("/tmp/test_parent_seg_shared_union_a");
        let parent_path_b = Path::from("/tmp/test_parent_seg_shared_union_b");
        let clone_path = Path::from("/tmp/test_clone_seg_shared_union");
        let extractor = Arc::new(test_utils::FixedThreeBytePrefixExtractor);
        let settings = wal_disabled_settings();

        // Both parents have a `bbb` segment with disjoint keys within it.
        // Per-source projection slices each parent so the union has to merge
        // their `bbb` segments — L0 SSTs from both parents land in the same
        // output segment.
        let table_a = BTreeMap::from([
            (Bytes::from_static(b"aaa-001"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"bbb-001"), Bytes::from_static(b"v2")),
            (Bytes::from_static(b"bbb-002"), Bytes::from_static(b"v3")),
        ]);
        let table_b = BTreeMap::from([
            (Bytes::from_static(b"bbb-007"), Bytes::from_static(b"v4")),
            (Bytes::from_static(b"bbb-008"), Bytes::from_static(b"v5")),
            (Bytes::from_static(b"ccc-001"), Bytes::from_static(b"v6")),
        ]);

        build_segmented_parent(
            &parent_path_a,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table_a,
        )
        .await;
        build_segmented_parent(
            &parent_path_b,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table_b,
        )
        .await;

        let range_a = (
            Bound::Included(Bytes::from_static(b"aaa")),
            Bound::Excluded(Bytes::from_static(b"bbb-005")),
        );
        let range_b = (
            Bound::Included(Bytes::from_static(b"bbb-005")),
            Bound::Unbounded,
        );

        run_segmented_clone(
            vec![
                CloneSourceSpec::new(parent_path_a.clone()).with_projection_range(range_a.clone()),
                CloneSourceSpec::new(parent_path_b.clone()).with_projection_range(range_b.clone()),
            ],
            &clone_path,
            object_store.clone(),
            None,
        )
        .await;

        assert_clone_segments(&clone_path, object_store.clone(), &[b"aaa", b"bbb", b"ccc"]).await;

        // The shared `bbb` segment in the union must hold one L0 SST
        // contributed by each parent.
        let store = ManifestStore::new(&clone_path, object_store.clone());
        let stored = store.read_latest_manifest().await.unwrap();
        let bbb_segment = stored
            .manifest
            .core
            .segments
            .iter()
            .find(|s| s.prefix == Bytes::from_static(b"bbb"))
            .expect("bbb segment");
        assert_eq!(bbb_segment.tree.l0.len(), 2);
        assert_eq!(stored.manifest.external_dbs.len(), 2);

        let mut expected: BTreeMap<Bytes, Bytes> = BTreeMap::new();
        expected.extend(
            table_a
                .iter()
                .filter(|(k, _)| range_a.contains(*k))
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        expected.extend(
            table_b
                .iter()
                .filter(|(k, _)| range_b.contains(*k))
                .map(|(k, v)| (k.clone(), v.clone())),
        );

        let clone_db =
            open_segmented_clone(&clone_path, object_store.clone(), extractor, settings).await;
        let mut full_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&expected, .., IterationOrder::Ascending, &mut full_iter)
            .await;
        // The prefix scan on the shared `bbb` segment must surface rows from
        // both parents through a single segment-routed read path.
        assert_segment_prefix_scan(&clone_db, &expected, b"bbb", b"bbc").await;
        clone_db.close().await.unwrap();
    }

    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_project_segmented_db() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_seg_project");
        let clone_path = Path::from("/tmp/test_clone_seg_project");
        let extractor = Arc::new(test_utils::FixedThreeBytePrefixExtractor);
        let settings = wal_disabled_settings();
        let table = segmented_table();

        build_segmented_parent(
            &parent_path,
            object_store.clone(),
            extractor.clone(),
            settings.clone(),
            &table,
        )
        .await;

        let range = (
            Bound::Included(Bytes::from_static(b"bbb")),
            Bound::Excluded(Bytes::from_static(b"ddd")),
        );
        run_segmented_clone(
            vec![CloneSourceSpec::new(parent_path.clone())],
            &clone_path,
            object_store.clone(),
            Some(range),
        )
        .await;

        assert_clone_segments(&clone_path, object_store.clone(), &[b"bbb"]).await;

        let clone_db =
            open_segmented_clone(&clone_path, object_store.clone(), extractor, settings).await;
        let mut full_iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(
            &table,
            Bytes::from_static(b"bbb")..Bytes::from_static(b"ddd"),
            IterationOrder::Ascending,
            &mut full_iter,
        )
        .await;
        assert_segment_prefix_scan(&clone_db, &table, b"bbb", b"bbc").await;
        clone_db.close().await.unwrap();
    }

    /// Builds a WAL-disabled parent DB at `path` holding `table` in L0 (no
    /// segment extractor, so it can be unioned with other unsegmented sources).
    #[cfg(feature = "wal_disable")]
    async fn build_plain_wal_disabled_parent(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        table: &BTreeMap<Bytes, Bytes>,
    ) {
        let db = Db::builder(path.clone(), object_store.clone())
            .with_settings(wal_disabled_settings())
            .build()
            .await
            .unwrap();
        test_utils::seed_database(&db, table, false).await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    /// Builds a WAL-disabled parent DB at `path` holding `table` in L0 (so the
    /// natural WAL range is empty), then manually extends the manifest's WAL
    /// range by one id and plants a WAL object at that id. `wal_bytes` controls
    /// whether the planted WAL is a fence (zero bytes) or carries data
    /// (non-empty). Returns the id of the planted WAL object.
    #[cfg(feature = "wal_disable")]
    async fn build_parent_with_planted_wal(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        table: &BTreeMap<Bytes, Bytes>,
        wal_bytes: Bytes,
        system_clock: Arc<dyn SystemClock>,
    ) -> u64 {
        build_plain_wal_disabled_parent(path, object_store.clone(), table).await;

        // Extend the manifest's WAL range so that
        // `next_wal_sst_id - 1 > replay_after_wal_id`, forcing validation to
        // inspect the planted WAL object.
        let manifest_store = Arc::new(ManifestStore::new(path, object_store.clone()));
        let mut sm = StoredManifest::load(manifest_store, system_clock)
            .await
            .unwrap();
        let planted_wal_id = sm.db_state().next_wal_sst_id;
        let mut dirty = sm.prepare_dirty().unwrap();
        dirty.value.core.next_wal_sst_id = planted_wal_id + 1;
        sm.update(dirty).await.unwrap();

        // Plant the WAL object directly in the object store at the resolved path.
        use object_store::ObjectStoreExt;
        let wal_path =
            PathResolver::from_root(path.clone()).wal_sst_path(&WalFileId::from(planted_wal_id));
        object_store.put(&wal_path, wal_bytes.into()).await.unwrap();

        planted_wal_id
    }

    /// Builds a WAL-disabled parent DB at `path` holding `table` in L0, then
    /// extends the manifest's WAL range by one id *without* planting any WAL
    /// object. This leaves a manifest-referenced WAL id whose object is missing,
    /// exercising the missing-object (`NotFound`) branch of
    /// `validate_no_data_wal`, which must fail.
    #[cfg(feature = "wal_disable")]
    async fn build_parent_with_missing_wal(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
        table: &BTreeMap<Bytes, Bytes>,
        system_clock: Arc<dyn SystemClock>,
    ) {
        build_plain_wal_disabled_parent(path, object_store.clone(), table).await;

        // Extend the manifest's WAL range so that
        // `next_wal_sst_id - 1 > replay_after_wal_id`, forcing validation to
        // inspect a WAL object that was never written.
        let manifest_store = Arc::new(ManifestStore::new(path, object_store.clone()));
        let mut sm = StoredManifest::load(manifest_store, system_clock)
            .await
            .unwrap();
        let mut dirty = sm.prepare_dirty().unwrap();
        dirty.value.core.next_wal_sst_id += 1;
        sm.update(dirty).await.unwrap();
    }

    /// A union clone whose source references only a zero-byte (fence) WAL above
    /// `replay_after_wal_id` must SUCCEED: the fence WAL holds no data and the
    /// union clone drops WAL objects anyway.
    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_union_clone_with_fence_only_wal_succeeds() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let parent_path_a = Path::from("/tmp/test_parent_fence_union_a");
        let parent_path_b = Path::from("/tmp/test_parent_fence_union_b");
        let clone_path = Path::from("/tmp/test_clone_fence_union");

        let table_a = BTreeMap::from([
            (Bytes::from_static(b"aaa-001"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"aaa-002"), Bytes::from_static(b"v2")),
        ]);
        let table_b = BTreeMap::from([
            (Bytes::from_static(b"zzz-001"), Bytes::from_static(b"v3")),
            (Bytes::from_static(b"zzz-002"), Bytes::from_static(b"v4")),
        ]);

        // Source A carries a fence (zero-byte) WAL above replay_after_wal_id.
        build_parent_with_planted_wal(
            &parent_path_a,
            object_store.clone(),
            &table_a,
            Bytes::new(),
            system_clock.clone(),
        )
        .await;
        // Source B has no extra WAL.
        build_plain_wal_disabled_parent(&parent_path_b, object_store.clone(), &table_b).await;

        create_native_clone(
            vec![
                CloneSourceSpec::new(parent_path_a.clone()),
                CloneSourceSpec::new(parent_path_b.clone()),
            ],
            clone_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            Arc::new(DbRand::default()),
            None,
            None,
            None,
        )
        .await
        .expect("union clone with a fence-only WAL should succeed");

        // The unioned clone should contain data from both sources.
        let mut expected: BTreeMap<Bytes, Bytes> = table_a.clone();
        expected.extend(table_b.clone());
        let clone_db = Db::builder(clone_path.clone(), object_store.clone())
            .with_settings(wal_disabled_settings())
            .build()
            .await
            .unwrap();
        let mut iter = clone_db.scan(..).await.unwrap();
        test_utils::assert_ranged_db_scan(&expected, .., IterationOrder::Ascending, &mut iter)
            .await;
        clone_db.close().await.unwrap();
    }

    /// A union clone whose source references a real (non-empty) data WAL above
    /// `replay_after_wal_id` must FAIL with `InvalidCloneSourceWithWal`, since
    /// the union clone would silently drop that WAL data.
    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_fail_union_clone_with_data_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let parent_path_a = Path::from("/tmp/test_parent_data_wal_union_a");
        let parent_path_b = Path::from("/tmp/test_parent_data_wal_union_b");
        let clone_path = Path::from("/tmp/test_clone_data_wal_union");

        let table_a = BTreeMap::from([
            (Bytes::from_static(b"aaa-001"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"aaa-002"), Bytes::from_static(b"v2")),
        ]);
        let table_b = BTreeMap::from([
            (Bytes::from_static(b"zzz-001"), Bytes::from_static(b"v3")),
            (Bytes::from_static(b"zzz-002"), Bytes::from_static(b"v4")),
        ]);

        // Source A carries a real data WAL (non-empty) above replay_after_wal_id.
        build_parent_with_planted_wal(
            &parent_path_a,
            object_store.clone(),
            &table_a,
            Bytes::from_static(b"this-is-not-a-fence-it-has-data"),
            system_clock.clone(),
        )
        .await;
        build_plain_wal_disabled_parent(&parent_path_b, object_store.clone(), &table_b).await;

        let err = create_native_clone(
            vec![
                CloneSourceSpec::new(parent_path_a.clone()),
                CloneSourceSpec::new(parent_path_b.clone()),
            ],
            clone_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            Arc::new(DbRand::default()),
            None,
            None,
            None,
        )
        .await
        .unwrap_err();

        match err {
            SlateDBError::InvalidCloneSourceWithWal { paths } => {
                assert!(paths.contains(&parent_path_a));
            }
            other => panic!("expected InvalidCloneSourceWithWal, got {other:?}"),
        }
    }

    /// A union clone whose source references a WAL id that has no backing object
    /// (HEAD returns `NotFound`) must FAIL: a WAL object missing within the
    /// manifest's WAL bounds violates an invariant and signals a misconfigured
    /// WAL object store or data loss.
    #[cfg(feature = "wal_disable")]
    #[tokio::test]
    async fn should_fail_union_clone_with_missing_wal() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let system_clock: Arc<dyn SystemClock> = Arc::new(DefaultSystemClock::new());
        let parent_path_a = Path::from("/tmp/test_parent_missing_wal_union_a");
        let parent_path_b = Path::from("/tmp/test_parent_missing_wal_union_b");
        let clone_path = Path::from("/tmp/test_clone_missing_wal_union");

        let table_a = BTreeMap::from([
            (Bytes::from_static(b"aaa-001"), Bytes::from_static(b"v1")),
            (Bytes::from_static(b"aaa-002"), Bytes::from_static(b"v2")),
        ]);
        let table_b = BTreeMap::from([
            (Bytes::from_static(b"zzz-001"), Bytes::from_static(b"v3")),
            (Bytes::from_static(b"zzz-002"), Bytes::from_static(b"v4")),
        ]);

        // Source A references a WAL id above replay_after_wal_id whose object is
        // missing.
        build_parent_with_missing_wal(
            &parent_path_a,
            object_store.clone(),
            &table_a,
            system_clock.clone(),
        )
        .await;
        build_plain_wal_disabled_parent(&parent_path_b, object_store.clone(), &table_b).await;

        let expected_missing_wal_path = PathResolver::from_root(parent_path_a.clone())
            .wal_sst_path(&WalFileId::from({
                let manifest_store =
                    Arc::new(ManifestStore::new(&parent_path_a, object_store.clone()));
                let sm = StoredManifest::load(manifest_store, system_clock.clone())
                    .await
                    .unwrap();
                sm.manifest().core.replay_after_wal_id + 1
            }))
            .to_string();

        let err = create_native_clone(
            vec![
                CloneSourceSpec::new(parent_path_a.clone()),
                CloneSourceSpec::new(parent_path_b.clone()),
            ],
            clone_path.clone(),
            object_store.clone(),
            object_store.clone(),
            Arc::new(FailPointRegistry::new()),
            system_clock.clone(),
            Arc::new(DbRand::default()),
            None,
            None,
            None,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                err,
                SlateDBError::WalUnavailable(ref source)
                    if matches!(
                        source.downcast_ref::<ObjectStoreError>(),
                        Some(ObjectStoreError::NotFound { path, .. })
                            if path == &expected_missing_wal_path
                    )
            ),
            "expected NotFound for the missing WAL object, got {err:?}"
        );
    }

    /// A key space with three regions: `hot/` and `row/` hold rows that a
    /// tenant range owns, `idx/` holds rows that no tenant range owns. A tenant
    /// range is one range in `hot/` and one range in `row/`.
    fn three_region_table() -> BTreeMap<Bytes, Bytes> {
        BTreeMap::from([
            (Bytes::from_static(b"hot/1"), Bytes::from_static(b"h1")),
            (Bytes::from_static(b"hot/7"), Bytes::from_static(b"h7")),
            (Bytes::from_static(b"idx/1"), Bytes::from_static(b"i1")),
            (Bytes::from_static(b"row/1"), Bytes::from_static(b"r1")),
            (Bytes::from_static(b"row/7"), Bytes::from_static(b"r7")),
        ])
    }

    type KeyRange = (Bound<Bytes>, Bound<Bytes>);

    fn tenant_ranges(lo: &'static [u8], hi: &'static [u8]) -> Vec<KeyRange> {
        let key = |region: &[u8], suffix: &[u8]| Bytes::from([region, suffix].concat());
        vec![
            (
                Bound::Included(key(b"hot/", lo)),
                Bound::Excluded(key(b"hot/", hi)),
            ),
            (
                Bound::Included(key(b"row/", lo)),
                Bound::Excluded(key(b"row/", hi)),
            ),
        ]
    }

    async fn build_parent(path: &Path, object_store: Arc<dyn ObjectStore>) {
        let db = Db::builder(path.clone(), object_store)
            .build()
            .await
            .unwrap();
        test_utils::seed_database(&db, &three_region_table(), false)
            .await
            .unwrap();
        flush_and_close(db).await;
    }

    /// Flush the WAL and the memtable, then close. A projected clone of the DB
    /// is then allowed, because no data is left in the WAL.
    async fn flush_and_close(db: Db) {
        db.flush().await.unwrap();
        db.flush_with_options(FlushOptions {
            flush_type: FlushType::MemTable,
        })
        .await
        .unwrap();
        db.close().await.unwrap();
    }

    async fn clone_sources(
        sources: Vec<CloneSourceSpec>,
        clone_path: &Path,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(), SlateDBError> {
        create_native_clone(
            sources,
            clone_path.clone(),
            object_store.clone(),
            object_store,
            Arc::new(FailPointRegistry::new()),
            Arc::new(DefaultSystemClock::new()),
            Arc::new(DbRand::default()),
            None,
            None,
            None,
        )
        .await
    }

    /// Open a DB whose compactor compacts all L0 SSTs and sorted runs into one
    /// sorted run each time the returned flag is set.
    async fn open_with_on_demand_compactor(
        path: &Path,
        object_store: Arc<dyn ObjectStore>,
    ) -> (Db, Arc<AtomicBool>) {
        let should_compact = Arc::new(AtomicBool::new(false));
        let flag = should_compact.clone();
        let supplier = Arc::new(test_utils::OnDemandCompactionSchedulerSupplier::new(
            Arc::new(move |_| flag.swap(false, Ordering::SeqCst)),
        ));
        let db = Db::builder(path.clone(), object_store.clone())
            .with_compactor_builder(
                CompactorBuilder::new(path.clone(), object_store)
                    .with_scheduler_supplier(supplier)
                    .with_options(CompactorOptions {
                        poll_interval: Duration::from_millis(100),
                        worker: Some(CompactionWorkerOptions {
                            compactions_poll_interval: Duration::from_millis(100),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
            )
            .build()
            .await
            .unwrap();
        (db, should_compact)
    }

    async fn compact_all(db: &Db, should_compact: &AtomicBool) {
        should_compact.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                {
                    let state = db.inner.state.read();
                    let state = state.state();
                    let tree = &state.core().tree;
                    if tree.l0.is_empty() && !tree.compacted.is_empty() {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("compaction did not finish");
    }

    async fn assert_scan(db: &Db, expected: &[&'static [u8]]) {
        let mut iter = db.scan(..).await.unwrap();
        let mut keys = vec![];
        while let Some(kv) = iter.next().await.unwrap() {
            keys.push(kv.key);
        }
        let expected: Vec<Bytes> = expected.iter().map(|k| Bytes::from_static(k)).collect();
        assert_eq!(keys, expected);
    }

    #[tokio::test]
    async fn should_project_clone_on_multiple_ranges() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_multi_range");
        let clone_path = Path::from("/tmp/test_clone_multi_range");
        build_parent(&parent_path, object_store.clone()).await;

        // when: the clone keeps the tenant range [0, 5) in both regions
        clone_sources(
            vec![CloneSourceSpec::new(parent_path.clone())
                .with_projection_ranges(tenant_ranges(b"0", b"5"))],
            &clone_path,
            object_store.clone(),
        )
        .await
        .unwrap();

        // then: the parent's L0 SST has one view for each range, with
        // different view IDs
        let store = ManifestStore::new(&clone_path, object_store.clone());
        let stored = store.read_latest_manifest().await.unwrap();
        let l0 = &stored.manifest.core.tree.l0;
        assert_eq!(l0.len(), 2);
        assert_eq!(l0[0].sst.id, l0[1].sst.id);
        assert_ne!(l0[0].id, l0[1].id);

        // then: keys in both ranges are visible, keys between and outside the
        // ranges are not
        let (clone_db, should_compact) =
            open_with_on_demand_compactor(&clone_path, object_store.clone()).await;
        assert_eq!(
            clone_db.get(b"hot/1").await.unwrap(),
            Some(Bytes::from_static(b"h1"))
        );
        assert_eq!(
            clone_db.get(b"row/1").await.unwrap(),
            Some(Bytes::from_static(b"r1"))
        );
        for hidden in [b"hot/7", b"idx/1", b"row/7"] {
            assert_eq!(clone_db.get(hidden).await.unwrap(), None);
        }
        assert_scan(&clone_db, &[b"hot/1", b"row/1"]).await;

        // when: the clone writes a key between its ranges, and compacts
        clone_db.put(b"idx/9", b"i9").await.unwrap();
        clone_db
            .flush_with_options(FlushOptions {
                flush_type: FlushType::MemTable,
            })
            .await
            .unwrap();
        compact_all(&clone_db, &should_compact).await;

        // then: the clone's own write is visible, and the parent's keys
        // outside the ranges stay hidden
        assert_scan(&clone_db, &[b"hot/1", b"idx/9", b"row/1"]).await;
        clone_db.close().await.unwrap();

        // then: the same is true after reopen
        let clone_db = Db::open(clone_path.clone(), object_store.clone())
            .await
            .unwrap();
        assert_scan(&clone_db, &[b"hot/1", b"idx/9", b"row/1"]).await;
        assert_eq!(clone_db.get(b"idx/1").await.unwrap(), None);
        clone_db.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_union_clones_projected_on_multiple_ranges() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_multi_range_union");
        let left_path = Path::from("/tmp/test_left_multi_range_union");
        let right_path = Path::from("/tmp/test_right_multi_range_union");
        let merged_path = Path::from("/tmp/test_merged_multi_range_union");
        build_parent(&parent_path, object_store.clone()).await;

        // given: two clones that split the tenant ranges at 5. Neither clone
        // compacts, so both keep views of the parent's L0 SST.
        for (path, lo, hi) in [(&left_path, b"0", b"5"), (&right_path, b"5", b"9")] {
            clone_sources(
                vec![CloneSourceSpec::new(parent_path.clone())
                    .with_projection_ranges(tenant_ranges(lo, hi))],
                path,
                object_store.clone(),
            )
            .await
            .unwrap();
        }
        let left = Db::open(left_path.clone(), object_store.clone())
            .await
            .unwrap();
        left.put(b"hot/2", b"h2").await.unwrap();
        left.put(b"idx/L", b"left-index").await.unwrap();
        flush_and_close(left).await;
        let right = Db::open(right_path.clone(), object_store.clone())
            .await
            .unwrap();
        right.put(b"row/8", b"r8").await.unwrap();
        flush_and_close(right).await;

        // when: the union keeps the ranges of each clone
        clone_sources(
            vec![
                CloneSourceSpec::new(left_path.clone())
                    .with_projection_ranges(tenant_ranges(b"0", b"5")),
                CloneSourceSpec::new(right_path.clone())
                    .with_projection_ranges(tenant_ranges(b"5", b"9")),
            ],
            &merged_path,
            object_store.clone(),
        )
        .await
        .unwrap();

        // then: L0 view IDs are unique, although both clones have views of
        // the parent's SST with the parent's view ID
        let store = ManifestStore::new(&merged_path, object_store.clone());
        let stored = store.read_latest_manifest().await.unwrap();
        let l0 = &stored.manifest.core.tree.l0;
        let view_ids: std::collections::HashSet<_> = l0.iter().map(|view| view.id).collect();
        assert_eq!(view_ids.len(), l0.len());

        // then: the union holds the tenant rows of both clones, and drops the
        // rows outside the ranges, also after compaction and reopen
        let expected: [&'static [u8]; 6] =
            [b"hot/1", b"hot/2", b"hot/7", b"row/1", b"row/7", b"row/8"];
        let (merged, should_compact) =
            open_with_on_demand_compactor(&merged_path, object_store.clone()).await;
        assert_scan(&merged, &expected).await;
        compact_all(&merged, &should_compact).await;
        assert_scan(&merged, &expected).await;
        merged.close().await.unwrap();
        let merged = Db::open(merged_path.clone(), object_store.clone())
            .await
            .unwrap();
        assert_scan(&merged, &expected).await;
        merged.close().await.unwrap();
    }

    #[tokio::test]
    async fn should_reject_projection_without_ranges() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let parent_path = Path::from("/tmp/test_parent_no_ranges");
        let clone_path = Path::from("/tmp/test_clone_no_ranges");
        build_parent(&parent_path, object_store.clone()).await;

        let err = clone_sources(
            vec![CloneSourceSpec::new(parent_path.clone()).with_projection_ranges(vec![])],
            &clone_path,
            object_store.clone(),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, SlateDBError::InvalidProjection { .. }));
    }
}
