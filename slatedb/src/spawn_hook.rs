//! A process-wide hook that lets an embedding application run the tasks
//! SlateDB spawns inside a context of its own (cairnflow fork).
//!
//! SlateDB spawns background tasks (the writer, WAL buffer, memtable flusher,
//! compactor, garbage collector, manifest poller) and short-lived helper tasks
//! (block prefetches, SST uploads). Task-local state of the caller, such as a
//! label that attributes object store requests to the component that issued
//! them, does not cross `tokio::spawn`. With a hook installed, every spawn site
//! asks the hook for a [`TaskScope`] while still running in the spawner's
//! context, and every poll of the spawned future runs inside that scope.
//!
//! Background tasks pass their name (the dispatcher name, such as
//! `"compactor"`, or the part of a [`spawn_bg_task`](crate::utils::spawn_bg_task)
//! name before the first `:`, such as `"subcompaction"`). Helper tasks pass
//! `None`: they work on behalf of whoever spawned them, and a hook that
//! captures its caller's context makes them inherit it.
//!
//! Without a hook, spawned futures run as before; the only cost is one atomic
//! load per spawn.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

/// The context a spawned task runs in, as returned by a [`SpawnHook`].
pub trait TaskScope: Send + Sync {
    /// Runs `poll` (one poll of the task) inside this scope. An implementation
    /// must call `poll` exactly once.
    fn enter(&self, poll: &mut dyn FnMut());
}

/// Called at every spawn site, in the spawner's context, with the task name
/// (`None` for a helper task). `None` runs the task without a scope.
pub type SpawnHook = dyn Fn(Option<&str>) -> Option<Arc<dyn TaskScope>> + Send + Sync;

static HOOK: OnceLock<Box<SpawnHook>> = OnceLock::new();

/// Installs the process-wide spawn hook. Only the first call wins; returns
/// whether this call installed `hook`. Tasks spawned before the hook was
/// installed keep running without a scope.
pub fn set_spawn_hook(hook: Box<SpawnHook>) -> bool {
    HOOK.set(hook).is_ok()
}

/// Wraps `future` so that it runs in the scope the hook returns for `name`.
pub(crate) fn scoped<F: Future>(name: Option<&str>, future: F) -> Scoped<F> {
    Scoped {
        inner: Box::pin(future),
        scope: HOOK.get().and_then(|hook| hook(name)),
    }
}

/// The task name a hook sees for a [`spawn_bg_task`](crate::utils::spawn_bg_task)
/// name: the part before the first `:`, which drops per-job ids.
pub(crate) fn base_name(name: &str) -> &str {
    name.split(':').next().unwrap_or(name)
}

/// A future that polls `inner` inside `scope`.
pub(crate) struct Scoped<F> {
    inner: Pin<Box<F>>,
    scope: Option<Arc<dyn TaskScope>>,
}

impl<F: Future> Future for Scoped<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let this = self.get_mut();
        let Some(scope) = &this.scope else {
            return this.inner.as_mut().poll(cx);
        };
        let inner = &mut this.inner;
        let mut out = None;
        scope.enter(&mut || {
            if out.is_none() {
                out = Some(inner.as_mut().poll(cx));
            }
        });
        // A scope that did not call `poll` must not stall the task.
        match out {
            Some(poll) => poll,
            None => this.inner.as_mut().poll(cx),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_name_drops_job_ids() {
        assert_eq!(base_name("subcompaction:01J:3"), "subcompaction");
        assert_eq!(base_name("compactor_executor"), "compactor_executor");
    }

    struct Counting(std::sync::atomic::AtomicUsize);

    impl TaskScope for Counting {
        fn enter(&self, poll: &mut dyn FnMut()) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            poll();
        }
    }

    struct Skipping;

    impl TaskScope for Skipping {
        fn enter(&self, _poll: &mut dyn FnMut()) {}
    }

    #[tokio::test]
    async fn every_poll_runs_inside_the_scope() {
        let scope = Arc::new(Counting(Default::default()));
        let fut = Scoped {
            inner: Box::pin(async {
                tokio::task::yield_now().await;
                7
            }),
            scope: Some(scope.clone() as Arc<dyn TaskScope>),
        };
        assert_eq!(fut.await, 7);
        assert_eq!(scope.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_scope_that_skips_the_poll_does_not_stall_the_task() {
        let fut = Scoped {
            inner: Box::pin(async { 3 }),
            scope: Some(Arc::new(Skipping) as Arc<dyn TaskScope>),
        };
        assert_eq!(fut.await, 3);
    }
}
