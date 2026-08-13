//! Web backend.
//!
//! Futures are spawned as JavaScript promises, timers use `setTimeout` and
//! blocking tasks run on a pool of web workers.

use std::future::Future;

pub mod runtime;
pub mod task;
pub mod time;

mod thread_pool;

impl crate::ext::HandleExt for runtime::Handle {
    #[track_caller]
    fn spawn_named<Fut>(&self, name: impl Into<String>, future: Fut) -> task::JoinHandle<Fut::Output>
    where
        Fut: Future + task::MaybeSend + 'static,
        Fut::Output: task::MaybeSend + 'static,
    {
        let _ = name;
        self.spawn(future)
    }

    #[track_caller]
    fn spawn_blocking_named<F, R>(&self, name: impl Into<String>, f: F) -> task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let _ = name;
        self.spawn_blocking(f)
    }
}

impl<T: 'static> crate::ext::JoinSetExt<T> for task::JoinSet<T> {
    #[track_caller]
    fn spawn_named<Fut>(&mut self, name: impl Into<String>, future: Fut) -> task::AbortHandle
    where
        Fut: Future<Output = T> + task::MaybeSend + 'static,
    {
        let _ = name;
        self.spawn(future)
    }

    #[track_caller]
    fn spawn_blocking_named<F>(&mut self, name: impl Into<String>, f: F) -> task::AbortHandle
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send,
    {
        let _ = name;
        self.spawn_blocking(f)
    }
}
