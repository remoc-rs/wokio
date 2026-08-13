//! Native backend using Tokio.
//!
//! Every item is a re-export of, or a thin wrapper around, its Tokio counterpart.

use std::future::Future;

/// The runtime.
pub mod runtime {
    #[doc(no_inline)]
    pub use tokio::runtime::{Handle, TryCurrentError};

    pub use crate::ext::HandleExt;
}

/// Asynchronous green-threads.
pub mod task {
    use std::future::Future;

    #[doc(no_inline)]
    pub use tokio::task::{AbortHandle, JoinError, JoinHandle, JoinSet, spawn, spawn_blocking};

    #[doc(no_inline)]
    pub use tokio::task::{LocalKey, Unconstrained, futures, unconstrained, yield_now};

    pub use crate::ext::{BoxFuture, JoinSetExt, MaybeSendFuture, MaybeSendFutureExt, has_threads};

    /// Whether blocking the current thread is allowed.
    ///
    /// This is always true on native platforms.
    #[inline]
    pub fn is_blocking_allowed() -> bool {
        true
    }

    /// Runs a future to completion.
    #[track_caller]
    pub fn block_on<F: Future>(future: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(future)
    }

    /// Spawns a task providing a name for diagnostic purposes.
    ///
    /// See the [crate-level documentation](crate#task-names) for when the name
    /// is actually applied.
    #[track_caller]
    pub fn spawn_named<Fut>(name: impl Into<String>, future: Fut) -> JoinHandle<Fut::Output>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        #[cfg(all(tokio_unstable, feature = "task-names"))]
        {
            let name = name.into();
            tokio::task::Builder::new().name(&name).spawn(future).expect("spawning task failed")
        }

        #[cfg(not(all(tokio_unstable, feature = "task-names")))]
        {
            let _ = name;
            spawn(future)
        }
    }

    /// Runs the provided function on a thread pool dedicated to blocking operations,
    /// providing a name for diagnostic purposes.
    ///
    /// See the [crate-level documentation](crate#task-names) for when the name
    /// is actually applied.
    #[track_caller]
    pub fn spawn_blocking_named<F, R>(name: impl Into<String>, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        #[cfg(all(tokio_unstable, feature = "task-names"))]
        {
            let name = name.into();
            tokio::task::Builder::new().name(&name).spawn_blocking(f).expect("spawning blocking task failed")
        }

        #[cfg(not(all(tokio_unstable, feature = "task-names")))]
        {
            let _ = name;
            spawn_blocking(f)
        }
    }

    /// The [Send] bound required to [spawn] a task on this platform.
    ///
    /// On native platforms tasks may be moved between threads, thus this requires [Send].
    pub trait MaybeSend: Send {}
    impl<T: Send + ?Sized> MaybeSend for T {}

    /// The [Sync] bound required to share a value between tasks on this platform.
    ///
    /// On native platforms tasks may run on different threads, thus this requires [Sync].
    pub trait MaybeSync: Sync {}
    impl<T: Sync + ?Sized> MaybeSync for T {}
}

/// Time.
pub mod time {
    #[doc(no_inline)]
    pub use tokio::time::{
        Duration, Instant, Interval, MissedTickBehavior, Sleep, Timeout, interval, interval_at, sleep,
        sleep_until, timeout, timeout_at,
    };

    /// Time errors.
    pub mod error {
        #[doc(no_inline)]
        pub use tokio::time::error::Elapsed;
    }

    pub use crate::ext::{IntervalStream, interval_stream};
}

impl crate::ext::HandleExt for runtime::Handle {
    #[track_caller]
    fn spawn_named<Fut>(&self, name: impl Into<String>, future: Fut) -> task::JoinHandle<Fut::Output>
    where
        Fut: Future + task::MaybeSend + 'static,
        Fut::Output: task::MaybeSend + 'static,
    {
        #[cfg(all(tokio_unstable, feature = "task-names"))]
        {
            let name = name.into();
            tokio::task::Builder::new().name(&name).spawn_on(future, self).expect("spawning task failed")
        }

        #[cfg(not(all(tokio_unstable, feature = "task-names")))]
        {
            let _ = name;
            self.spawn(future)
        }
    }

    #[track_caller]
    fn spawn_blocking_named<F, R>(&self, name: impl Into<String>, f: F) -> task::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        #[cfg(all(tokio_unstable, feature = "task-names"))]
        {
            let name = name.into();
            tokio::task::Builder::new()
                .name(&name)
                .spawn_blocking_on(f, self)
                .expect("spawning blocking task failed")
        }

        #[cfg(not(all(tokio_unstable, feature = "task-names")))]
        {
            let _ = name;
            self.spawn_blocking(f)
        }
    }
}

impl<T: task::MaybeSend + 'static> crate::ext::JoinSetExt<T> for task::JoinSet<T> {
    #[track_caller]
    fn spawn_named<Fut>(&mut self, name: impl Into<String>, future: Fut) -> task::AbortHandle
    where
        Fut: Future<Output = T> + task::MaybeSend + 'static,
    {
        #[cfg(all(tokio_unstable, feature = "task-names"))]
        {
            let name = name.into();
            self.build_task().name(&name).spawn(future).expect("spawning task failed")
        }

        #[cfg(not(all(tokio_unstable, feature = "task-names")))]
        {
            let _ = name;
            self.spawn(future)
        }
    }

    #[track_caller]
    fn spawn_blocking_named<F>(&mut self, name: impl Into<String>, f: F) -> task::AbortHandle
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send,
    {
        #[cfg(all(tokio_unstable, feature = "task-names"))]
        {
            let name = name.into();
            self.build_task().name(&name).spawn_blocking(f).expect("spawning blocking task failed")
        }

        #[cfg(not(all(tokio_unstable, feature = "task-names")))]
        {
            let _ = name;
            self.spawn_blocking(f)
        }
    }
}
