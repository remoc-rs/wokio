//! Backend-independent extensions to the Tokio API.
//!
//! These are re-exported from the module they belong to and the trait
//! implementations live in the respective backend.

use futures::Stream;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    task::{AbortHandle, JoinHandle, MaybeSend},
    time::{Instant, Interval},
};

/// Tests whether threads are available and working on this platform
/// by spawning a test thread.
///
/// The result is cached after the first call.
pub async fn has_threads() -> bool {
    use tokio::sync::{OnceCell, oneshot};

    static AVAILABLE: OnceCell<bool> = OnceCell::const_new();
    *AVAILABLE
        .get_or_init(|| async move {
            tracing::trace!("spawning test thread");

            let (tx, rx) = oneshot::channel();
            let res = std::thread::Builder::new().name("threads available".into()).spawn(move || {
                tracing::trace!("test thread started");
                let _ = tx.send(());
            });

            match res {
                Ok(_) => {
                    tracing::trace!("waiting for test thread");
                    match rx.await {
                        Ok(()) => {
                            tracing::trace!("threads are available");
                            true
                        }
                        Err(_) => {
                            tracing::warn!("test thread failed");
                            false
                        }
                    }
                }
                Err(os_error) => {
                    tracing::warn!(%os_error, "threads not available");
                    false
                }
            }
        })
        .await
}

/// Runtime handle extensions.
pub trait HandleExt {
    /// Spawns a task providing a name for diagnostic purposes.
    ///
    /// See the [crate-level documentation](crate#task-names) for when the name
    /// is actually applied.
    #[track_caller]
    fn spawn_named<Fut>(&self, name: impl Into<String>, future: Fut) -> JoinHandle<Fut::Output>
    where
        Fut: Future + MaybeSend + 'static,
        Fut::Output: MaybeSend + 'static;

    /// Runs the provided function on a thread pool dedicated to blocking operations,
    /// providing a name for diagnostic purposes.
    ///
    /// See the [crate-level documentation](crate#task-names) for when the name
    /// is actually applied.
    #[track_caller]
    fn spawn_blocking_named<F, R>(&self, name: impl Into<String>, f: F) -> JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}

/// A future that can be run as a task on this platform.
///
/// This is automatically implemented and requires [`Send`] on platforms
/// where tasks can be moved between threads.
pub trait MaybeSendFuture: Future + MaybeSend {}
impl<T> MaybeSendFuture for T where T: Future + MaybeSend + ?Sized {}

/// A boxed future that can be run as a task on this platform.
///
/// This corresponds to [`futures::future::BoxFuture`] on native platforms
/// and to [`futures::future::LocalBoxFuture`] on the web.
pub type BoxFuture<'a, T> = Pin<Box<dyn MaybeSendFuture<Output = T> + 'a>>;

/// [`Future`] extensions.
pub trait MaybeSendFutureExt<'a>: Future + MaybeSend + 'a {
    /// Boxes this future, erasing its type.
    ///
    /// This is the platform-dependent equivalent of
    /// [`FutureExt::boxed`](futures::FutureExt::boxed).
    fn maybe_boxed(self) -> BoxFuture<'a, Self::Output>
    where
        Self: Sized,
    {
        Box::pin(self)
    }
}

impl<'a, T> MaybeSendFutureExt<'a> for T where T: Future + MaybeSend + 'a {}

/// [`JoinSet`](crate::task::JoinSet) extensions.
pub trait JoinSetExt<T> {
    /// Spawns a task on the JoinSet providing a name for diagnostic purposes.
    ///
    /// See the [crate-level documentation](crate#task-names) for when the name
    /// is actually applied.
    #[track_caller]
    fn spawn_named<Fut>(&mut self, name: impl Into<String>, future: Fut) -> AbortHandle
    where
        Fut: Future<Output = T> + MaybeSend + 'static;

    /// Spawns a blocking function on the JoinSet providing a name for diagnostic purposes.
    ///
    /// See the [crate-level documentation](crate#task-names) for when the name
    /// is actually applied.
    #[track_caller]
    fn spawn_blocking_named<F>(&mut self, name: impl Into<String>, f: F) -> AbortHandle
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send;
}

/// A stream that produces an event at a fixed time interval.
///
/// This is not part of Tokio itself, but mirrors the wrapper provided by
/// [tokio-stream](https://docs.rs/tokio-stream).
#[derive(Debug)]
pub struct IntervalStream {
    inner: Interval,
}

impl IntervalStream {
    /// Creates a stream from an interval.
    pub fn new(interval: Interval) -> Self {
        Self { inner: interval }
    }

    /// Consumes the stream, returning the underlying interval.
    pub fn into_inner(self) -> Interval {
        self.inner
    }
}

impl From<Interval> for IntervalStream {
    fn from(interval: Interval) -> Self {
        Self::new(interval)
    }
}

impl AsRef<Interval> for IntervalStream {
    fn as_ref(&self) -> &Interval {
        &self.inner
    }
}

impl AsMut<Interval> for IntervalStream {
    fn as_mut(&mut self) -> &mut Interval {
        &mut self.inner
    }
}

impl Stream for IntervalStream {
    type Item = Instant;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.inner.poll_tick(cx).map(Some)
    }
}

/// Creates a stream that produces an event at the specified time interval.
///
/// The first event is produced immediately.
///
/// # Panics
/// Panics if `period` is zero.
pub fn interval_stream(period: Duration) -> IntervalStream {
    IntervalStream::new(crate::time::interval(period))
}
