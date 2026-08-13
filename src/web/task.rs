//! Asynchronous green-threads.

use pin_project_lite::pin_project;
use std::{
    any::{Any, type_name},
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    task::{Context, Poll, ready},
};
use sync_wrapper::SyncWrapper;
use tokio::sync::{mpsc, oneshot};

use super::runtime::Handle;

#[doc(no_inline)]
pub use tokio::task::{LocalKey, Unconstrained, futures, unconstrained, yield_now};

pub use crate::ext::{JoinSetExt, has_threads};

/// Whether blocking the current thread is allowed.
///
/// Blocking is only allowed inside a web worker; blocking the main thread of a
/// browser window freezes the user interface and is prohibited for some
/// operations by the browser itself.
#[inline]
pub fn is_blocking_allowed() -> bool {
    use std::cell::LazyCell;
    use wasm_bindgen::JsCast;

    thread_local! {
        static ALLOWED: LazyCell<bool> =
            LazyCell::new(|| js_sys::global().is_instance_of::<web_sys::WorkerGlobalScope>());
    }

    ALLOWED.with(|allowed| **allowed)
}

/// Task failed to execute to completion.
pub struct JoinError(pub(super) JoinErrorRepr);

pub(super) enum JoinErrorRepr {
    /// Aborted.
    Aborted,
    /// Panicked.
    ///
    /// The message is extracted eagerly, so that it remains accessible through a
    /// shared reference even though the payload itself is not [Sync].
    Panicked { msg: Option<String>, payload: SyncWrapper<Box<dyn Any + Send + 'static>> },
    /// Thread failed.
    Failed,
    /// Spawning worker thread failed.
    Spawn(std::io::Error),
}

impl JoinErrorRepr {
    /// Builds a panic error from the payload of a caught panic.
    pub(super) fn panicked(payload: Box<dyn Any + Send + 'static>) -> Self {
        let msg = if let Some(s) = payload.downcast_ref::<String>() {
            Some(s.clone())
        } else {
            payload.downcast_ref::<&'static str>().map(|s| s.to_string())
        };

        Self::Panicked { msg, payload: SyncWrapper::new(payload) }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.0 {
            JoinErrorRepr::Aborted => f.debug_tuple("Aborted").finish(),
            JoinErrorRepr::Panicked { msg, .. } => {
                f.debug_tuple("Panicked").field(&msg.as_deref().unwrap_or("...")).finish()
            }
            JoinErrorRepr::Failed => f.debug_tuple("Failed").finish(),
            JoinErrorRepr::Spawn(err) => f.debug_tuple("Spawn").field(&err).finish(),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.0 {
            JoinErrorRepr::Aborted => write!(f, "task was cancelled"),
            JoinErrorRepr::Panicked { msg, .. } => match msg {
                Some(msg) => write!(f, "task panicked with message {msg}"),
                None => write!(f, "task panicked"),
            },
            JoinErrorRepr::Failed => write!(f, "task failed"),
            JoinErrorRepr::Spawn(err) => write!(f, "spawning worker failed: {err}"),
        }
    }
}

impl From<JoinError> for std::io::Error {
    fn from(err: JoinError) -> Self {
        std::io::Error::other(err)
    }
}

impl Error for JoinError {}

impl JoinError {
    /// Returns true if the error was caused by the task being cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(&self.0, JoinErrorRepr::Aborted)
    }

    /// Returns true if the error was caused by thread failure.
    pub fn is_failed(&self) -> bool {
        matches!(&self.0, JoinErrorRepr::Failed)
    }

    /// Returns true if the error was caused by the task panicking.
    pub fn is_panic(&self) -> bool {
        matches!(&self.0, JoinErrorRepr::Panicked { .. })
    }

    /// Returns true if the error was caused by worker thread spawning failure.
    pub fn is_spawn(&self) -> bool {
        matches!(&self.0, JoinErrorRepr::Spawn(_))
    }

    /// Consumes the join error, returning the object with which the task panicked.
    #[track_caller]
    pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
        self.try_into_panic().expect("`JoinError` reason is not a panic.")
    }

    /// Consumes the join error, returning the object with which the task
    /// panicked if the task terminated due to a panic. Otherwise, `self` is
    /// returned.
    pub fn try_into_panic(self) -> Result<Box<dyn Any + Send + 'static>, JoinError> {
        match self.0 {
            JoinErrorRepr::Panicked { payload, .. } => Ok(payload.into_inner()),
            _ => Err(self),
        }
    }
}

pin_project! {
    /// An owned permission to join on a task.
    pub struct JoinHandle<T> {
        #[pin]
        pub(super) result_rx: oneshot::Receiver<Result<T, JoinError>>,
        pub(super) abort_tx: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_tuple("JoinHandle").finish()
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let result = ready!(self.project().result_rx.poll(cx));
        Poll::Ready(result.unwrap_or(Err(JoinError(JoinErrorRepr::Failed))))
    }
}

impl<T> JoinHandle<T> {
    /// Abort the task associated with the handle.
    pub fn abort(&self) {
        let mut abort_tx = self.abort_tx.lock().unwrap();
        if let Some(abort_tx) = abort_tx.take() {
            let _ = abort_tx.send(());
        }
    }
}

/// Spawns a future onto the browser.
#[track_caller]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    Handle::current().spawn(future)
}

/// Spawns a task providing a name for diagnostic purposes.
///
/// Task names are unsupported on the web and the name is dropped.
#[track_caller]
pub fn spawn_named<Fut>(name: impl Into<String>, future: Fut) -> JoinHandle<Fut::Output>
where
    Fut: Future + 'static,
    Fut::Output: 'static,
{
    let _ = name;
    spawn(future)
}

/// Runs the provided function on a thread pool dedicated to blocking operations.
#[track_caller]
pub fn spawn_blocking<F, R>(f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    Handle::current().spawn_blocking(f)
}

/// Runs the provided function on a thread pool dedicated to blocking operations,
/// providing a name for diagnostic purposes.
///
/// Task names are unsupported on the web and the name is dropped.
#[track_caller]
pub fn spawn_blocking_named<F, R>(name: impl Into<String>, f: F) -> JoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let _ = name;
    spawn_blocking(f)
}

/// Runs a future to completion.
///
/// This blocks the current thread and thus panics when called on a thread where
/// [blocking is not allowed](is_blocking_allowed).
#[track_caller]
pub fn block_on<F: Future>(future: F) -> F::Output {
    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    rt.block_on(future)
}

/// The [Send] bound required to [spawn] a task on this platform.
///
/// On the web tasks are spawned onto the current thread, thus this imposes no
/// requirement and is implemented for every type.
pub trait MaybeSend {}
impl<T: ?Sized> MaybeSend for T {}

/// An owned permission to abort a spawned task, without awaiting its completion.
#[derive(Clone)]
pub struct AbortHandle(mpsc::Sender<()>);

impl fmt::Debug for AbortHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AbortHandle").field("is_finished", &self.is_finished()).finish()
    }
}

impl AbortHandle {
    /// Abort the task associated with the handle.
    pub fn abort(&self) {
        let _ = self.0.try_send(());
    }

    /// Checks if the task associated with this `AbortHandle` has finished.
    pub fn is_finished(&self) -> bool {
        self.0.is_closed()
    }
}

/// A collection of tasks spawned on a Wokio runtime.
///
/// When the JoinSet is dropped, all tasks in the JoinSet are immediately aborted.
pub struct JoinSet<T> {
    tasks: Vec<TaskHandle<T>>,
}

impl<T> fmt::Debug for JoinSet<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct(&format!("JoinSet<{:?}>", type_name::<T>())).field("tasks", &self.tasks.len()).finish()
    }
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

struct TaskHandle<T> {
    abort: AbortHandle,
    #[expect(dead_code)]
    result_rx: oneshot::Receiver<T>,
}

impl<T> JoinSet<T> {
    /// Create a new JoinSet.
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    /// Returns the number of tasks currently in the JoinSet.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Returns whether the JoinSet is empty.
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Aborts all tasks on this JoinSet.
    pub fn abort_all(&mut self) {
        for task in &self.tasks {
            task.abort.abort();
        }
    }

    /// Removes all tasks from this JoinSet without aborting them.
    pub fn detach_all(&mut self) {
        self.tasks.clear();
    }

    /// Returns a `Builder` that can be used to configure a task prior to spawning it on this `JoinSet`.
    pub fn build_task(&mut self) -> join_set::Builder<'_, T> {
        join_set::Builder { join_set: self, name: "unnamed" }
    }
}

impl<T: 'static> JoinSet<T> {
    /// Spawn the provided task on the JoinSet, returning an [AbortHandle]
    /// that can be used to remotely cancel the task.
    pub fn spawn<F>(&mut self, task: F) -> AbortHandle
    where
        F: Future<Output = T> + 'static,
    {
        let (abort_tx, mut abort_rx) = mpsc::channel(1);
        let (result_tx, result_rx) = oneshot::channel();

        wasm_bindgen_futures::spawn_local(async move {
            tokio::select! {
                biased;
                result = task => {
                    let _ = result_tx.send(result);
                },
                Some(()) = abort_rx.recv() => {
                    tracing::trace!("aborted joinset task");
                },
            }
        });

        let abort = AbortHandle(abort_tx);
        let task = TaskHandle { abort: abort.clone(), result_rx };
        self.tasks.push(task);
        abort
    }

    /// Spawn the provided blocking function on the JoinSet, returning an [AbortHandle]
    /// that can be used to remotely cancel the task.
    pub fn spawn_blocking<F>(&mut self, f: F) -> AbortHandle
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send,
    {
        self.spawn(async move { spawn_blocking(f).await.expect("blocking task panicked") })
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}

/// Configuring tasks spawned on a [`JoinSet`].
pub mod join_set {
    use super::{AbortHandle, JoinSet};
    use std::future::Future;

    /// Factory which is used to configure the properties of a task spawned on a [`JoinSet`].
    #[derive(Debug)]
    pub struct Builder<'a, T> {
        pub(super) join_set: &'a mut JoinSet<T>,
        pub(super) name: &'a str,
    }

    impl<'a, T: 'static> Builder<'a, T> {
        /// Assigns a name to the task which will be spawned.
        ///
        /// Task names are unsupported on the web and the name is dropped.
        pub fn name(mut self, name: &'a str) -> Self {
            self.name = name;
            self
        }

        /// Spawn the provided task on the JoinSet, returning an [AbortHandle]
        /// that can be used to remotely cancel the task.
        pub fn spawn<F>(self, task: F) -> std::io::Result<AbortHandle>
        where
            F: Future<Output = T> + 'static,
        {
            Ok(self.join_set.spawn(task))
        }

        /// Spawn the provided blocking function on the JoinSet, returning an [AbortHandle]
        /// that can be used to remotely cancel the task.
        pub fn spawn_blocking<F>(self, f: F) -> std::io::Result<AbortHandle>
        where
            F: FnOnce() -> T + Send + 'static,
            T: Send,
        {
            Ok(self.join_set.spawn_blocking(f))
        }
    }
}
