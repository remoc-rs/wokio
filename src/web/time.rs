//! Time.

use pin_project_lite::pin_project;
use std::{
    fmt,
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll, ready},
};

#[doc(no_inline)]
pub use std::time::Duration;

/// JavaScript sleep wrapper.
mod js {
    use js_sys::Function;
    use std::{
        cell::RefCell,
        fmt,
        future::Future,
        pin::Pin,
        rc::Rc,
        task::{Context, Poll, Waker},
        time::Duration,
    };
    use wasm_bindgen::{JsCast, prelude::*};
    use web_sys::{Window, WorkerGlobalScope};

    /// Longest delay that can be passed to `setTimeout`, i.e. roughly 24.8 days.
    ///
    /// The delay is specified as a 32-bit signed integer and a larger value would
    /// overflow it, causing the timer to fire immediately. Longer sleeps are
    /// performed by re-arming the timer in chunks of this size.
    const MAX_TIMEOUT: Duration = Duration::from_millis(i32::MAX as u64);

    /// JavaScript sleep.
    ///
    /// This is not Send + Sync since the underlying callback is bound to a
    /// JavaScript thread.
    pub struct JsSleep {
        inner: Rc<RefCell<JsSleepInner>>,
    }

    // Implement Send + Sync for targets without threads.
    //
    // On a target without thread support there is exactly one thread, thus a value
    // can never actually be moved to or shared with another thread and the binding
    // of the callback to its JavaScript thread cannot be violated.
    #[cfg(all(target_family = "wasm", not(target_feature = "atomics")))]
    #[allow(unsafe_code)]
    unsafe impl Send for JsSleep {}
    #[cfg(all(target_family = "wasm", not(target_feature = "atomics")))]
    #[allow(unsafe_code)]
    unsafe impl Sync for JsSleep {}

    impl fmt::Debug for JsSleep {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let inner = self.inner.borrow();
            f.debug_struct("Sleep")
                .field("timeout_id", &inner.timeout_id)
                .field("remaining", &inner.remaining)
                .finish()
        }
    }

    struct JsSleepInner {
        /// Sleep duration that is not covered by the currently armed timer.
        remaining: Duration,
        fired: bool,
        waker: Option<Waker>,
        timeout_id: Option<i32>,
        /// Timer callback, kept alive for as long as the timer may fire.
        ///
        /// It is created once and reused for every re-arming, since dropping it
        /// while it executes would free the closure that is currently running.
        callback: Option<Closure<dyn FnMut()>>,
    }

    impl JsSleep {
        pub(super) fn new(duration: Duration) -> Self {
            let inner = Rc::new(RefCell::new(JsSleepInner {
                remaining: duration,
                fired: false,
                waker: None,
                timeout_id: None,
                callback: None,
            }));

            let callback = {
                // A weak reference, so that the callback stored in the inner state
                // does not keep the inner state alive.
                let inner = Rc::downgrade(&inner);
                Closure::new(move || {
                    let Some(inner) = inner.upgrade() else { return };

                    let mut guard = inner.borrow_mut();
                    guard.timeout_id = None;

                    if guard.remaining.is_zero() {
                        guard.fired = true;
                        let waker = guard.waker.take();

                        // The waker must not be woken while the inner state is
                        // borrowed, since that may poll this future immediately.
                        drop(guard);

                        if let Some(waker) = waker {
                            waker.wake();
                        }
                    } else {
                        drop(guard);
                        Self::arm(&inner);
                    }
                })
            };
            inner.borrow_mut().callback = Some(callback);

            Self::arm(&inner);

            Self { inner }
        }

        /// Arms the timer for the next chunk of the remaining sleep duration.
        fn arm(inner: &Rc<RefCell<JsSleepInner>>) {
            let mut guard = inner.borrow_mut();

            let chunk = guard.remaining.min(MAX_TIMEOUT);
            guard.remaining -= chunk;

            // The delay is specified in whole milliseconds and must be rounded up,
            // so that the sleep never completes early.
            let timeout = chunk.as_nanos().div_ceil(1_000_000) as i32;
            let handler = guard.callback.as_ref().expect("callback is set").as_ref().unchecked_ref();
            guard.timeout_id = Some(Self::register_timeout(handler, timeout));
        }

        fn register_timeout(handler: &Function, timeout: i32) -> i32 {
            let global = js_sys::global();

            if let Some(window) = global.dyn_ref::<Window>() {
                window.set_timeout_with_callback_and_timeout_and_arguments_0(handler, timeout).unwrap()
            } else if let Some(worker) = global.dyn_ref::<WorkerGlobalScope>() {
                worker.set_timeout_with_callback_and_timeout_and_arguments_0(handler, timeout).unwrap()
            } else {
                panic!("unsupported JavaScript global: {global:?}");
            }
        }

        fn unregister_timeout(id: i32) {
            let global = js_sys::global();

            if let Some(window) = global.dyn_ref::<Window>() {
                window.clear_timeout_with_handle(id);
            } else if let Some(worker) = global.dyn_ref::<WorkerGlobalScope>() {
                worker.clear_timeout_with_handle(id);
            } else {
                panic!("unsupported JavaScript global: {global:?}");
            }
        }
    }

    impl Future for JsSleep {
        type Output = ();

        /// Waits until the sleep duration has elapsed.
        fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            let mut inner = self.inner.borrow_mut();

            if inner.fired {
                return Poll::Ready(());
            }

            inner.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    impl Drop for JsSleep {
        fn drop(&mut self) {
            let inner = self.inner.borrow_mut();
            if let Some(timeout_id) = inner.timeout_id {
                Self::unregister_timeout(timeout_id);
            }
        }
    }
}

/// Future for [`sleep`].
#[cfg(all(target_family = "wasm", not(target_feature = "atomics")))]
pub use js::JsSleep as Sleep;

/// Thread-safe sleep.
///
/// Timers are bound to the JavaScript thread that created them, so on a target
/// with thread support all timers are registered on one dedicated thread and
/// their expiry is signalled back over a channel.
#[cfg(all(target_family = "wasm", target_feature = "atomics"))]
mod threads {
    use futures::{FutureExt, ready};
    use std::{
        fmt,
        future::Future,
        pin::Pin,
        sync::LazyLock,
        task::{Context, Poll},
        time::Duration,
    };
    use tokio::sync::{mpsc, oneshot};
    use wasm_bindgen_futures::spawn_thread;

    use super::js::JsSleep;

    struct SleepReq {
        duration: Duration,
        wake_tx: oneshot::Sender<()>,
    }

    static SLEEP_TX: LazyLock<mpsc::UnboundedSender<SleepReq>> = LazyLock::new(|| {
        let (sleep_tx, mut sleep_rx) = mpsc::unbounded_channel::<SleepReq>();
        spawn_thread(move || async move {
            while let Some(SleepReq { duration, mut wake_tx }) = sleep_rx.recv().await {
                wasm_bindgen_futures::spawn_local(async move {
                    tokio::select! {
                        biased;
                        () = JsSleep::new(duration) => {
                            let _ = wake_tx.send(());
                        },
                        () = wake_tx.closed() => (),
                    }
                });
            }
        });
        sleep_tx
    });

    /// Thread-safe sleep wrapper.
    pub struct Sleep(oneshot::Receiver<()>);

    impl fmt::Debug for Sleep {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.debug_tuple("Sleep").field(&self.0).finish()
        }
    }

    impl Sleep {
        pub(super) fn new(duration: Duration) -> Self {
            let (wake_tx, wake_rx) = oneshot::channel();
            SLEEP_TX.send(SleepReq { duration, wake_tx }).expect("sleep thread failed");
            Self(wake_rx)
        }
    }

    impl Future for Sleep {
        type Output = ();

        /// Waits until the sleep duration has elapsed.
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            ready!(self.0.poll_unpin(cx)).expect("sleep thread failed");
            Poll::Ready(())
        }
    }
}

/// Future for [`sleep`].
#[cfg(all(target_family = "wasm", target_feature = "atomics"))]
pub use threads::Sleep;

/// Waits until `duration` has elapsed.
///
/// `setTimeout` accepts a delay of at most roughly 24.8 days, so longer sleeps
/// are performed by re-arming the timer repeatedly. Any duration is accepted and
/// [`Duration::MAX`] effectively sleeps forever.
///
/// Timers have millisecond resolution, so the duration is rounded up to whole
/// milliseconds and the sleep never completes early.
pub fn sleep(duration: Duration) -> Sleep {
    Sleep::new(duration)
}

/// Waits until `deadline` is reached.
pub fn sleep_until(deadline: Instant) -> Sleep {
    sleep(deadline.duration_since(Instant::now()))
}

/// On WASI the platform provides a monotonic clock.
#[cfg(all(target_family = "wasm", target_os = "wasi"))]
#[doc(no_inline)]
pub use tokio::time::Instant;

/// Monotonic clock backed by JavaScript's `performance.now()`.
#[cfg(all(target_family = "wasm", not(target_os = "wasi")))]
mod instant {
    use std::{
        ops::{Add, AddAssign, Sub, SubAssign},
        time::Duration,
    };
    use wasm_bindgen::JsCast;
    use web_sys::{Window, WorkerGlobalScope};

    /// A measurement of a monotonically increasing clock.
    ///
    /// This is backed by JavaScript's [`performance.now()`], which is monotonic
    /// within the current execution context.
    ///
    /// [`performance.now()`]: https://developer.mozilla.org/en-US/docs/Web/API/Performance/now
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub struct Instant {
        micros: u64,
    }

    impl Instant {
        /// Returns an instant corresponding to "now".
        pub fn now() -> Self {
            Self { micros: (performance_now() * 1_000.0) as u64 }
        }

        /// Returns the amount of time elapsed since this instant.
        pub fn elapsed(&self) -> Duration {
            Self::now().duration_since(*self)
        }

        /// Returns the amount of time elapsed from `earlier` to this instant, or
        /// zero duration if `earlier` is later than this instant.
        pub fn duration_since(&self, earlier: Instant) -> Duration {
            Duration::from_micros(self.micros.saturating_sub(earlier.micros))
        }

        /// Returns the amount of time elapsed from `earlier` to this instant, or
        /// zero duration if `earlier` is later than this instant.
        pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
            self.duration_since(earlier)
        }

        /// Adds the duration to this instant, if the resulting value is representable.
        pub fn checked_add(&self, duration: Duration) -> Option<Self> {
            let micros = u64::try_from(duration.as_micros()).ok()?;
            Some(Self { micros: self.micros.checked_add(micros)? })
        }

        /// Subtracts the duration from this instant, if the resulting value is representable.
        pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
            let micros = u64::try_from(duration.as_micros()).ok()?;
            Some(Self { micros: self.micros.checked_sub(micros)? })
        }
    }

    impl Add<Duration> for Instant {
        type Output = Self;

        fn add(self, duration: Duration) -> Self::Output {
            Self { micros: self.micros.saturating_add(duration.as_micros().try_into().unwrap_or(u64::MAX)) }
        }
    }

    impl AddAssign<Duration> for Instant {
        fn add_assign(&mut self, duration: Duration) {
            *self = *self + duration;
        }
    }

    impl Sub<Instant> for Instant {
        type Output = Duration;

        fn sub(self, other: Instant) -> Self::Output {
            self.duration_since(other)
        }
    }

    impl Sub<Duration> for Instant {
        type Output = Self;

        fn sub(self, duration: Duration) -> Self::Output {
            Self { micros: self.micros.saturating_sub(duration.as_micros().try_into().unwrap_or(u64::MAX)) }
        }
    }

    impl SubAssign<Duration> for Instant {
        fn sub_assign(&mut self, duration: Duration) {
            *self = *self - duration;
        }
    }

    /// Returns the current value of `performance.now()` in milliseconds.
    fn performance_now() -> f64 {
        let global = js_sys::global();

        if let Some(window) = global.dyn_ref::<Window>() {
            window.performance().expect("performance not available").now()
        } else if let Some(worker) = global.dyn_ref::<WorkerGlobalScope>() {
            worker.performance().expect("performance not available").now()
        } else {
            panic!("unsupported JavaScript global: {global:?}");
        }
    }
}

#[cfg(all(target_family = "wasm", not(target_os = "wasi")))]
pub use instant::Instant;

/// Time errors.
pub mod error {
    use super::*;

    /// Timeout elapsed.
    #[derive(Debug, Clone)]
    pub struct Elapsed {}

    impl fmt::Display for Elapsed {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "timeout elapsed")
        }
    }

    impl std::error::Error for Elapsed {}

    impl From<Elapsed> for std::io::Error {
        fn from(_err: Elapsed) -> Self {
            std::io::ErrorKind::TimedOut.into()
        }
    }
}

pin_project! {
    /// Future returned by [`timeout`].
    pub struct Timeout<F> {
        #[pin]
        sleep: Sleep,
        #[pin]
        future: F,
    }
}

impl<F> fmt::Debug for Timeout<F> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Timeout").finish()
    }
}

impl<F> Future for Timeout<F>
where
    F: Future,
{
    type Output = Result<F::Output, error::Elapsed>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();

        if let Poll::Ready(res) = this.future.poll(cx) {
            return Poll::Ready(Ok(res));
        }

        if let Poll::Ready(()) = this.sleep.poll(cx) {
            return Poll::Ready(Err(error::Elapsed {}));
        }

        Poll::Pending
    }
}

/// Requires a `Future` to complete before the specified duration has elapsed.
pub fn timeout<F>(duration: Duration, future: F) -> Timeout<F::IntoFuture>
where
    F: IntoFuture,
{
    Timeout { sleep: sleep(duration), future: future.into_future() }
}

/// Requires a `Future` to complete before the specified instant in time.
pub fn timeout_at<F>(deadline: Instant, future: F) -> Timeout<F::IntoFuture>
where
    F: IntoFuture,
{
    Timeout { sleep: sleep_until(deadline), future: future.into_future() }
}

#[doc(no_inline)]
pub use tokio::time::MissedTickBehavior;

/// A deadline that is never reached.
fn far_future() -> Instant {
    // Roughly 30 years, as used by Tokio.
    Instant::now() + Duration::from_secs(86400 * 365 * 30)
}

/// Adds a duration to an instant, saturating at [`far_future`].
fn add_deadline(instant: Instant, duration: Duration) -> Instant {
    instant.checked_add(duration).unwrap_or_else(far_future)
}

/// Determines the next deadline according to the missed tick behavior.
fn next_deadline(behavior: MissedTickBehavior, timeout: Instant, now: Instant, period: Duration) -> Instant {
    match behavior {
        MissedTickBehavior::Burst => add_deadline(timeout, period),
        MissedTickBehavior::Delay => add_deadline(now, period),
        MissedTickBehavior::Skip => {
            let behind = u64::try_from((now - timeout).as_nanos() % period.as_nanos()).unwrap_or(0);
            add_deadline(now, period) - Duration::from_nanos(behind)
        }
    }
}

/// Interval returned by [`interval`] and [`interval_at`].
///
/// This type allows you to wait on a sequence of instants with a certain
/// duration between each instant.
#[derive(Debug)]
pub struct Interval {
    deadline: Instant,
    period: Duration,
    missed_tick_behavior: MissedTickBehavior,
    sleep: Sleep,
}

impl Interval {
    /// Completes when the next instant in the interval has been reached.
    ///
    /// Returns the instant the tick was scheduled for, which may lie in the past
    /// when a tick was missed.
    pub async fn tick(&mut self) -> Instant {
        std::future::poll_fn(|cx| self.poll_tick(cx)).await
    }

    /// Polls for the next instant in the interval to be reached.
    pub fn poll_tick(&mut self, cx: &mut Context) -> Poll<Instant> {
        ready!(Pin::new(&mut self.sleep).poll(cx));

        let timeout = self.deadline;
        let now = Instant::now();

        // Timers in a JavaScript runtime are throttled heavily in background
        // tabs, so ticks are missed regularly and the behavior configured for
        // that case matters.
        self.deadline = if now > add_deadline(timeout, Duration::from_millis(5)) {
            next_deadline(self.missed_tick_behavior, timeout, now, self.period)
        } else {
            add_deadline(timeout, self.period)
        };
        self.sleep = sleep_until(self.deadline);

        Poll::Ready(timeout)
    }

    /// Resets the interval to complete one period after the current time.
    pub fn reset(&mut self) {
        self.reset_at(add_deadline(Instant::now(), self.period));
    }

    /// Resets the interval immediately.
    pub fn reset_immediately(&mut self) {
        self.reset_at(Instant::now());
    }

    /// Resets the interval after the specified duration.
    pub fn reset_after(&mut self, after: Duration) {
        self.reset_at(add_deadline(Instant::now(), after));
    }

    /// Resets the interval to a specified instant.
    pub fn reset_at(&mut self, deadline: Instant) {
        self.deadline = deadline;
        self.sleep = sleep_until(deadline);
    }

    /// Returns the behavior of missed ticks.
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.missed_tick_behavior
    }

    /// Sets the behavior of missed ticks.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.missed_tick_behavior = behavior;
    }

    /// Returns the period of the interval.
    pub fn period(&self) -> Duration {
        self.period
    }
}

/// Creates a new interval that yields with the specified period.
///
/// The first tick completes immediately.
///
/// # Panics
/// Panics if `period` is zero.
pub fn interval(period: Duration) -> Interval {
    interval_at(Instant::now(), period)
}

/// Creates a new interval that yields with the specified period, starting at
/// the specified instant.
///
/// # Panics
/// Panics if `period` is zero.
pub fn interval_at(start: Instant, period: Duration) -> Interval {
    assert!(period > Duration::ZERO, "`period` must be non-zero.");

    Interval {
        deadline: start,
        period,
        missed_tick_behavior: MissedTickBehavior::default(),
        sleep: sleep_until(start),
    }
}

pub use crate::ext::{IntervalStream, interval_stream};
