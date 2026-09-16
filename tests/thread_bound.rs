//! Tests for [`ThreadBound`].
//!
//! These run on native only: they exercise the checks against real foreign
//! threads and the aborting cases are driven as child processes; each has an
//! `#[ignore]`d `child_*` test doing the actual work and a driver test that runs
//! the test binary again and inspects how it died.
//!
//! The parts that make sense on a single thread are covered in `tests.rs`
//! for both backends.
#![cfg(not(target_family = "wasm"))]

use futures::{Sink, Stream};
use std::{
    collections::hash_map::DefaultHasher,
    env,
    future::Future,
    hash::{Hash, Hasher},
    marker::PhantomPinned,
    mem,
    panic::{self, AssertUnwindSafe},
    pin::Pin,
    process::{Command, Output},
    ptr,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicPtr, AtomicUsize, Ordering::SeqCst},
    },
    task::{Context, Poll, Waker},
    thread,
};
use wokio::task::{ThreadBound, thread_bound};

/// Counts its own drops, so double drops and skipped drops are both visible.
struct Counted(Arc<AtomicUsize>);

impl Counted {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        (Self(count.clone()), count)
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, SeqCst);
    }
}

/// Runs `f` on another thread and returns its result.
///
/// Uses a scoped thread so that borrows of values owned by this thread work.
fn on_other_thread<R>(f: impl FnOnce() -> R + Send) -> R
where
    R: Send,
{
    thread::scope(|s| s.spawn(f).join().unwrap())
}

/// Serializes the tests that replace the global panic hook.
static PANIC_HOOK: Mutex<()> = Mutex::new(());

/// Asserts that `f` panics when run on another thread.
///
/// `f` must not take ownership of the [`ThreadBound`] it touches, otherwise
/// unwinding would drop it on the wrong thread and abort the process.
fn expect_panic_on_other_thread(f: impl FnOnce() + Send) {
    let _guard = PANIC_HOOK.lock().unwrap_or_else(|e| e.into_inner());

    let prev = panic::take_hook();
    panic::set_hook(Box::new(|_| ()));
    let res = on_other_thread(move || panic::catch_unwind(AssertUnwindSafe(f)));
    panic::set_hook(prev);

    assert!(res.is_err(), "expected a panic on the foreign thread");
}

// ---------------------------------------------------------------------------
// Marker traits
// ---------------------------------------------------------------------------

#[test]
fn is_send_and_sync_even_for_non_send_values() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<ThreadBound<Rc<u32>>>();
    assert_send_sync::<ThreadBound<*mut u32>>();
    assert_send_sync::<ThreadBound<std::cell::RefCell<Rc<u32>>>>();
}

#[test]
fn is_unpin_when_value_is_unpin() {
    fn assert_unpin<T: Unpin>() {}

    assert_unpin::<ThreadBound<u32>>();
    // `ThreadBound<PhantomPinned>` must *not* be `Unpin`; that is what makes the
    // `Pin` projections in the `Future`/`Sink`/`Stream` impls meaningful. It is
    // exercised by `pinned_drop_on_wrong_thread_aborts`.
}

// ---------------------------------------------------------------------------
// Home thread
// ---------------------------------------------------------------------------

#[test]
fn deref_and_deref_mut() {
    let mut tb = ThreadBound::new(vec![1, 2, 3]);
    assert_eq!(tb.len(), 3);
    tb.push(4);
    assert_eq!(*tb, vec![1, 2, 3, 4]);
}

#[test]
fn thread_bound_fn_matches_new() {
    let tb = thread_bound(7u32);
    assert_eq!(*tb, 7);
    assert_eq!(ThreadBound::thread_id(&tb), thread::current().id());
}

#[test]
fn default_binds_to_current_thread() {
    let tb: ThreadBound<String> = Default::default();
    assert!(tb.is_empty());
    assert_eq!(ThreadBound::thread_id(&tb), thread::current().id());
}

#[test]
fn is_usable_on_home_thread() {
    let tb = ThreadBound::new(1u32);
    assert!(ThreadBound::is_usable(&tb));
}

#[test]
fn into_inner_returns_value_without_dropping_it() {
    let (counted, drops) = Counted::new();
    let tb = ThreadBound::new(counted);

    let counted = ThreadBound::into_inner(tb);
    assert_eq!(drops.load(SeqCst), 0, "into_inner must not drop the value");

    drop(counted);
    assert_eq!(drops.load(SeqCst), 1, "value must be dropped exactly once");
}

#[test]
fn drop_on_home_thread_drops_value_exactly_once() {
    let (counted, drops) = Counted::new();
    drop(ThreadBound::new(counted));
    assert_eq!(drops.load(SeqCst), 1);
}

#[test]
fn clone_produces_an_independent_value() {
    let (counted, drops) = Counted::new();
    let tb = ThreadBound::new(Rc::new(counted));

    let cloned = tb.clone();
    assert_eq!(Rc::strong_count(&*tb), 2);
    assert_eq!(ThreadBound::thread_id(&cloned), ThreadBound::thread_id(&tb));

    drop(cloned);
    assert_eq!(drops.load(SeqCst), 0);
    drop(tb);
    assert_eq!(drops.load(SeqCst), 1);
}

#[test]
fn comparison_and_hashing_delegate_to_the_value() {
    fn hash_of(v: &impl Hash) -> u64 {
        let mut h = DefaultHasher::new();
        v.hash(&mut h);
        h.finish()
    }

    let a = ThreadBound::new(1u32);
    let b = ThreadBound::new(2u32);

    assert_eq!(a, ThreadBound::new(1u32));
    assert_ne!(a, b);
    assert_eq!(a, 1u32);
    assert!(a < b);
    assert!(a < 2u32);
    assert_eq!(a.cmp(&b), std::cmp::Ordering::Less);
    assert_eq!(hash_of(&a), hash_of(&1u32), "must hash like the inner value");
}

#[test]
fn borrow_and_display() {
    use std::borrow::{Borrow, BorrowMut};

    let mut tb = ThreadBound::new(String::from("hello"));
    let borrowed: &String = tb.borrow();
    assert_eq!(borrowed, "hello");
    let borrowed: &mut String = tb.borrow_mut();
    borrowed.push('!');

    assert_eq!(tb.to_string(), "hello!");
}

#[test]
fn debug_shows_value_on_home_thread() {
    let tb = ThreadBound::new(42u32);
    let repr = format!("{tb:?}");
    assert!(repr.contains("42"), "{repr}");
    assert!(repr.contains("thread_id"), "{repr}");
}

// ---------------------------------------------------------------------------
// Foreign thread: permitted operations
// ---------------------------------------------------------------------------

#[test]
fn thread_id_and_is_usable_readable_from_other_thread() {
    let tb = ThreadBound::new(Rc::new(1u32));
    let home = thread::current().id();

    let (id, usable) = on_other_thread(|| (ThreadBound::thread_id(&tb), ThreadBound::is_usable(&tb)));

    assert_eq!(id, home);
    assert!(!usable);
}

#[test]
fn debug_hides_value_on_other_thread() {
    let tb = ThreadBound::new(42u32);
    let repr = on_other_thread(|| format!("{tb:?}"));

    assert!(!repr.contains("42"), "value must not be read off-thread: {repr}");
    assert!(repr.contains("thread_id"), "{repr}");
}

#[test]
fn value_without_drop_glue_may_be_dropped_on_other_thread() {
    // `needs_drop` is false, so there is no destructor anyone could rely on and
    // the drop is a permitted no-op rather than an abort.
    let tb = ThreadBound::new(42u32);
    on_other_thread(move || drop(tb));

    let tb = ThreadBound::new("no drop glue");
    on_other_thread(move || drop(tb));
}

#[test]
#[cfg_attr(miri, ignore = "leaks by design, which the Miri leak checker reports")]
fn forget_discards_a_value_on_other_thread_without_aborting() {
    let (counted, drops) = Counted::new();
    let tb = ThreadBound::new(counted);

    on_other_thread(move || mem::forget(tb));

    assert_eq!(drops.load(SeqCst), 0, "forget must leak rather than drop");
}

// ---------------------------------------------------------------------------
// Foreign thread: panicking operations
// ---------------------------------------------------------------------------

#[test]
fn deref_panics_on_other_thread() {
    let tb = ThreadBound::new(Rc::new(1u32));
    expect_panic_on_other_thread(|| {
        let _ = **tb;
    });
}

#[test]
fn deref_mut_panics_on_other_thread() {
    let mut tb = ThreadBound::new(vec![1u32]);
    expect_panic_on_other_thread(|| tb.push(2));
}

#[test]
fn display_panics_on_other_thread() {
    let tb = ThreadBound::new(String::from("x"));
    expect_panic_on_other_thread(|| {
        let _ = tb.to_string();
    });
}

#[test]
fn clone_panics_on_other_thread() {
    let tb = ThreadBound::new(String::from("x"));
    expect_panic_on_other_thread(|| {
        // The clone is never produced, so nothing is dropped off-thread.
        let _ = tb.clone();
    });
}

#[test]
fn eq_panics_on_other_thread() {
    let a = ThreadBound::new(1u32);
    let b = ThreadBound::new(1u32);
    expect_panic_on_other_thread(|| {
        let _ = a == b;
    });
}

#[test]
fn eq_against_bare_value_panics_on_other_thread() {
    let a = ThreadBound::new(1u32);
    expect_panic_on_other_thread(|| {
        let _ = a == 1u32;
    });
}

#[test]
fn ord_panics_on_other_thread() {
    let a = ThreadBound::new(1u32);
    let b = ThreadBound::new(2u32);
    expect_panic_on_other_thread(|| {
        let _ = a.cmp(&b);
    });
    expect_panic_on_other_thread(|| {
        let _ = a.partial_cmp(&b);
    });
}

#[test]
fn hash_panics_on_other_thread() {
    let tb = ThreadBound::new(1u32);
    expect_panic_on_other_thread(|| {
        let mut h = DefaultHasher::new();
        tb.hash(&mut h);
    });
}

#[test]
fn borrow_panics_on_other_thread() {
    use std::borrow::Borrow;

    let tb = ThreadBound::new(String::from("x"));
    expect_panic_on_other_thread(|| {
        let _: &String = tb.borrow();
    });
}

#[test]
fn panic_message_names_both_threads() {
    let _guard = PANIC_HOOK.lock().unwrap_or_else(|e| e.into_inner());
    let tb = ThreadBound::new(String::from("x"));

    let prev = panic::take_hook();
    panic::set_hook(Box::new(|_| ()));
    let err = on_other_thread(|| {
        panic::catch_unwind(AssertUnwindSafe(|| {
            let _ = tb.len();
        }))
        .unwrap_err()
    });
    panic::set_hook(prev);

    let msg = err.downcast_ref::<String>().expect("panic payload should be a String");
    assert!(msg.contains("alloc::string::String"), "{msg}");
    assert!(msg.contains("belongs to thread"), "{msg}");
}

// ---------------------------------------------------------------------------
// Async trait impls
// ---------------------------------------------------------------------------

fn noop_context() -> Context<'static> {
    Context::from_waker(Waker::noop())
}

#[test]
fn future_impl_polls_the_inner_future() {
    let mut fut = Box::pin(ThreadBound::new(std::future::ready(7u32)));
    assert_eq!(fut.as_mut().poll(&mut noop_context()), Poll::Ready(7));
}

#[test]
fn future_impl_panics_on_other_thread() {
    let mut fut = Box::pin(ThreadBound::new(std::future::pending::<()>()));
    expect_panic_on_other_thread(move || {
        // `pending::<()>` has no drop glue, so unwinding here does not abort.
        let _ = fut.as_mut().poll(&mut noop_context());
    });
}

/// Yields `0`, `1`, then ends; also accepts items as a [`Sink`].
#[derive(Default)]
struct Counter {
    next: u32,
    sent: Vec<u32>,
}

impl Stream for Counter {
    type Item = u32;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<u32>> {
        if self.next < 2 {
            let n = self.next;
            self.next += 1;
            Poll::Ready(Some(n))
        } else {
            Poll::Ready(None)
        }
    }
}

impl Sink<u32> for Counter {
    type Error = ();

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: u32) -> Result<(), ()> {
        self.sent.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), ()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn stream_impl_polls_the_inner_stream() {
    let mut s = Box::pin(ThreadBound::new(Counter::default()));
    assert_eq!(s.as_mut().poll_next(&mut noop_context()), Poll::Ready(Some(0)));
    assert_eq!(s.as_mut().poll_next(&mut noop_context()), Poll::Ready(Some(1)));
    assert_eq!(s.as_mut().poll_next(&mut noop_context()), Poll::Ready(None));
}

#[test]
fn sink_impl_forwards_to_the_inner_sink() {
    let mut s = Box::pin(ThreadBound::new(Counter::default()));
    assert_eq!(s.as_mut().poll_ready(&mut noop_context()), Poll::Ready(Ok(())));
    s.as_mut().start_send(9).unwrap();
    assert_eq!(s.as_mut().poll_flush(&mut noop_context()), Poll::Ready(Ok(())));
    assert_eq!(s.as_mut().poll_close(&mut noop_context()), Poll::Ready(Ok(())));
    assert_eq!(s.sent, vec![9]);
}

// ---------------------------------------------------------------------------
// Aborting operations, driven as child processes
// ---------------------------------------------------------------------------

/// Re-runs this test binary for the single `#[ignore]`d test named `test`.
fn run_child(test: &str) -> Output {
    Command::new(env::current_exe().unwrap())
        // `--nocapture` is required: the library writes its abort message straight
        // to stderr, and libtest's capture would swallow it before the abort.
        .args(["--exact", "--ignored", "--nocapture", test])
        .output()
        .expect("failed to re-run the test binary")
}

/// Asserts that the child died by abort, complaining about `type_name`.
fn assert_aborted(out: &Output, type_name: &str) {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let ctx = format!("status: {:?}\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}", out.status);

    assert!(!out.status.success(), "child was expected to abort\n{ctx}");

    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(out.status.signal(), Some(libc_sigabrt()), "expected SIGABRT\n{ctx}");
    }

    assert!(stderr.contains("cannot drop"), "missing abort message\n{ctx}");
    assert!(stderr.contains(type_name), "abort message should name the type\n{ctx}");
}

#[cfg(unix)]
fn libc_sigabrt() -> i32 {
    6
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot spawn processes")]
fn drop_on_wrong_thread_aborts() {
    assert_aborted(&run_child("child_drop_on_wrong_thread"), "alloc::string::String");
}

#[test]
#[ignore = "aborts the process; driven by drop_on_wrong_thread_aborts"]
fn child_drop_on_wrong_thread() {
    let tb = ThreadBound::new(String::from("boom"));
    let _ = thread::spawn(move || drop(tb)).join();
    unreachable!("dropping on the wrong thread should have aborted");
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot spawn processes")]
fn into_inner_on_wrong_thread_aborts() {
    assert_aborted(&run_child("child_into_inner_on_wrong_thread"), "alloc::string::String");
}

#[test]
#[ignore = "aborts the process; driven by into_inner_on_wrong_thread_aborts"]
fn child_into_inner_on_wrong_thread() {
    // `check` panics, then unwinding drops `tb` on the wrong thread, which aborts.
    let tb = ThreadBound::new(String::from("boom"));
    let _ = thread::spawn(move || ThreadBound::into_inner(tb)).join();
    unreachable!("into_inner on the wrong thread should have aborted");
}

/// Registers its own address while pinned and clears it on drop, the way an
/// intrusive collection does. Skipping its destructor leaves a dangling pointer.
struct Registering {
    _pin: PhantomPinned,
}

static REGISTERED: AtomicPtr<Registering> = AtomicPtr::new(ptr::null_mut());

impl Future for Registering {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<()> {
        // Sound only because the `Pin` drop guarantee promises `Drop` will run
        // before this address becomes invalid.
        let this = unsafe { self.get_unchecked_mut() } as *mut Registering;
        REGISTERED.store(this, SeqCst);
        Poll::Pending
    }
}

impl Drop for Registering {
    fn drop(&mut self) {
        REGISTERED.store(ptr::null_mut(), SeqCst);
    }
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot spawn processes")]
fn pinned_drop_on_wrong_thread_aborts() {
    let out = run_child("child_pinned_drop_on_wrong_thread");
    assert_aborted(&out, "thread_bound::Registering");

    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("registered"), "child never reached the pinned state:\n{stdout}");
}

#[test]
#[ignore = "aborts the process; driven by pinned_drop_on_wrong_thread_aborts"]
fn child_pinned_drop_on_wrong_thread() {
    let mut fut = Box::pin(ThreadBound::new(Registering { _pin: PhantomPinned }));

    // Polling goes through the crate's own `Pin<&mut Self>` -> `Pin<&mut T>`
    // projection, so the inner value is now pinned and registered.
    assert_eq!(fut.as_mut().poll(&mut noop_context()), Poll::Pending);
    assert!(!REGISTERED.load(SeqCst).is_null());
    println!("registered");

    // Without the abort, the box would be freed while `REGISTERED` still points
    // into it: use after free reachable from entirely safe code.
    let _ = thread::spawn(move || drop(fut)).join();
    unreachable!("dropping a pinned value on the wrong thread should have aborted");
}
