//! Tests running against both the native and the web backend.

use std::time::Duration;
use wokio::{
    runtime::{Handle, HandleExt},
    sync::mpsc,
    task::{self, JoinSetExt},
    time,
};

#[cfg(all(target_family = "wasm", feature = "web"))]
use wasm_bindgen_test::wasm_bindgen_test;

/// Whether the web backend is in use.
const WEB: bool = cfg!(all(target_family = "wasm", feature = "web"));

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn spawn_and_join() {
    assert_eq!(task::spawn(async { 42 }).await.unwrap(), 42);
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn spawn_named() {
    assert_eq!(task::spawn_named("free function", async { 42 }).await.unwrap(), 42);
    assert_eq!(Handle::current().spawn_named("handle method", async { 43 }).await.unwrap(), 43);
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn abort_task() {
    let hnd = task::spawn(std::future::pending::<()>());
    hnd.abort();
    assert!(hnd.await.unwrap_err().is_cancelled());
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn sleep_elapses() {
    let start = time::Instant::now();
    time::sleep(Duration::from_millis(50)).await;
    assert!(start.elapsed() >= Duration::from_millis(50));
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn sleep_until_deadline() {
    let deadline = time::Instant::now() + Duration::from_millis(50);
    time::sleep_until(deadline).await;
    assert!(time::Instant::now() >= deadline);
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn timeout_expires_and_passes() {
    let res = time::timeout(Duration::from_millis(50), std::future::pending::<()>()).await;
    assert!(res.is_err());

    let err: std::io::Error = res.unwrap_err().into();
    assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);

    assert_eq!(time::timeout(Duration::from_millis(50), async { 42 }).await.unwrap(), 42);
}

/// A sleep longer than the maximum delay accepted by `setTimeout` must neither
/// fail nor fire early, but re-arm the timer instead.
#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn very_long_sleep_does_not_fire() {
    assert!(time::timeout(Duration::from_millis(50), time::sleep(Duration::MAX)).await.is_err());
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn join_set_runs_tasks() {
    let (tx, mut rx) = mpsc::unbounded_channel();

    let _set = {
        let mut set = task::JoinSet::new();
        for i in 1..=3 {
            let tx = tx.clone();
            set.spawn_named("worker", async move {
                let _ = tx.send(i);
            });
        }
        set
    };
    drop(tx);

    let mut sum = 0;
    while let Some(i) = rx.recv().await {
        sum += i;
    }
    assert_eq!(sum, 6);
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn join_set_abort_all() {
    let (tx, rx) = wokio::sync::oneshot::channel::<()>();

    let mut set = task::JoinSet::new();
    set.spawn(async move {
        std::future::pending::<()>().await;
        drop(tx);
    });
    set.abort_all();

    // The sender is dropped together with the aborted task.
    assert!(rx.await.is_err());
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn timeout_at_expires_and_passes() {
    let deadline = time::Instant::now() + Duration::from_millis(50);
    assert!(time::timeout_at(deadline, std::future::pending::<()>()).await.is_err());
    assert!(time::Instant::now() >= deadline);

    let deadline = time::Instant::now() + Duration::from_millis(50);
    assert_eq!(time::timeout_at(deadline, async { 42 }).await.unwrap(), 42);
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn interval_ticks() {
    let mut interval = time::interval(Duration::from_millis(20));
    assert_eq!(interval.period(), Duration::from_millis(20));

    // The first tick completes immediately.
    let start = time::Instant::now();
    interval.tick().await;
    assert!(start.elapsed() < Duration::from_millis(20));

    interval.tick().await;
    interval.tick().await;
    assert!(start.elapsed() >= Duration::from_millis(40));
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn interval_at_starts_at_deadline() {
    let start = time::Instant::now() + Duration::from_millis(50);
    let mut interval = time::interval_at(start, Duration::from_millis(20));
    interval.tick().await;
    assert!(time::Instant::now() >= start);
}

/// A tick that was missed must not be made up for when skipping is configured,
/// which matters on the web because browsers throttle timers in background tabs.
#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn interval_missed_tick_behavior() {
    let mut interval = time::interval(Duration::from_millis(20));
    interval.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    assert_eq!(interval.missed_tick_behavior(), time::MissedTickBehavior::Skip);

    interval.tick().await;

    // Miss several ticks.
    time::sleep(Duration::from_millis(100)).await;
    interval.tick().await;

    // The next tick must not complete immediately, as it would when bursting.
    let start = time::Instant::now();
    interval.tick().await;
    assert!(start.elapsed() >= Duration::from_millis(5));
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn interval_reset() {
    let mut interval = time::interval(Duration::from_millis(20));
    interval.tick().await;

    interval.reset_after(Duration::from_millis(60));
    let start = time::Instant::now();
    interval.tick().await;
    assert!(start.elapsed() >= Duration::from_millis(60));
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn interval_stream_ticks() {
    use futures::StreamExt;

    let mut stream = time::interval_stream(Duration::from_millis(10));
    for _ in 0..3 {
        assert!(stream.next().await.is_some());
    }
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn platform_capabilities() {
    // Web workers are required for blocking, so tests running on the main thread
    // of a browser window must not allow it.
    assert_eq!(task::is_blocking_allowed(), !WEB);

    // Threads require a target with atomics support.
    assert_eq!(task::has_threads().await, !cfg!(all(target_family = "wasm", not(target_feature = "atomics"))));
}

/// Blocking tasks need threads and panics need unwinding, neither of which is
/// available on a WebAssembly target without thread support.
#[cfg(not(all(target_family = "wasm", feature = "web")))]
mod native {
    use super::*;

    #[tokio::test]
    async fn spawn_blocking() {
        assert_eq!(task::spawn_blocking(|| 42).await.unwrap(), 42);
        assert_eq!(task::spawn_blocking_named("blocking", || 43).await.unwrap(), 43);
        assert_eq!(Handle::current().spawn_blocking_named("blocking", || 44).await.unwrap(), 44);
    }

    #[tokio::test]
    async fn join_set_spawn_blocking_named() {
        let mut set = task::JoinSet::new();
        set.spawn_blocking_named("blocking", || 42);
        assert_eq!(set.join_next().await.unwrap().unwrap(), 42);
    }

    #[tokio::test]
    async fn panicking_task_yields_panic_payload() {
        let err = task::spawn(async { panic!("boom") }).await.unwrap_err();
        assert!(err.is_panic());
        assert_eq!(err.into_panic().downcast_ref::<&'static str>(), Some(&"boom"));
    }
}

wokio::task_local! {
    static GREETING: String;
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn yield_now() {
    task::yield_now().await;
    task::yield_now().await;
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn task_local() {
    GREETING
        .scope("hello".to_string(), async {
            task::yield_now().await;
            GREETING.with(|g| assert_eq!(g, "hello"));
        })
        .await;
}

#[cfg_attr(not(all(target_family = "wasm", feature = "web")), tokio::test)]
#[cfg_attr(all(target_family = "wasm", feature = "web"), wasm_bindgen_test)]
async fn unconstrained() {
    assert_eq!(task::unconstrained(async { 42 }).await, 42);
}
