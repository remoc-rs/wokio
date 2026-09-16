Wokio 🕸️ — Tokio API for the web
=================================

[![crates.io page](https://img.shields.io/crates/v/wokio)](https://crates.io/crates/wokio)
[![docs.rs page](https://docs.rs/wokio/badge.svg)](https://docs.rs/wokio)
[![Apache 2.0 license](https://img.shields.io/crates/l/wokio)](https://raw.githubusercontent.com/remoc-rs/wokio/master/LICENSE)

Wokio provides the parts of the [Tokio] API that cannot work as-is inside a
JavaScript runtime and implements them on top of the web platform, so that 
the same code builds and runs both natively and on WebAssembly.

On native platforms every item is a direct re-export of its Tokio counterpart and
thus has no overhead whatsoever. On WebAssembly, futures are spawned as
JavaScript promises and timers use web browser timeouts, while providing 
the same `task` and `time` modules API as Tokio.

The parts of Tokio that already work unmodified on WebAssembly are re-exported
verbatim, so that `use wokio as tokio;` can be used to make existing code work on the web. 
The parts that cannot exist there at all (`net`, `fs`, `process`, `signal`) are absent.

[Tokio]: https://tokio.rs

## Usage

Include the following in your `Cargo.toml`:

```toml
[dependencies]
wokio = { version = "0.1", features = ["web"] }
```

**The `web` feature must be enabled to run on the web.** It is off by default,
because a WebAssembly target does not always mean that you want to target a web browser.
The `web` feature is only honored on WebAssembly targets. On every other target Wokio
is Tokio and the feature has no effect whatsoever, so it is safe to enable
unconditionally rather than per target.

The web backend requires a browser-like JavaScript environment, i.e. a global
object of type [`Window`] or [`WorkerGlobalScope`]. Server-side JavaScript
runtimes such as Node.js are **not** supported.
Both `wasm32-unknown-unknown` and `wasm32-wasip1-threads` (via [rust-wasi-web])
are supported. When the target supports threads, timers and blocking tasks are
thread-safe.

[`Window`]: https://developer.mozilla.org/en-US/docs/Web/API/Window
[`WorkerGlobalScope`]: https://developer.mozilla.org/en-US/docs/Web/API/WorkerGlobalScope
[rust-wasi-web]: https://github.com/rust-wasi-web

## Extensions

Wokio provides a few items that have no Tokio counterpart:

  * [`task::has_threads()`] and [`task::is_blocking_allowed()`] to query the
    capabilities of the current platform and thread,
  * [`task::MaybeSend`], the `Send` bound required to spawn a task on the current
    platform,
  * [`task::spawn_named()`] for spawning named tasks (only effective when `task-names`
    feature is active *and* the crate is built with `RUSTFLAGS=--cfg tokio_unstable`)
  * [`runtime::HandleExt`] and [`task::JoinSetExt`] providing named task spawning,
  * [`time::IntervalStream`] and [`time::interval_stream()`],
  * [`task::ThreadBound`] to make a `!Send + !Sync` value, such as a JavaScript
    object, `Send + Sync` by binding it to the thread that created it.

[`task::has_threads()`]: https://docs.rs/wokio/latest/wokio/task/fn.has_threads.html
[`task::is_blocking_allowed()`]: https://docs.rs/wokio/latest/wokio/task/fn.is_blocking_allowed.html
[`task::MaybeSend`]: https://docs.rs/wokio/latest/wokio/task/trait.MaybeSend.html
[`task::spawn_named()`]: https://docs.rs/wokio/latest/wokio/task/fn.spawn_named.html
[`runtime::HandleExt`]: https://docs.rs/wokio/latest/wokio/runtime/trait.HandleExt.html
[`task::JoinSetExt`]: https://docs.rs/wokio/latest/wokio/task/trait.JoinSetExt.html
[`time::IntervalStream`]: https://docs.rs/wokio/latest/wokio/time/struct.IntervalStream.html
[`time::interval_stream()`]: https://docs.rs/wokio/latest/wokio/time/fn.interval_stream.html
[`task::ThreadBound`]: https://docs.rs/wokio/latest/wokio/task/struct.ThreadBound.html

## Development

The test suite runs against both backends.
On native platforms use `cargo test` as usual.

To run it in a JavaScript runtime environment, install
[`wasm-bindgen-test-runner`] and [Google ChromeDriver], then use:

```text
WASM_BINDGEN_USE_BROWSER=1 WASM_BINDGEN_TEST_TIMEOUT=90 cargo test --target wasm32-unknown-unknown --all-features --release --tests
```

A proper web-compatible runtime environment is required, thus Node.js will not
work.

[`wasm-bindgen-test-runner`]: https://github.com/wasm-bindgen/wasm-bindgen
[Google ChromeDriver]: https://developer.chrome.com/docs/chromedriver/downloads

## License

Wokio is licensed under the [Apache 2.0 license].

Wokio is not affiliated with the Tokio project.

[Apache 2.0 license]: https://raw.githubusercontent.com/remoc-rs/wokio/master/LICENSE
