# Changelog
All notable changes to wokio will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.1.3 - 2026-09-16
### Added
- `task::ThreadBound` and `task::thread_bound()`, moved here from the
  `threadporter` crate: a wrapper that makes any value `Send + Sync` by
  restricting access to the thread that created it.

## 0.1.2 - 2026-09-15
### Added
- `task::AbortOnDrop` wrapper aborting a task when dropped, for all backends
- `runtime::Handle::enter` and `runtime::EnterGuard` on the web, as no-ops

## 0.1.1 - 2026-08-13
### Added
- `task::MaybeSync` bound
- `task::MaybeSendFuture` trait
- `task::BoxFuture` type 

## 0.1.0 - 2026-08-13
Initial release
