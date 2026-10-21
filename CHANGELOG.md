# Changelog

## Unreleased

### Added

- `with_health(router, Health)`: `GET /livez` answers 200 while the process serves;
  `GET /readyz` answers 200 only while the readiness flag is set and every named
  check passes, otherwise 503 problem+json listing each check as `ok`, `fail` or
  `timeout` (no error text). The flag is a `tokio::sync::watch::Receiver<bool>`,
  e.g. `rustclamp_runtime::app::AppRunner::readiness()`. Paths and the per-check
  timeout (1 s) are configurable; the probes sit outside auth applied before them.

### Changed

- MSRV lowered from 1.96.1 to 1.88, set by tonic 0.14. Without the `grpc`
  feature the library also builds on 1.85.
- CI runs check and test (all features) on 1.88 and stable.

## 1.0.0 - 2026-10-19

- First release on crates.io.
