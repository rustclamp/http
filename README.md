<img src="https://docs.rustclamp.com/assets/rustclamp-logo.png" alt="RustClamp logo" width="160">

# rustclamp-http

Axum and Tower integration for [RustClamp](https://github.com/rustclamp/rustclamp).
Modules contribute routes; this crate validates them (duplicate paths fail at
composition time) and compiles them into a plain Axum `Router`. You keep the
listener, the runtime and any extra middleware. Core and Kernel stay free of HTTP types.

## Install

Not yet published to crates.io; depend on it from git (Rust 1.96.1+, edition 2024):

```toml
[dependencies]
rustclamp-http = { git = "https://github.com/rustclamp/http" }
```

## Example

```rust
use axum::routing::get;
use rustclamp_core::{ContributionTarget, ModuleId};
use rustclamp_http::{HttpRoute, HttpRoutes, Public};

const USERS: ModuleId = ModuleId::new("app.users");

let routes = vec![(USERS, HttpRoute::<Public>::new("/users", get(|| async { "users" })))];
let router = HttpRoutes::<Public>::new().build(&routes)?;
// serve(listener, router, shutdown, drain_timeout).await
```

## Main API

- `HttpRoute`, `HttpRoutes`, `RouteBuildError`: route contribution and compilation, per qualifier (`Public` is provided).
- `serve`: graceful serving on a caller-owned `TcpListener`; the drain timeout starts at the shutdown signal.
- `with_request_context`, `RequestContext`, `PrincipalResolver`: principal, tenant, deadline and cancellation per request; honors a valid `X-Request-Id` or generates one, and echoes it. Missing auth answers 401 as problem+json.
- `require_role`: 403 problem+json unless `PrincipalResolver::roles` grants the role.
- `HttpError` (`problem_json`, `with_field_error`), `negotiate`, `streaming_body`.
- `with_body_limit`, `with_extractor_body_limit`: request size limits.

`HEAD` and `405` `Allow` handling is Axum's; register `head(...)` explicitly to answer `HEAD` differently.

## Features

| Feature | Adds |
| --- | --- |
| `cors` | `with_cors` |
| `access-log` | `with_access_log` |
| `rate-limit` | `with_rate_limit`: in-process fixed window per key, 429 problem+json with `Retry-After` |
| `ws` | `ws_route`: `GET` WebSocket upgrade; open sockets get a close frame (1001) on shutdown so they do not hold the drain open |

Full documentation: <https://docs.rustclamp.com>

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
