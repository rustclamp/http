# rustclamp-http

Optional HTTP integration built on Axum and Tower. It owns route contribution
validation and compilation while returning the underlying Axum `Router` for
application-owned middleware, testing, and listener lifetimes. Core and Kernel
remain independent of HTTP types.

Axum was selected because it uses Tower services/layers and exposes its router
and request types directly. The integration adds no custom server protocol.
Applications may pass a developer-owned `TcpListener` to the graceful serving
helper, or adopt the compiled router in an existing Axum server.

Enable the `ws` feature for WebSocket upgrades: `ws_route` builds a `GET` upgrade
route whose open sockets receive a close frame (1001) when the shutdown signal
fires, so they do not hold the graceful drain open.

See [Phase 6 boundary evidence](../rustclamp/docs/adr/0005-phase6-integration-boundaries.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
