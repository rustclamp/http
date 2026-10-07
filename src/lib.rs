//! Axum and Tower integration for qualified route contributions.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::MethodRouter;
use futures_core::Stream;
use matchit::Router as PathRouter;
use rustclamp_core::{
    Contribution, ContributionId, ContributionTarget, ContributionTargetId, ModuleId, Qualifier,
};
use tokio::net::TcpListener;

/// Underlying Axum types remain available to callers.
pub use axum as ecosystem;
/// Tokio remains available so callers bind the listener `serve` expects.
pub use tokio;
/// Tower HTTP middleware remains available to callers.
pub use tower_http;

/// Public route qualifier for ordinary application routes.
pub struct Public;

impl Qualifier for Public {
    const ID: rustclamp_core::QualifierId =
        rustclamp_core::QualifierId::new("rustclamp.http.public");
}

/// Stable contribution identity shared by HTTP route declarations.
pub const HTTP_ROUTE_CONTRIBUTION: ContributionId = ContributionId::new("rustclamp.http.route");

/// Stable target identity for one qualified route set.
pub const HTTP_ROUTES_TARGET: ContributionTargetId =
    ContributionTargetId::new("rustclamp.http.routes");

/// One Axum method router contributed at a path, or a whole Axum router
/// mounted under a prefix, under a typed qualifier.
pub struct HttpRoute<Q: Qualifier> {
    path: String,
    entry: Entry,
    qualifier: PhantomData<Q>,
}

#[derive(Clone)]
enum Entry {
    Method(Box<MethodRouter>),
    Mount(Router),
}

impl<Q: Qualifier> HttpRoute<Q> {
    /// Creates a route declaration while retaining Axum's underlying method router.
    pub fn new(path: impl Into<String>, router: MethodRouter) -> Self {
        Self {
            path: path.into(),
            entry: Entry::Method(Box::new(router)),
            qualifier: PhantomData,
        }
    }

    /// Mounts an existing Axum router, with its nested routers, layers and
    /// state already applied, under `prefix`: an Axum app joins Clamp as one
    /// contribution instead of one per route.
    ///
    /// `prefix` is a static path such as `/legacy`; the router then answers
    /// `/legacy` and everything below it. The prefix `/` mounts the router at
    /// the root: it answers every request no other contribution matches.
    /// A contributed path inside a mounted prefix, overlapping prefixes, or
    /// two root mounts fail [`HttpRoutes::build`].
    pub fn mount(prefix: impl Into<String>, router: Router) -> Self {
        Self {
            path: prefix.into(),
            entry: Entry::Mount(router),
            qualifier: PhantomData,
        }
    }

    /// Contributes a tonic service (a generated `FooServer`) at its gRPC path
    /// prefix `/package.Service/`, beside ordinary routes on the same listener.
    ///
    /// The service sees the full request path, so it answers its own unknown
    /// methods with gRPC `Unimplemented`. Two contributions of one service
    /// fail [`HttpRoutes::build`] like any duplicate path.
    #[cfg(feature = "grpc")]
    pub fn grpc<S>(service: S) -> Self
    where
        S: tonic::server::NamedService
            + tonic::codegen::Service<Request, Error = std::convert::Infallible>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Response: IntoResponse + 'static,
        S::Future: Send + 'static,
    {
        // ponytail: a catch-all route, not a nest: nesting would strip the
        // prefix the generated service matches on.
        Self::new(
            format!("/{}/{{*method}}", S::NAME),
            axum::routing::any_service(service),
        )
    }

    /// Returns the declared route path, or the prefix of a mounted router.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl<Q: Qualifier> Clone for HttpRoute<Q> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            entry: self.entry.clone(),
            qualifier: PhantomData,
        }
    }
}

impl<Q: Qualifier> Contribution for HttpRoute<Q> {
    const ID: ContributionId = HTTP_ROUTE_CONTRIBUTION;
}

/// Builds and validates one qualifier's route contributions.
pub struct HttpRoutes<Q: Qualifier>(PhantomData<Q>);

impl<Q: Qualifier> HttpRoutes<Q> {
    /// Creates a route target for the selected qualifier.
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<Q: Qualifier> Default for HttpRoutes<Q> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Q: Qualifier> ContributionTarget for HttpRoutes<Q> {
    type Contribution = HttpRoute<Q>;
    type Runtime = Router;
    type Error = RouteBuildError;

    const ID: ContributionTargetId = HTTP_ROUTES_TARGET;

    fn build(
        &self,
        contributions: &[(ModuleId, Self::Contribution)],
    ) -> Result<Router, Self::Error> {
        let mut paths = PathRouter::new();
        let mut routes = contributions.to_vec();
        routes.sort_by(|left, right| left.1.path.cmp(&right.1.path).then(left.0.cmp(&right.0)));
        let mut router = Router::new();
        let mut by_path: BTreeMap<String, (ModuleId, MethodRouter)> = BTreeMap::new();
        let mut mounts: Vec<(String, ModuleId, Router)> = Vec::new();
        let mut root: Option<Router> = None;
        for (module, route) in routes {
            let conflict = |path: &str| RouteBuildError::ConflictingPath {
                path: path.to_owned(),
                contributor: module,
            };
            match route.entry {
                Entry::Method(method_router) => {
                    paths
                        .insert(route.path.clone(), ())
                        .map_err(|_| conflict(&route.path))?;
                    if by_path.contains_key(&route.path) {
                        return Err(conflict(&route.path));
                    }
                    by_path.insert(route.path, (module, *method_router));
                }
                Entry::Mount(mounted) if route.path == "/" => {
                    // Two fallbacks cannot share the root.
                    if root.replace(mounted).is_some() {
                        return Err(conflict("/"));
                    }
                }
                Entry::Mount(mounted) => {
                    let prefix = route.path;
                    if !valid_mount_prefix(&prefix) {
                        return Err(RouteBuildError::InvalidMount {
                            prefix,
                            contributor: module,
                        });
                    }
                    for pattern in [prefix.clone(), format!("{prefix}/{{*rest}}")] {
                        paths.insert(pattern, ()).map_err(|_| conflict(&prefix))?;
                    }
                    mounts.push((prefix, module, mounted));
                }
            }
        }
        // A mounted router owns its whole subtree: nothing else may claim a
        // path inside it, where it would silently shadow the mounted route.
        for (prefix, _, _) in &mounts {
            let inside = |path: &str| {
                path.strip_prefix(prefix.as_str())
                    .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
            };
            if let Some((path, (module, _))) = by_path.iter().find(|(path, _)| inside(path)) {
                return Err(RouteBuildError::ConflictingPath {
                    path: path.clone(),
                    contributor: *module,
                });
            }
            if let Some((path, module, _)) = mounts
                .iter()
                .find(|(other, _, _)| other != prefix && inside(other))
            {
                return Err(RouteBuildError::ConflictingPath {
                    path: path.clone(),
                    contributor: *module,
                });
            }
        }
        for (path, (_, method_router)) in by_path {
            router = router.route(&path, method_router);
        }
        for (prefix, _, mounted) in mounts {
            router = router.nest(&prefix, mounted);
        }
        if let Some(mounted) = root {
            router = router.fallback_service(mounted);
        }
        Ok(router)
    }
}

/// A static path below the root: Axum nests only those, and a trailing `/`
/// would mount at a different path than the one declared.
fn valid_mount_prefix(prefix: &str) -> bool {
    prefix.len() > 1
        && prefix.starts_with('/')
        && !prefix.ends_with('/')
        && !prefix.contains(['{', '}', '*'])
}

/// Route-target validation failure with contributor identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RouteBuildError {
    /// Axum's route matcher found a duplicate or overlapping path pattern.
    ConflictingPath {
        /// Path that overlaps an earlier declaration.
        path: String,
        /// Contributor whose declaration conflicts.
        contributor: ModuleId,
    },
    /// A mount prefix is not a static path such as `/legacy`.
    InvalidMount {
        /// Prefix as declared.
        prefix: String,
        /// Contributor of the mount.
        contributor: ModuleId,
    },
}

impl fmt::Display for RouteBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingPath { path, contributor } => write!(
                formatter,
                "HTTP route {path:?} from {} conflicts with another path",
                contributor.as_str()
            ),
            Self::InvalidMount {
                prefix,
                contributor,
            } => write!(
                formatter,
                "HTTP mount {prefix:?} from {} must be a static path below the root, like \"/legacy\"",
                contributor.as_str()
            ),
        }
    }
}

impl std::error::Error for RouteBuildError {}

/// Serves a compiled router on a caller-owned listener with a bounded graceful drain.
///
/// The caller owns `listener` and the Tokio runtime. On shutdown, Axum stops
/// accepting connections and finishes active requests for up to
/// `drain_timeout`, counted from the shutdown signal; timing out drops the
/// server future, cancelling outstanding request work.
pub async fn serve(
    listener: TcpListener,
    router: Router,
    shutdown: impl FutureShutdown,
    drain_timeout: Duration,
) -> io::Result<()> {
    let (signalled, drain_started) = tokio::sync::oneshot::channel();
    let signal = shutdown.wait();
    let server = axum::serve(listener, router).with_graceful_shutdown(async move {
        signal.await;
        let _ = signalled.send(());
    });
    // The drain clock starts at the signal, not at startup.
    let deadline = async move {
        match drain_started.await {
            Ok(()) => tokio::time::sleep(drain_timeout).await,
            // The server stopped without a signal; its own result decides.
            Err(_) => std::future::pending().await,
        }
    };
    tokio::select! {
        result = server => result,
        () = deadline => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "HTTP graceful drain timed out",
        )),
    }
}

/// Shutdown future supplied by the application-owned process runtime.
pub trait FutureShutdown {
    /// Returns a future that completes when shutdown is requested.
    fn wait(self) -> PinFuture;
}

/// Boxed shutdown future accepted by the listener adapter.
pub type PinFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;

/// A shutdown future adapter for a Tokio watch receiver.
pub struct ShutdownReceiver(pub tokio::sync::watch::Receiver<bool>);

impl FutureShutdown for ShutdownReceiver {
    fn wait(mut self) -> PinFuture {
        Box::pin(async move { while !*self.0.borrow() && self.0.changed().await.is_ok() {} })
    }
}

/// Any `Send` future, e.g. `rustclamp_runtime::tokio_runtime::shutdown_signal()`, is a shutdown signal.
impl<F> FutureShutdown for F
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    fn wait(self) -> PinFuture {
        Box::pin(self)
    }
}

/// Identity lookup delegated to the application's authentication boundary.
pub trait PrincipalResolver: Send + Sync + 'static {
    /// Returns an authenticated principal or a rejected credential diagnostic.
    fn resolve(&self, headers: &HeaderMap) -> Result<Option<String>, String>;

    /// Returns the roles granted to an authenticated principal; none by default.
    fn roles(&self, _principal: &str) -> Vec<String> {
        Vec::new()
    }
}

/// Cancellation flag shared by a request handler and its adapters.
#[derive(Clone, Debug, Default)]
pub struct RequestCancellation(Arc<AtomicBool>);

impl RequestCancellation {
    /// Requests cancellation of work associated with this request.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Reports whether work should stop.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Identity and execution metadata extracted at the HTTP boundary.
#[derive(Clone, Debug)]
pub struct RequestContext {
    principal: Option<Arc<str>>,
    roles: Arc<[String]>,
    tenant: Option<Arc<str>>,
    correlation_id: Arc<str>,
    deadline: tokio::time::Instant,
    cancellation: RequestCancellation,
}

impl RequestContext {
    /// Returns the authenticated principal, if one was resolved.
    pub fn principal(&self) -> Option<&str> {
        self.principal.as_deref()
    }
    /// Reports whether the resolver granted the principal `role`.
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|granted| granted == role)
    }
    /// Returns the selected tenant, if supplied and accepted by the application.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }
    /// Returns the request correlation identity.
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }
    /// Returns the bounded monotonic deadline.
    pub fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }
    /// Returns request cancellation state.
    pub fn cancellation(&self) -> &RequestCancellation {
        &self.cancellation
    }
}

#[derive(Clone)]
struct ContextState {
    resolver: Arc<dyn PrincipalResolver>,
    max_deadline: Duration,
}

/// Adds resolved identity and execution metadata to Axum request extensions.
pub fn with_request_context(
    router: Router,
    resolver: Arc<dyn PrincipalResolver>,
    max_deadline: Duration,
) -> Router {
    router.layer(middleware::from_fn_with_state(
        ContextState {
            resolver,
            max_deadline,
        },
        request_context_middleware,
    ))
}

async fn request_context_middleware(
    State(state): State<ContextState>,
    mut request: Request,
    next: Next,
) -> Response {
    let principal = match state.resolver.resolve(request.headers()) {
        Ok(principal) => principal,
        Err(_) => return problem(StatusCode::UNAUTHORIZED, "unauthorized"),
    };
    let roles = principal
        .as_deref()
        .map(|principal| state.resolver.roles(principal))
        .unwrap_or_default();
    let tenant = text_header(request.headers(), "x-tenant-id");
    // ponytail: an invalid or missing id is replaced, never rejected.
    let correlation = ["x-request-id", "x-correlation-id"]
        .into_iter()
        .find_map(|name| text_header(request.headers(), name).filter(|id| valid_request_id(id)))
        .unwrap_or_else(|| {
            format!(
                "request-{}",
                CORRELATION_IDS.fetch_add(1, Ordering::Relaxed)
            )
        });
    let requested = text_header(request.headers(), "x-timeout-ms")
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or(state.max_deadline);
    let cancellation = RequestCancellation::default();
    request.extensions_mut().insert(RequestContext {
        principal: principal.map(Arc::from),
        roles: roles.into(),
        tenant: tenant.map(Arc::from),
        correlation_id: Arc::from(correlation.as_str()),
        deadline: tokio::time::Instant::now() + requested.min(state.max_deadline),
        cancellation: cancellation.clone(),
    });
    let _cancel_on_drop = CancelOnDrop(cancellation);
    let mut response = next.run(request).await;
    if let Ok(value) = correlation.parse() {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

/// Visible ASCII, 1 to 128 bytes.
fn valid_request_id(id: &str) -> bool {
    (1..=128).contains(&id.len()) && id.bytes().all(|byte| byte.is_ascii_graphic())
}

fn problem(status: StatusCode, detail: &'static str) -> Response {
    HttpError::new((), status, detail)
        .problem_json()
        .into_response()
}

/// Answers 401 (no principal) or 403 (role not granted) as problem+json.
///
/// Apply it to a route or router inside [`with_request_context`], which
/// supplies the principal and roles.
pub fn require_role(router: Router, role: &'static str) -> Router {
    router.layer(middleware::from_fn(
        move |request: Request, next: Next| async move {
            match request.extensions().get::<RequestContext>() {
                Some(context) if context.has_role(role) => next.run(request).await,
                Some(context) if context.principal().is_some() => {
                    problem(StatusCode::FORBIDDEN, "forbidden")
                }
                _ => problem(StatusCode::UNAUTHORIZED, "unauthorized"),
            }
        },
    ))
}

static CORRELATION_IDS: AtomicU64 = AtomicU64::new(1);

fn text_header(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers.get(name)?.to_str().ok().map(str::to_owned)
}

struct CancelOnDrop(RequestCancellation);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Supported HTTP representation selected from the `Accept` header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Representation {
    /// JSON media types.
    Json,
    /// HTML media type.
    Html,
    /// XML media types.
    Xml,
    /// Binary octet stream.
    Binary,
    /// Event stream.
    Stream,
}

/// No supported media type had an acceptable quality value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnsupportedRepresentation;

impl fmt::Display for UnsupportedRepresentation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("no acceptable representation is available")
    }
}
impl Error for UnsupportedRepresentation {}

/// Selects the highest-quality supported media type; ties preserve client order.
pub fn negotiate(accept: Option<&str>) -> Result<Representation, UnsupportedRepresentation> {
    let Some(accept) = accept else {
        return Ok(Representation::Json);
    };
    let mut values = accept
        .split(',')
        .enumerate()
        .filter_map(|(order, entry)| {
            let mut parts = entry.trim().split(';');
            let media = parts.next()?.trim();
            let quality = parts
                .find_map(|part| {
                    let (name, value) = part.trim().split_once('=')?;
                    (name.trim() == "q")
                        .then(|| value.trim().parse::<f32>().ok())
                        .flatten()
                })
                .unwrap_or(1.0);
            (quality > 0.0).then_some((quality, order, media))
        })
        .collect::<Vec<_>>();
    values.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
    values
        .into_iter()
        .find_map(|(_, _, media)| match media {
            "application/json" | "*/*" => Some(Representation::Json),
            "text/html" => Some(Representation::Html),
            "application/xml" | "text/xml" => Some(Representation::Xml),
            "application/octet-stream" => Some(Representation::Binary),
            "text/event-stream" => Some(Representation::Stream),
            _ => None,
        })
        .ok_or(UnsupportedRepresentation)
}

/// Typed application failure paired with an explicit public status and message.
pub struct HttpError<E> {
    source: E,
    status: StatusCode,
    public_message: &'static str,
    problem: bool,
    field_errors: BTreeMap<String, Vec<String>>,
}

impl<E> HttpError<E> {
    /// Preserves the source error for diagnostics while selecting public output.
    pub const fn new(source: E, status: StatusCode, public_message: &'static str) -> Self {
        Self {
            source,
            status,
            public_message,
            problem: false,
            field_errors: BTreeMap::new(),
        }
    }
    /// Renders an RFC 9457 `application/problem+json` body instead of plain text.
    pub fn problem_json(mut self) -> Self {
        self.problem = true;
        self
    }
    /// Adds a field-level validation message under `errors`; implies [`Self::problem_json`].
    pub fn with_field_error(
        mut self,
        field: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        self.field_errors
            .entry(field.into())
            .or_default()
            .push(message.into());
        self.problem_json()
    }
    /// Returns the original typed error for internal diagnostics.
    pub fn source_error(&self) -> &E {
        &self.source
    }
}

impl<E> IntoResponse for HttpError<E> {
    fn into_response(self) -> Response {
        if !self.problem {
            return (self.status, self.public_message).into_response();
        }
        let mut body = serde_json::json!({
            "type": "about:blank",
            "title": self.status.canonical_reason().unwrap_or("Error"),
            "status": self.status.as_u16(),
            "detail": self.public_message,
        });
        if !self.field_errors.is_empty() {
            body["errors"] = serde_json::json!(self.field_errors);
        }
        (
            self.status,
            [(axum::http::header::CONTENT_TYPE, "application/problem+json")],
            body.to_string(),
        )
            .into_response()
    }
}

/// Wraps a stream directly as an HTTP body without collecting it in memory.
pub fn streaming_body<S, E>(stream: S) -> Body
where
    S: Stream<Item = Result<bytes::Bytes, E>> + Send + 'static,
    E: Into<Box<dyn Error + Send + Sync>> + 'static,
{
    Body::from_stream(stream)
}

/// Caps bodies read by extractors (`Bytes`, `Json`, ...) at `max_bytes`.
///
/// Unlike [`with_body_limit`] the check runs when a handler reads the body, so
/// routing (404/405) and layers such as [`with_request_context`] (401) answer
/// first and only then 413 applies.
///
/// ponytail: handlers that consume `Body` directly bypass it; use
/// [`with_body_limit`] when every byte must be capped.
pub fn with_extractor_body_limit(router: Router, max_bytes: usize) -> Router {
    router.layer(axum::extract::DefaultBodyLimit::max(max_bytes))
}

/// Applies Tower HTTP's request-body limit layer.
///
/// It rejects before routing and auth; see [`with_extractor_body_limit`] for
/// the variant that lets 404/401/403 take precedence over 413.
pub fn with_body_limit(router: Router, max_bytes: usize) -> Router {
    router.layer(tower_http::limit::RequestBodyLimitLayer::new(max_bytes))
}

/// Adds a CORS layer; build the policy with [`tower_http::cors`].
#[cfg(feature = "cors")]
pub fn with_cors(router: Router, cors: tower_http::cors::CorsLayer) -> Router {
    router.layer(cors)
}

/// Logs one line per request and response through `tracing` at INFO.
#[cfg(feature = "access-log")]
pub fn with_access_log(router: Router) -> Router {
    router.layer(tower_http::trace::TraceLayer::new_for_http())
}

/// Fixed-window per-key rate limit answering 429 problem+json with `Retry-After`.
///
/// `key` picks the bucket for a request (client address header, principal, ...);
/// `None` skips limiting. At most `limit` requests per key per `window`.
///
/// ponytail: state is in-process and pruned lazily; use a shared store for a
/// multi-instance limit.
#[cfg(feature = "rate-limit")]
pub fn with_rate_limit(
    router: Router,
    limit: u32,
    window: Duration,
    key: fn(&Request) -> Option<String>,
) -> Router {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::time::Instant;
    let windows: Arc<Mutex<HashMap<String, (Instant, u32)>>> = Arc::default();
    router.layer(middleware::from_fn(move |request: Request, next: Next| {
        let windows = windows.clone();
        async move {
            let retry_after = key(&request).and_then(|key| {
                let now = Instant::now();
                let mut windows = windows.lock().unwrap_or_else(|e| e.into_inner());
                if windows.len() > 1024 {
                    windows.retain(|_, (start, _)| now.duration_since(*start) < window);
                }
                let entry = windows.entry(key).or_insert((now, 0));
                if now.duration_since(entry.0) >= window {
                    *entry = (now, 0);
                }
                entry.1 += 1;
                (entry.1 > limit).then(|| window.saturating_sub(now.duration_since(entry.0)))
            });
            match retry_after {
                None => next.run(request).await,
                Some(wait) => {
                    let mut response =
                        problem(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded");
                    response
                        .headers_mut()
                        .insert("retry-after", wait.as_secs().max(1).into());
                    response
                }
            }
        }
    }))
}

/// Boxed future borrowing the socket for the duration of a WebSocket session.
#[cfg(feature = "ws")]
pub type SocketFuture<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

/// Builds a `GET` upgrade route whose sockets are closed when `shutdown` fires.
///
/// `handler` runs one session on the upgraded socket. When the shutdown signal
/// fires first, the session future is dropped and the peer receives a close
/// frame (1001, going away), so upgraded connections do not hold the graceful
/// drain open until its timeout.
#[cfg(feature = "ws")]
pub fn ws_route<H>(shutdown: tokio::sync::watch::Receiver<bool>, handler: H) -> MethodRouter
where
    H: for<'a> Fn(&'a mut axum::extract::ws::WebSocket) -> SocketFuture<'a>
        + Clone
        + Send
        + Sync
        + 'static,
{
    use axum::extract::ws::{CloseFrame, Message, WebSocketUpgrade, close_code};
    axum::routing::get(move |upgrade: WebSocketUpgrade| async move {
        upgrade.on_upgrade(move |mut socket| async move {
            let stop = ShutdownReceiver(shutdown).wait();
            tokio::select! {
                () = handler(&mut socket) => {}
                () = stop => {
                    let frame = CloseFrame { code: close_code::AWAY, reason: "server shutting down".into() };
                    let _ = socket.send(Message::Close(Some(frame))).await;
                }
            }
        })
    })
}
