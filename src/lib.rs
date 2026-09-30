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

/// One Axum method router contributed at a path under a typed qualifier.
pub struct HttpRoute<Q: Qualifier> {
    path: String,
    router: MethodRouter,
    qualifier: PhantomData<Q>,
}

impl<Q: Qualifier> HttpRoute<Q> {
    /// Creates a route declaration while retaining Axum's underlying method router.
    pub fn new(path: impl Into<String>, router: MethodRouter) -> Self {
        Self {
            path: path.into(),
            router,
            qualifier: PhantomData,
        }
    }

    /// Returns the declared route path.
    pub fn path(&self) -> &str {
        &self.path
    }
}

impl<Q: Qualifier> Clone for HttpRoute<Q> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            router: self.router.clone(),
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
        for (module, route) in routes {
            paths
                .insert(route.path.clone(), ())
                .map_err(|_| RouteBuildError::ConflictingPath {
                    path: route.path.clone(),
                    contributor: module,
                })?;
            if by_path.contains_key(&route.path) {
                return Err(RouteBuildError::ConflictingPath {
                    path: route.path,
                    contributor: module,
                });
            }
            by_path.insert(route.path, (module, route.router));
        }
        for (path, (_, method_router)) in by_path {
            router = router.route(&path, method_router);
        }
        Ok(router)
    }
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
}

impl fmt::Display for RouteBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingPath { path, contributor } => write!(
                formatter,
                "HTTP route {path:?} from {} conflicts with another path",
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
        Err(_) => return (StatusCode::UNAUTHORIZED, "unauthorized").into_response(),
    };
    let tenant = text_header(request.headers(), "x-tenant-id");
    let correlation = text_header(request.headers(), "x-correlation-id").unwrap_or_else(|| {
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
        tenant: tenant.map(Arc::from),
        correlation_id: Arc::from(correlation),
        deadline: tokio::time::Instant::now() + requested.min(state.max_deadline),
        cancellation: cancellation.clone(),
    });
    let _cancel_on_drop = CancelOnDrop(cancellation);
    next.run(request).await
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
