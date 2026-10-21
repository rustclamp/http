//! `Idempotency-Key` handling for unsafe methods (feature `idempotency`).

use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{OriginalUri, Request, State};
use axum::http::{HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use sha2::{Digest, Sha256};

use crate::{RequestContext, problem};

/// Response header set on a replayed response.
pub const IDEMPOTENT_REPLAYED: &str = "idempotent-replayed";

/// Response headers stored for replay; everything else (cookies, hop-by-hop,
/// `Date`, the request id) is dropped.
const SAFE_HEADERS: [&str; 6] = [
    "content-type",
    "content-language",
    "content-location",
    "location",
    "etag",
    "last-modified",
];

/// Failure reported by an [`IdempotencyStore`].
pub type IdempotencyError = Box<dyn Error + Send + Sync>;

/// Boxed future returned by [`IdempotencyStore`] methods.
pub type StoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, IdempotencyError>> + Send + 'a>>;

/// Scope of one idempotency key: who sent it, and to which operation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct IdempotencyKey {
    /// [`RequestContext::principal`], or `""` for anonymous requests, which
    /// share one scope: their keys must be unguessable.
    pub principal: String,
    /// The `Idempotency-Key` header value.
    pub key: String,
    /// Request method, e.g. `POST`.
    pub method: String,
    /// Request path without the query string, before any `nest` stripping.
    pub path: String,
}

/// A response kept for replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredResponse {
    /// Status code.
    pub status: u16,
    /// Stored header subset, in response order.
    pub headers: Vec<(String, Vec<u8>)>,
    /// Complete response body.
    pub body: Vec<u8>,
}

/// What a store holds for a claimed key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdempotencyRecord {
    /// SHA-256 of the request body that claimed the key.
    pub fingerprint: [u8; 32],
    /// `None` while the first request is still in flight.
    pub response: Option<StoredResponse>,
}

/// Outcome of [`IdempotencyStore::claim`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Claim {
    /// The key was free (or expired) and is now in progress for this request.
    Claimed,
    /// An unexpired record already holds the key.
    Existing(IdempotencyRecord),
}

/// Shared storage for idempotency records.
///
/// `claim` must be atomic: of two concurrent claims of one key, exactly one
/// returns [`Claim::Claimed`].
pub trait IdempotencyStore: Send + Sync + 'static {
    /// Claims `key` as in progress until `ttl` from now, unless an unexpired
    /// record holds it; an expired record is replaced.
    fn claim<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fingerprint: [u8; 32],
        ttl: Duration,
    ) -> StoreFuture<'a, Claim>;

    /// Stores the response of a claimed key; the expiry set by `claim` stays.
    fn complete<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        response: &'a StoredResponse,
    ) -> StoreFuture<'a, ()>;

    /// Forgets a claimed key so the next request with it runs the handler.
    fn release<'a>(&'a self, key: &'a IdempotencyKey) -> StoreFuture<'a, ()>;
}

/// In-process [`IdempotencyStore`] for tests and development.
///
/// ponytail: one mutex, expired entries pruned lazily; not shared between
/// instances. Use a database store (rustclamp-postgres) in production.
#[derive(Debug, Default)]
pub struct MemoryIdempotencyStore(Mutex<HashMap<IdempotencyKey, (Instant, IdempotencyRecord)>>);

impl MemoryIdempotencyStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn records(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<IdempotencyKey, (Instant, IdempotencyRecord)>> {
        self.0.lock().unwrap_or_else(|error| error.into_inner())
    }
}

impl IdempotencyStore for MemoryIdempotencyStore {
    fn claim<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        fingerprint: [u8; 32],
        ttl: Duration,
    ) -> StoreFuture<'a, Claim> {
        let now = Instant::now();
        let mut records = self.records();
        if records.len() > 1024 {
            records.retain(|_, (expires, _)| *expires > now);
        }
        let claim = match records.get(key) {
            Some((expires, record)) if *expires > now => Claim::Existing(record.clone()),
            _ => {
                let record = IdempotencyRecord {
                    fingerprint,
                    response: None,
                };
                records.insert(key.clone(), (now + ttl, record));
                Claim::Claimed
            }
        };
        Box::pin(std::future::ready(Ok(claim)))
    }

    fn complete<'a>(
        &'a self,
        key: &'a IdempotencyKey,
        response: &'a StoredResponse,
    ) -> StoreFuture<'a, ()> {
        if let Some((_, record)) = self.records().get_mut(key) {
            record.response = Some(response.clone());
        }
        Box::pin(std::future::ready(Ok(())))
    }

    fn release<'a>(&'a self, key: &'a IdempotencyKey) -> StoreFuture<'a, ()> {
        self.records().remove(key);
        Box::pin(std::future::ready(Ok(())))
    }
}

/// Configuration for [`with_idempotency`].
#[derive(Clone)]
pub struct Idempotency {
    store: Arc<dyn IdempotencyStore>,
    ttl: Duration,
    max_body: usize,
    methods: Arc<[Method]>,
}

impl Idempotency {
    /// Keys expire after 24 hours; request and response bodies are capped at
    /// 2 MiB (Axum's default extractor limit); POST, PUT, PATCH and DELETE
    /// are covered.
    pub fn new(store: Arc<dyn IdempotencyStore>) -> Self {
        Self {
            store,
            ttl: Duration::from_secs(24 * 60 * 60),
            max_body: 2 * 1024 * 1024,
            methods: Arc::new([Method::POST, Method::PUT, Method::PATCH, Method::DELETE]),
        }
    }

    /// Sets how long a key is held, counted from its first request. An
    /// in-progress claim whose process died also frees up only after `ttl`.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Sets the byte cap for the buffered request body (413 past it) and the
    /// stored response body.
    pub fn with_max_body(mut self, max_bytes: usize) -> Self {
        self.max_body = max_bytes;
        self
    }

    /// Replaces the covered methods; requests with other methods pass through.
    pub fn with_methods(mut self, methods: impl IntoIterator<Item = Method>) -> Self {
        self.methods = methods.into_iter().collect();
        self
    }
}

/// Makes requests carrying an `Idempotency-Key` header safe to retry.
///
/// A request on a covered method is keyed on ([`RequestContext::principal`],
/// key, method, path) and fingerprinted by the SHA-256 of its body, which is
/// buffered up to [`Idempotency::with_max_body`]:
///
/// - first request: the key is claimed, the handler runs, and its status,
///   a safe header subset and body are stored and returned;
/// - same key and body after completion: the stored response is replayed
///   with `Idempotent-Replayed: true`, without running the handler;
/// - same key while the first request runs: 409 problem+json;
/// - same key, different body: 422 problem+json;
/// - malformed key (not 1 to 255 visible ASCII bytes): 400; body over the
///   cap: 413; store failure: 503, all problem+json.
///
/// A 5xx answer is not stored: the key is released and a retry runs the
/// handler again, since a server error usually means the work did not
/// happen. The same holds when the handler is dropped (client gone, panic).
/// If the response cannot be stored (body over the cap, store failure), the
/// claim stays in progress until the TTL rather than risk running the
/// handler twice; the first answer is then a 500 or, respectively, the
/// handler's own response.
///
/// Apply it inside [`crate::with_request_context`] (call this first), so the
/// principal is known. Requests without the header pass through untouched.
pub fn with_idempotency(router: Router, idempotency: Idempotency) -> Router {
    router.layer(middleware::from_fn_with_state(
        idempotency,
        idempotency_middleware,
    ))
}

async fn idempotency_middleware(
    State(config): State<Idempotency>,
    request: Request,
    next: Next,
) -> Response {
    let Some(header) = request.headers().get("idempotency-key") else {
        return next.run(request).await;
    };
    if !config.methods.contains(request.method()) {
        return next.run(request).await;
    }
    let Some(key) = header.to_str().ok().filter(|key| valid_key(key)) else {
        return problem(StatusCode::BAD_REQUEST, "invalid Idempotency-Key header");
    };
    let path = request
        .extensions()
        .get::<OriginalUri>()
        .map_or(request.uri(), |original| &original.0)
        .path()
        .to_owned();
    let key = IdempotencyKey {
        principal: request
            .extensions()
            .get::<RequestContext>()
            .and_then(RequestContext::principal)
            .unwrap_or_default()
            .to_owned(),
        key: key.to_owned(),
        method: request.method().to_string(),
        path,
    };
    let (parts, body) = request.into_parts();
    let Ok(body) = axum::body::to_bytes(body, config.max_body).await else {
        return problem(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
    };
    let fingerprint: [u8; 32] = Sha256::digest(&body).into();

    match config.store.claim(&key, fingerprint, config.ttl).await {
        Err(_) => problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "idempotency store unavailable",
        ),
        Ok(Claim::Existing(record)) if record.fingerprint != fingerprint => problem(
            StatusCode::UNPROCESSABLE_ENTITY,
            "Idempotency-Key was already used with a different request body",
        ),
        Ok(Claim::Existing(IdempotencyRecord { response: None, .. })) => problem(
            StatusCode::CONFLICT,
            "a request with this Idempotency-Key is still in progress",
        ),
        Ok(Claim::Existing(IdempotencyRecord {
            response: Some(stored),
            ..
        })) => replay(stored),
        Ok(Claim::Claimed) => {
            let mut claim = ClaimGuard(Some((Arc::clone(&config.store), key.clone())));
            let response = next.run(Request::from_parts(parts, Body::from(body))).await;
            if response.status().is_server_error() {
                claim.release().await;
                return response;
            }
            // The handler has run: from here on, never release the key.
            claim.keep();
            let (parts, body) = response.into_parts();
            let Ok(body) = axum::body::to_bytes(body, config.max_body).await else {
                return problem(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "response could not be stored for replay",
                );
            };
            let stored = StoredResponse {
                status: parts.status.as_u16(),
                headers: parts
                    .headers
                    .iter()
                    .filter(|(name, _)| SAFE_HEADERS.contains(&name.as_str()))
                    .map(|(name, value)| (name.as_str().to_owned(), value.as_bytes().to_vec()))
                    .collect(),
                body: body.to_vec(),
            };
            // ponytail: a failed write leaves the claim in progress; no
            // tracing dependency here to report it.
            let _ = config.store.complete(&key, &stored).await;
            Response::from_parts(parts, Body::from(body))
        }
    }
}

fn replay(stored: StoredResponse) -> Response {
    let mut response = Response::new(Body::from(stored.body));
    *response.status_mut() =
        StatusCode::from_u16(stored.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let headers = response.headers_mut();
    for (name, value) in stored.headers {
        if let (Ok(name), Ok(value)) = (HeaderName::try_from(name), HeaderValue::from_bytes(&value))
        {
            headers.append(name, value);
        }
    }
    headers.insert(IDEMPOTENT_REPLAYED, HeaderValue::from_static("true"));
    response
}

/// 1 to 255 visible ASCII bytes.
fn valid_key(key: &str) -> bool {
    (1..=255).contains(&key.len()) && key.bytes().all(|byte| byte.is_ascii_graphic())
}

/// Releases an in-progress claim unless completed or kept, including when the
/// middleware future is dropped mid-handler.
struct ClaimGuard(Option<(Arc<dyn IdempotencyStore>, IdempotencyKey)>);

impl ClaimGuard {
    async fn release(&mut self) {
        if let Some((store, key)) = self.0.take() {
            let _ = store.release(&key).await;
        }
    }

    fn keep(&mut self) {
        self.0 = None;
    }
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        if let (Some((store, key)), Ok(runtime)) =
            (self.0.take(), tokio::runtime::Handle::try_current())
        {
            runtime.spawn(async move {
                let _ = store.release(&key).await;
            });
        }
    }
}
