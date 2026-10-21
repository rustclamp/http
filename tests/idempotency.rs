//! `Idempotency-Key` behaviour against the in-memory store.
#![cfg(feature = "idempotency")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use http_body_util::BodyExt;
use rustclamp_http::{
    Idempotency, IdempotencyStore, MemoryIdempotencyStore, PrincipalResolver, with_idempotency,
    with_request_context,
};
use tokio::sync::Notify;
use tower::ServiceExt;

struct HeaderPrincipal;
impl PrincipalResolver for HeaderPrincipal {
    fn resolve(&self, headers: &HeaderMap) -> Result<Option<String>, String> {
        Ok(headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned))
    }
}

/// `POST /orders` counting handler runs; answers 201 with a run-numbered body.
fn app(runs: Arc<AtomicUsize>, config: Idempotency) -> Router {
    let orders = Router::new().route(
        "/orders",
        post(move |body: Bytes| {
            let runs = Arc::clone(&runs);
            async move {
                let run = runs.fetch_add(1, Ordering::SeqCst) + 1;
                (
                    StatusCode::CREATED,
                    [("location", "/orders/1"), ("set-cookie", "s=1")],
                    format!("run {run}: {}", String::from_utf8_lossy(&body)),
                )
            }
        }),
    );
    with_request_context(
        with_idempotency(orders, config),
        Arc::new(HeaderPrincipal),
        Duration::from_secs(5),
    )
}

fn memory() -> Idempotency {
    Idempotency::new(Arc::new(MemoryIdempotencyStore::new()))
}

fn order(key: &str, body: &'static str) -> Request<Body> {
    Request::post("/orders")
        .header("authorization", "alice")
        .header("idempotency-key", key)
        .body(Body::from(body))
        .unwrap()
}

async fn send(router: &Router, request: Request<Body>) -> (StatusCode, HeaderMap, String) {
    let response = router.clone().oneshot(request).await.unwrap();
    let (parts, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    (
        parts.status,
        parts.headers,
        String::from_utf8(body.to_vec()).unwrap(),
    )
}

#[tokio::test]
async fn first_request_runs_and_a_retry_replays_the_stored_response() {
    let runs = Arc::new(AtomicUsize::new(0));
    let router = app(Arc::clone(&runs), memory());
    let (status, headers, body) = send(&router, order("k1", "pay 10")).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body, "run 1: pay 10");
    assert!(headers.get("idempotent-replayed").is_none());

    let (status, headers, body) = send(&router, order("k1", "pay 10")).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body, "run 1: pay 10");
    assert_eq!(headers["idempotent-replayed"], "true");
    assert_eq!(headers["location"], "/orders/1");
    assert!(
        headers.get("set-cookie").is_none(),
        "unsafe headers are not stored"
    );
    assert!(headers.get("x-request-id").is_some());
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_retry_while_the_first_request_runs_answers_409() {
    let started = Arc::new(Notify::new());
    let finish = Arc::new(Notify::new());
    let (s, f) = (Arc::clone(&started), Arc::clone(&finish));
    let slow = Router::new().route(
        "/orders",
        post(move || async move {
            s.notify_one();
            f.notified().await;
            "done"
        }),
    );
    let router = with_idempotency(slow, memory());
    let first = tokio::spawn(router.clone().oneshot(order("k1", "a")));
    started.notified().await;

    let (status, headers, body) = send(&router, order("k1", "a")).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(headers["content-type"], "application/problem+json");
    assert!(body.contains("\"status\":409"));

    finish.notify_one();
    assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
}

#[tokio::test]
async fn the_same_key_with_a_different_body_answers_422() {
    let runs = Arc::new(AtomicUsize::new(0));
    let router = app(Arc::clone(&runs), memory());
    send(&router, order("k1", "pay 10")).await;
    let (status, headers, body) = send(&router, order("k1", "pay 99")).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(headers["content-type"], "application/problem+json");
    assert!(body.contains("different request body"));
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn server_errors_release_the_key_so_a_retry_runs_again() {
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);
    let flaky = Router::new().route(
        "/orders",
        post(move || {
            let counter = Arc::clone(&counter);
            async move {
                match counter.fetch_add(1, Ordering::SeqCst) {
                    0 => StatusCode::SERVICE_UNAVAILABLE.into_response(),
                    _ => "ok".into_response(),
                }
            }
        }),
    );
    let router = with_idempotency(flaky, memory());
    let (status, ..) = send(&router, order("k1", "a")).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let (status, headers, _) = send(&router, order("k1", "a")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(headers.get("idempotent-replayed").is_none());
    let (_, headers, _) = send(&router, order("k1", "a")).await;
    assert_eq!(headers["idempotent-replayed"], "true");
    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn an_expired_key_runs_the_handler_again() {
    let runs = Arc::new(AtomicUsize::new(0));
    let router = app(
        Arc::clone(&runs),
        memory().with_ttl(Duration::from_millis(50)),
    );
    send(&router, order("k1", "a")).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let (_, headers, body) = send(&router, order("k1", "a")).await;
    assert!(headers.get("idempotent-replayed").is_none());
    assert_eq!(body, "run 2: a");
}

#[tokio::test]
async fn concurrent_identical_requests_run_the_handler_once() {
    let runs = Arc::new(AtomicUsize::new(0));
    let router = app(Arc::clone(&runs), memory());
    let requests = (0..8).map(|_| {
        let router = router.clone();
        tokio::spawn(async move { send(&router, order("k1", "a")).await.0 })
    });
    for request in requests {
        let status = request.await.unwrap();
        assert!(matches!(status, StatusCode::CREATED | StatusCode::CONFLICT));
    }
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn keys_are_scoped_per_principal_and_path() {
    let runs = Arc::new(AtomicUsize::new(0));
    let router = app(Arc::clone(&runs), memory());
    send(&router, order("k1", "a")).await;
    let bob = Request::post("/orders")
        .header("authorization", "bob")
        .header("idempotency-key", "k1")
        .body(Body::from("a"))
        .unwrap();
    let (_, headers, body) = send(&router, bob).await;
    assert!(headers.get("idempotent-replayed").is_none());
    assert_eq!(body, "run 2: a");
}

#[tokio::test]
async fn safe_methods_missing_keys_bad_keys_and_big_bodies() {
    let runs = Arc::new(AtomicUsize::new(0));
    let router = app(Arc::clone(&runs), memory().with_max_body(8));
    // No header: every request runs.
    let plain = || {
        Request::post("/orders")
            .header("authorization", "alice")
            .body(Body::from("a"))
            .unwrap()
    };
    send(&router, plain()).await;
    send(&router, plain()).await;
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    let (status, ..) = send(&router, order("bad key", "a")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, headers, _) = send(&router, order("k1", "more than eight bytes")).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(headers["content-type"], "application/problem+json");
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    // Methods outside the covered set pass through.
    let only_put = app(
        Arc::clone(&runs),
        memory().with_methods([axum::http::Method::PUT]),
    );
    send(&only_put, order("k2", "a")).await;
    send(&only_put, order("k2", "a")).await;
    assert_eq!(runs.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn a_dropped_handler_releases_the_key() {
    let store = Arc::new(MemoryIdempotencyStore::new());
    let started = Arc::new(Notify::new());
    let s = Arc::clone(&started);
    let hanging = Router::new().route(
        "/orders",
        post(move || async move {
            s.notify_one();
            std::future::pending::<()>().await;
        }),
    );
    let router = with_idempotency(hanging, Idempotency::new(store.clone()));
    let first = tokio::spawn(router.oneshot(order("k1", "a")));
    started.notified().await;
    first.abort();
    let _ = first.await;
    tokio::task::yield_now().await;

    let key = rustclamp_http::IdempotencyKey {
        principal: String::new(),
        key: "k1".into(),
        method: "POST".into(),
        path: "/orders".into(),
    };
    let claim = store
        .claim(&key, [0; 32], Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(claim, rustclamp_http::Claim::Claimed);
}
