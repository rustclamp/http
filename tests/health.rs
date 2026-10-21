//! Liveness and readiness probes: lifecycle gating, checks, timeouts, auth bypass.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::routing::get;
use rustclamp_http::{Health, PrincipalResolver, require_role, with_health, with_request_context};
use tokio::sync::watch;
use tower::ServiceExt;

async fn call(router: &Router, uri: &str) -> (StatusCode, String, String) {
    let response = router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let kind = response
        .headers()
        .get("content-type")
        .map(|value| value.to_str().unwrap().to_owned())
        .unwrap_or_default();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, kind, String::from_utf8(bytes.to_vec()).unwrap())
}

fn app(health: Health) -> Router {
    with_health(Router::new().route("/", get(|| async { "home" })), health)
}

#[tokio::test]
async fn readiness_follows_the_lifecycle_and_liveness_always_answers() {
    // Before Ready, at Ready, during Drain: what AppRunner::readiness reports.
    let (lifecycle, readiness) = watch::channel(false);
    let router = app(Health::new(readiness));

    for ready in [false, true, false] {
        lifecycle.send_replace(ready);
        let (status, kind, body) = call(&router, "/livez").await;
        assert_eq!(
            (status, kind.as_str()),
            (StatusCode::OK, "application/json")
        );
        assert_eq!(body, r#"{"status":"ok"}"#);

        let (status, kind, body) = call(&router, "/readyz").await;
        if ready {
            assert_eq!(status, StatusCode::OK);
            assert_eq!(kind, "application/json");
            assert_eq!(body, r#"{"checks":{},"status":"ok"}"#);
        } else {
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(kind, "application/problem+json");
            let body: serde_json::Value = serde_json::from_str(&body).unwrap();
            assert_eq!(body["status"], 503);
            assert_eq!(body["detail"], "starting or shutting down");
        }
    }
}

#[tokio::test]
async fn a_failing_check_is_503_without_its_error() {
    let (_lifecycle, readiness) = watch::channel(true);
    let router = app(Health::new(readiness)
        .check("db", || async { Ok::<_, String>(()) })
        .check("cache", || async {
            Err::<(), _>("password=hunter2 at 10.0.0.5".to_owned())
        }));

    let (status, kind, body) = call(&router, "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(kind, "application/problem+json");
    assert!(
        !body.contains("hunter2") && !body.contains("10.0.0.5"),
        "{body}"
    );
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["checks"]["db"], "ok");
    assert_eq!(body["checks"]["cache"], "fail");
    assert_eq!(body["detail"], "readiness check failed");
}

#[tokio::test]
async fn a_slow_check_times_out_as_503_and_passing_checks_are_200() {
    let (_lifecycle, readiness) = watch::channel(true);
    let slow = app(Health::new(readiness.clone())
        .timeout(Duration::from_millis(20))
        .check("slow", || async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok::<_, ()>(())
        }));
    let started = std::time::Instant::now();
    let (status, _, body) = call(&slow, "/readyz").await;
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains(r#""slow":"timeout""#), "{body}");

    let fine = app(Health::new(readiness).check("db", || async { Ok::<_, ()>(()) }));
    let (status, _, body) = call(&fine, "/readyz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, r#"{"checks":{"db":"ok"},"status":"ok"}"#);
}

#[tokio::test]
async fn a_panicking_check_fails_the_probe() {
    let (_lifecycle, readiness) = watch::channel(true);
    let router = app(Health::new(readiness).check("broken", || async {
        panic!("boom");
        #[allow(unreachable_code)]
        Ok::<_, ()>(())
    }));
    let (status, _, body) = call(&router, "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains(r#""broken":"fail""#), "{body}");
}

#[tokio::test]
async fn paths_are_configurable() {
    let (_lifecycle, readiness) = watch::channel(true);
    let router = app(Health::new(readiness).paths("/health/live", "/health/ready"));
    assert_eq!(call(&router, "/health/live").await.0, StatusCode::OK);
    assert_eq!(call(&router, "/health/ready").await.0, StatusCode::OK);
    assert_eq!(call(&router, "/livez").await.0, StatusCode::NOT_FOUND);
}

/// Rejects every credential, and grants no roles.
struct Nobody;

impl PrincipalResolver for Nobody {
    fn resolve(&self, headers: &HeaderMap) -> Result<Option<String>, String> {
        match headers.get("authorization") {
            Some(_) => Err("bad credential".to_owned()),
            None => Ok(None),
        }
    }
}

#[tokio::test]
async fn probes_bypass_auth_applied_before_them() {
    let (_lifecycle, readiness) = watch::channel(true);
    let guarded = with_request_context(
        require_role(Router::new().route("/", get(|| async { "home" })), "admin"),
        Arc::new(Nobody),
        Duration::from_secs(1),
    );
    let router = with_health(guarded, Health::new(readiness));

    assert_eq!(call(&router, "/").await.0, StatusCode::UNAUTHORIZED);
    assert_eq!(call(&router, "/livez").await.0, StatusCode::OK);
    assert_eq!(call(&router, "/readyz").await.0, StatusCode::OK);
}
