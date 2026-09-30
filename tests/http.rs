//! Route contribution, request context, and graceful server tests.
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use axum::body::Body;
use axum::extract::Extension;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::routing::{get, post};
use axum::{Router, response::IntoResponse};
use rustclamp_core::{ContributionTarget, ModuleId, Qualifier};
use rustclamp_http::{
    HttpRoute, HttpRoutes, PrincipalResolver, Public, Representation, negotiate,
    with_request_context,
};
use tower::ServiceExt;

struct Admin;
impl Qualifier for Admin {
    const ID: rustclamp_core::QualifierId = rustclamp_core::QualifierId::new("test.http.admin");
}

const FIRST: ModuleId = ModuleId::new("test.http.first");
const SECOND: ModuleId = ModuleId::new("test.http.second");

#[tokio::test]
async fn builds_and_serves_a_contributed_route_while_retaining_axum_types() {
    let routes = vec![(
        FIRST,
        HttpRoute::<Public>::new("/users", get(|| async { "users" })),
    )];
    let router = HttpRoutes::<Public>::new().build(&routes).unwrap();
    let response = router
        .oneshot(
            Request::builder()
                .uri("/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let admin = HttpRoute::<Admin>::new("/admin", get(|| async { "private" }));
    let admin_router = HttpRoutes::<Admin>::new().build(&[(FIRST, admin)]).unwrap();
    assert!(admin_router.has_routes());
}

#[tokio::test]
async fn validates_contributed_paths_and_keeps_methods_inside_axum_router() {
    let routes = vec![(
        FIRST,
        HttpRoute::<Public>::new("/users", get(|| async { "get" }).post(|| async { "post" })),
    )];
    let router = HttpRoutes::<Public>::new().build(&routes).unwrap();
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/users")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let duplicate_paths = vec![
        (
            FIRST,
            HttpRoute::<Public>::new("/users", get(|| async { "one" })),
        ),
        (
            SECOND,
            HttpRoute::<Public>::new("/users", post(|| async { "two" })),
        ),
    ];
    assert!(HttpRoutes::<Public>::new().build(&duplicate_paths).is_err());

    let duplicate = vec![
        (
            FIRST,
            HttpRoute::<Public>::new("/users/one", get(|| async { "one" })),
        ),
        (
            SECOND,
            HttpRoute::<Public>::new("/users/two", get(|| async { "two" })),
        ),
    ];
    assert!(HttpRoutes::<Public>::new().build(&duplicate).is_ok());

    let overlap = vec![
        (
            FIRST,
            HttpRoute::<Public>::new("/users/{id}", get(|| async { "one" })),
        ),
        (
            SECOND,
            HttpRoute::<Public>::new("/users/{user_id}", get(|| async { "two" })),
        ),
    ];
    assert!(HttpRoutes::<Public>::new().build(&overlap).is_err());
}

struct HeaderIdentity;
impl PrincipalResolver for HeaderIdentity {
    fn resolve(&self, headers: &HeaderMap) -> Result<Option<String>, String> {
        Ok(headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned))
    }
}

async fn show_context(
    Extension(context): Extension<rustclamp_http::RequestContext>,
    Extension(seen): Extension<Arc<std::sync::Mutex<Option<rustclamp_http::RequestCancellation>>>>,
) -> impl IntoResponse {
    assert_eq!(context.principal(), Some("Bearer subject"));
    assert_eq!(context.tenant(), Some("tenant-a"));
    assert_eq!(context.correlation_id(), "correlation-1");
    assert!(context.deadline() <= tokio::time::Instant::now() + Duration::from_secs(2));
    *seen.lock().unwrap() = Some(context.cancellation().clone());
    "ok"
}

#[tokio::test]
async fn resolves_context_before_the_operation_and_cancels_it_when_request_finishes() {
    let seen = Arc::new(std::sync::Mutex::new(
        None::<rustclamp_http::RequestCancellation>,
    ));
    let router = Router::new()
        .route("/", get(show_context))
        .layer(axum::Extension(seen.clone()));
    let router = with_request_context(router, Arc::new(HeaderIdentity), Duration::from_secs(2));
    let response = router
        .oneshot(
            Request::builder()
                .uri("/")
                .header("authorization", "Bearer subject")
                .header("x-tenant-id", "tenant-a")
                .header("x-correlation-id", "correlation-1")
                .header("x-timeout-ms", "999999")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(seen.lock().unwrap().as_ref().unwrap().is_cancelled());

    let ran = Arc::new(AtomicBool::new(false));
    let operation_ran = ran.clone();
    let rejected = Router::new().route(
        "/",
        get(move || async move {
            operation_ran.store(true, Ordering::Release);
            "called"
        }),
    );
    struct Reject;
    impl PrincipalResolver for Reject {
        fn resolve(&self, _: &HeaderMap) -> Result<Option<String>, String> {
            Err("bad credential".into())
        }
    }
    let rejected = with_request_context(rejected, Arc::new(Reject), Duration::from_secs(1));
    let response = rejected
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(!ran.load(Ordering::Acquire));
}

#[tokio::test]
async fn body_limit_rejects_oversized_input_before_handler_execution() {
    let called = Arc::new(AtomicBool::new(false));
    let handler_called = called.clone();
    let router = Router::new().route(
        "/",
        post(move |body: axum::body::Bytes| async move {
            handler_called.store(true, Ordering::Release);
            body.len().to_string()
        }),
    );
    let router = rustclamp_http::with_body_limit(router, 3);
    let response = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/")
                .body(Body::from("four"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(!called.load(Ordering::Acquire));
}

#[tokio::test]
async fn bounded_stream_forwards_chunks_and_disconnect_drops_the_producer() {
    use axum::body::Bytes;
    use http_body_util::BodyExt;
    use tokio::sync::mpsc;

    let (sender, receiver) = mpsc::channel::<Bytes>(1);
    let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let producer_sent = sent.clone();
    let producer_cancelled = cancelled.clone();
    let producer = tokio::spawn(async move {
        for _ in 0..10 {
            if sender.send(Bytes::from_static(b"chunk")).await.is_err() {
                producer_cancelled.store(true, Ordering::Release);
                return;
            }
            producer_sent.fetch_add(1, Ordering::Relaxed);
        }
    });
    let chunks = futures_util::stream::unfold(receiver, |mut receiver| async move {
        receiver
            .recv()
            .await
            .map(|chunk| (Ok::<_, std::io::Error>(chunk), receiver))
    });
    let mut body = rustclamp_http::streaming_body(chunks);
    let first = body.frame().await.unwrap().unwrap().into_data().unwrap();
    assert_eq!(&first[..], b"chunk");
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(
        sent.load(Ordering::Relaxed) <= 2,
        "one chunk may be in flight and one queued"
    );
    drop(body);
    tokio::time::timeout(Duration::from_secs(1), producer)
        .await
        .unwrap()
        .unwrap();
    assert!(cancelled.load(Ordering::Acquire));
}

#[tokio::test]
async fn public_error_body_does_not_disclose_typed_source() {
    use http_body_util::BodyExt;
    let error = rustclamp_http::HttpError::new(
        std::io::Error::other("password=secret"),
        StatusCode::INTERNAL_SERVER_ERROR,
        "request failed",
    );
    assert_eq!(error.source_error().to_string(), "password=secret");
    let response = error.into_response();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"request failed");
}

#[test]
fn content_negotiation_obeys_quality_and_rejects_unknown_representations() {
    assert_eq!(negotiate(None).unwrap(), Representation::Json);
    assert_eq!(
        negotiate(Some("text/html, application/json;q=0.8")).unwrap(),
        Representation::Html
    );
    assert_eq!(
        negotiate(Some("application/json;q=0, application/xml")).unwrap(),
        Representation::Xml
    );
    assert_eq!(
        negotiate(Some("application/pdf")).unwrap_err(),
        rustclamp_http::UnsupportedRepresentation
    );
}

#[tokio::test]
async fn caller_owned_listener_stops_accepting_after_shutdown_signal() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::watch;

    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("local socket test skipped by sandbox policy");
            return;
        }
        Err(error) => panic!("bind listener: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let router = Router::new().route("/", get(|| async { "ready" }));
    let (sender, receiver) = watch::channel(false);
    let serving = tokio::spawn(rustclamp_http::serve(
        listener,
        router,
        rustclamp_http::ShutdownReceiver(receiver),
        Duration::from_secs(1),
    ));
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response).contains("ready"));
    sender.send(true).unwrap();
    serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn graceful_shutdown_finishes_inflight_requests_and_reports_port_conflict() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::{Notify, watch};

    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("bind listener: {error}"),
    };
    let address = listener.local_addr().unwrap();
    assert_eq!(
        tokio::net::TcpListener::bind(address)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::AddrInUse
    );

    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler_entered = entered.clone();
    let handler_release = release.clone();
    let route = get(move || {
        let entered = handler_entered.clone();
        let release = handler_release.clone();
        async move {
            entered.notify_one();
            release.notified().await;
            "drained"
        }
    });
    let router = Router::new().route("/", route);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let server = tokio::spawn(rustclamp_http::serve(
        listener,
        router,
        rustclamp_http::ShutdownReceiver(shutdown_rx),
        Duration::from_secs(2),
    ));
    let mut client = TcpStream::connect(address).await.unwrap();
    client
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    entered.notified().await;
    shutdown_tx.send(true).unwrap();
    tokio::task::yield_now().await;
    assert!(
        !server.is_finished(),
        "graceful stop waits for accepted work"
    );
    release.notify_one();
    let mut response = Vec::new();
    client.read_to_end(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response).contains("drained"));
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn drain_timeout_counts_from_the_shutdown_signal_not_startup() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::watch;

    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("bind listener: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/", get(|| async { "ready" }))
        .route("/stuck", get(std::future::pending::<&'static str>));
    let (sender, receiver) = watch::channel(false);
    let server = tokio::spawn(rustclamp_http::serve(
        listener,
        router,
        rustclamp_http::ShutdownReceiver(receiver),
        Duration::from_millis(50),
    ));
    let get = |path: &'static str| async move {
        let mut stream = TcpStream::connect(address).await.unwrap();
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        stream
    };

    // Well past the drain timeout, the server still answers.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !server.is_finished(),
        "server stopped without a shutdown signal"
    );
    let mut response = Vec::new();
    get("/").await.read_to_end(&mut response).await.unwrap();
    assert!(String::from_utf8_lossy(&response).contains("ready"));

    // After the signal, a request that never finishes is cut off at the deadline.
    let _stuck = get("/stuck").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    sender.send(true).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("drain deadline enforced")
        .unwrap();
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
}
