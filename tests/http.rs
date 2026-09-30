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

#[tokio::test]
async fn problem_json_carries_status_detail_and_field_errors_without_the_source() {
    use http_body_util::BodyExt;
    let response = rustclamp_http::HttpError::new(
        std::io::Error::other("password=secret"),
        StatusCode::UNPROCESSABLE_ENTITY,
        "validation failed",
    )
    .with_field_error("email", "must be an address")
    .with_field_error("email", "too long")
    .into_response();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        response.headers()["content-type"],
        "application/problem+json"
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], 422);
    assert_eq!(json["title"], "Unprocessable Entity");
    assert_eq!(json["detail"], "validation failed");
    assert_eq!(
        json["errors"]["email"],
        serde_json::json!(["must be an address", "too long"])
    );
    assert!(!String::from_utf8_lossy(&body).contains("secret"));
}

#[tokio::test]
async fn extractor_body_limit_lets_routing_and_auth_answer_before_413() {
    struct Reject;
    impl PrincipalResolver for Reject {
        fn resolve(&self, _: &HeaderMap) -> Result<Option<String>, String> {
            Err("bad credential".into())
        }
    }
    let send = |router: Router, uri: &'static str| async move {
        router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-length", "4")
                    .body(Body::from("four"))
                    .unwrap(),
            )
            .await
            .unwrap()
            .status()
    };
    let app = || {
        Router::new().route(
            "/",
            post(|body: axum::body::Bytes| async move { body.len().to_string() }),
        )
    };
    let open = rustclamp_http::with_extractor_body_limit(app(), 3);
    assert_eq!(send(open.clone(), "/").await, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(send(open, "/missing").await, StatusCode::NOT_FOUND);
    let guarded = with_request_context(app(), Arc::new(Reject), Duration::from_secs(1));
    let guarded = rustclamp_http::with_extractor_body_limit(guarded, 3);
    assert_eq!(send(guarded, "/").await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn any_future_is_a_shutdown_signal() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let served = rustclamp_http::serve(listener, Router::new(), async {}, Duration::from_secs(5));
    tokio::time::timeout(Duration::from_secs(2), served)
        .await
        .expect("serve did not stop")
        .unwrap();
}

#[cfg(feature = "ws")]
#[tokio::test]
async fn shutdown_sends_a_close_frame_to_open_websockets_and_ends_the_drain() {
    use futures_util::StreamExt;
    use rustclamp_http::{ShutdownReceiver, ws_route};
    use tokio_tungstenite::tungstenite::Message;

    let (stop, watch) = tokio::sync::watch::channel(false);
    // The session never ends on its own; only shutdown can close it.
    let route = ws_route(watch.clone(), |_socket| Box::pin(std::future::pending()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(rustclamp_http::serve(
        listener,
        Router::new().route("/ws", route),
        ShutdownReceiver(watch),
        Duration::from_secs(5),
    ));
    let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    stop.send(true).unwrap();
    let next = tokio::time::timeout(Duration::from_secs(2), client.next())
        .await
        .expect("no frame after shutdown");
    match next {
        Some(Ok(Message::Close(Some(frame)))) => assert_eq!(u16::from(frame.code), 1001),
        other => panic!("expected close frame, got {other:?}"),
    }
    drop(client);
    // Drain finished well inside the 5s timeout.
    server.await.unwrap().unwrap();
}

struct Roles;
impl PrincipalResolver for Roles {
    fn resolve(&self, headers: &HeaderMap) -> Result<Option<String>, String> {
        Ok(headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned))
    }
    fn roles(&self, principal: &str) -> Vec<String> {
        if principal == "admin" {
            vec!["admin".into()]
        } else {
            Vec::new()
        }
    }
}

async fn send(
    router: &Router,
    method: &str,
    auth: Option<&str>,
    id: Option<&str>,
) -> axum::response::Response {
    let mut builder = Request::builder().method(method).uri("/");
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth);
    }
    if let Some(id) = id {
        builder = builder.header("x-request-id", id);
    }
    router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn unauthorized_and_forbidden_are_problem_json_and_request_id_is_echoed() {
    let router = with_request_context(
        rustclamp_http::require_role(Router::new().route("/", get(|| async { "ok" })), "admin"),
        Arc::new(Roles),
        Duration::from_secs(1),
    );
    let denied = send(&router, "GET", None, Some("abc-123")).await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(denied.headers()["content-type"], "application/problem+json");
    assert_eq!(denied.headers()["x-request-id"], "abc-123");
    let forbidden = send(&router, "GET", Some("user"), None).await;
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        forbidden.headers()["content-type"],
        "application/problem+json"
    );
    let generated = forbidden.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(generated.starts_with("request-"));
    let ok = send(&router, "GET", Some("admin"), Some("has space")).await;
    assert_eq!(ok.status(), StatusCode::OK);
    assert!(
        ok.headers()["x-request-id"]
            .to_str()
            .unwrap()
            .starts_with("request-")
    );
}

#[tokio::test]
async fn head_and_unmatched_methods_follow_axum_routing() {
    let router = Router::new().route("/", get(|| async { "ok" }));
    let head = send(&router, "HEAD", None, None).await;
    assert_eq!(head.status(), StatusCode::OK);
    let post = send(&router, "POST", None, None).await;
    assert_eq!(post.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(post.headers()["allow"], "GET,HEAD");
}

#[cfg(feature = "rate-limit")]
#[tokio::test]
async fn rate_limit_answers_429_problem_json_with_retry_after() {
    let router = rustclamp_http::with_rate_limit(
        Router::new().route("/", get(|| async { "ok" })),
        2,
        Duration::from_secs(60),
        |request| {
            Some(
                request
                    .headers()
                    .get("authorization")?
                    .to_str()
                    .ok()?
                    .to_owned(),
            )
        },
    );
    for _ in 0..2 {
        assert_eq!(
            send(&router, "GET", Some("a"), None).await.status(),
            StatusCode::OK
        );
    }
    let limited = send(&router, "GET", Some("a"), None).await;
    assert_eq!(limited.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        limited.headers()["content-type"],
        "application/problem+json"
    );
    assert!(
        limited.headers()["retry-after"]
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            <= 60
    );
    assert_eq!(
        send(&router, "GET", Some("b"), None).await.status(),
        StatusCode::OK
    );
}

#[cfg(all(feature = "cors", feature = "access-log"))]
#[tokio::test]
async fn cors_and_access_log_layers_wrap_the_router() {
    let router = rustclamp_http::with_access_log(rustclamp_http::with_cors(
        Router::new().route("/", get(|| async { "ok" })),
        rustclamp_http::tower_http::cors::CorsLayer::permissive(),
    ));
    let response = router
        .oneshot(
            Request::builder()
                .uri("/")
                .header("origin", "http://x.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
}
