//! A tonic service and an Axum route served on one listener.
#![cfg(feature = "grpc")]

use std::convert::Infallible;
use std::time::Duration;

use axum::routing::get;
use rustclamp_core::{ContributionTarget, ModuleId};
use rustclamp_http::{HttpRoute, HttpRoutes, Public};
use tonic::codegen::{BoxFuture, Context, Poll, Service, http};
use tonic::server::{Grpc, NamedService, UnaryService};
use tonic::{Code, Request, Response, Status};
use tonic_prost::ProstCodec;

const APP: ModuleId = ModuleId::new("test.grpc.app");

#[derive(Clone, PartialEq, prost::Message)]
struct Text {
    #[prost(string, tag = "1")]
    value: String,
}

/// Hand-written stand-in for what `tonic-prost-build` generates: no protoc.
#[derive(Clone)]
struct EchoServer;

impl NamedService for EchoServer {
    const NAME: &'static str = "test.Echo";
}

struct Say;

impl UnaryService<Text> for Say {
    type Response = Text;
    type Future = BoxFuture<Response<Text>, Status>;
    fn call(&mut self, request: Request<Text>) -> Self::Future {
        let value = format!("echo {}", request.into_inner().value);
        Box::pin(async move { Ok(Response::new(Text { value })) })
    }
}

impl<B> Service<http::Request<B>> for EchoServer
where
    B: tonic::codegen::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        match request.uri().path() {
            "/test.Echo/Say" => Box::pin(async move {
                let mut grpc = Grpc::new(ProstCodec::<Text, Text>::default());
                Ok(grpc.unary(Say, request).await)
            }),
            _ => Box::pin(async { Ok(Status::unimplemented("").into_http()) }),
        }
    }
}

async fn call(
    channel: tonic::transport::Channel,
    path: &'static str,
) -> Result<Response<Text>, Status> {
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.unwrap();
    let request = Request::new(Text { value: "hi".into() });
    client
        .unary(
            request,
            http::uri::PathAndQuery::from_static(path),
            ProstCodec::<Text, Text>::default(),
        )
        .await
}

#[tokio::test]
async fn grpc_service_and_axum_route_share_one_listener() {
    let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
        Ok(listener) => listener,
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            eprintln!("local socket test skipped by sandbox policy");
            return;
        }
        Err(error) => panic!("bind listener: {error}"),
    };
    let address = listener.local_addr().unwrap();
    let routes = vec![
        (
            APP,
            HttpRoute::<Public>::new("/health", get(|| async { "ok" })),
        ),
        (APP, HttpRoute::<Public>::grpc(EchoServer)),
    ];
    let router = HttpRoutes::<Public>::new().build(&routes).unwrap();
    let (sender, receiver) = tokio::sync::watch::channel(false);
    let serving = tokio::spawn(rustclamp_http::serve(
        listener,
        router,
        rustclamp_http::ShutdownReceiver(receiver),
        Duration::from_secs(1),
    ));

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let reply = call(channel.clone(), "/test.Echo/Say").await.unwrap();
    assert_eq!(reply.into_inner().value, "echo hi");
    let unknown = call(channel.clone(), "/test.Echo/Missing")
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::Unimplemented);
    // No contributed service: Axum's 404, which gRPC clients read as Unimplemented.
    let unknown = call(channel.clone(), "/test.Missing/Say")
        .await
        .unwrap_err();
    assert_eq!(unknown.code(), Code::Unimplemented);
    drop(channel);

    // The plain HTTP/1.1 route answers on the same port.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    let response = String::from_utf8_lossy(&response);
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.ends_with("ok"));

    sender.send(true).unwrap();
    serving.await.unwrap().unwrap();
}

#[test]
fn one_service_contributed_twice_fails_the_build() {
    let routes = vec![
        (APP, HttpRoute::<Public>::grpc(EchoServer)),
        (
            ModuleId::new("test.grpc.other"),
            HttpRoute::<Public>::grpc(EchoServer),
        ),
    ];
    assert!(HttpRoutes::<Public>::new().build(&routes).is_err());
}
