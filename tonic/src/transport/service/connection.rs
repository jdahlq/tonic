use super::{grpc_timeout::GrpcTimeout, reconnect::Reconnect, AddOrigin, SharedExec, UserAgent};
use crate::{
    body::BoxBody,
    transport::{BoxFuture, Endpoint},
    Status,
};
use http::Uri;
use hyper::{
    client::{
        conn::http2::{Builder, SendRequest},
        connect::Connection as HyperConnection,
    },
    rt::Executor,
};
use std::{
    fmt,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tower::{
    layer::Layer,
    limit::{concurrency::ConcurrencyLimitLayer, rate::RateLimitLayer},
    load::Load,
    util::BoxService,
    ServiceBuilder, ServiceExt,
};
use tower_service::Service;

pub(crate) type Request = http::Request<BoxBody>;
pub(crate) type Response = http::Response<hyper::Body>;

pub(crate) struct Connection {
    inner: BoxService<Request, Response, crate::Error>,
}

impl Connection {
    fn new<C>(connector: C, endpoint: Endpoint, is_lazy: bool) -> Self
    where
        C: Service<Uri> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Unpin + Send,
        C::Response: AsyncRead + AsyncWrite + HyperConnection + Unpin + Send + 'static,
    {
        let mut settings = Builder::new(endpoint.executor.clone())
            .initial_stream_window_size(endpoint.init_stream_window_size)
            .initial_connection_window_size(endpoint.init_connection_window_size)
            .keep_alive_interval(endpoint.http2_keep_alive_interval)
            .clone();

        if let Some(val) = endpoint.http2_keep_alive_timeout {
            settings.keep_alive_timeout(val);
        }

        if let Some(val) = endpoint.http2_keep_alive_while_idle {
            settings.keep_alive_while_idle(val);
        }

        if let Some(val) = endpoint.http2_adaptive_window {
            settings.adaptive_window(val);
        }

        let stack = ServiceBuilder::new()
            .layer_fn(|s| {
                let origin = endpoint.origin.as_ref().unwrap_or(&endpoint.uri).clone();

                AddOrigin::new(s, origin)
            })
            .layer_fn(|s| UserAgent::new(s, endpoint.user_agent.clone()))
            .layer_fn(|s| GrpcTimeout::new(s, endpoint.timeout))
            .option_layer(endpoint.concurrency_limit.map(ConcurrencyLimitLayer::new))
            .option_layer(endpoint.rate_limit.map(|(l, d)| RateLimitLayer::new(l, d)))
            .into_inner();

        let connector = MakeSendRequestService::new(connector, endpoint.executor.clone(), settings);
        let conn = Reconnect::new(connector, endpoint.uri.clone(), is_lazy);

        let inner = stack.layer(conn);

        Self {
            inner: BoxService::new(inner),
        }
    }

    pub(crate) async fn connect<C>(connector: C, endpoint: Endpoint) -> Result<Self, crate::Error>
    where
        C: Service<Uri> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Unpin + Send,
        C::Response: AsyncRead + AsyncWrite + HyperConnection + Unpin + Send + 'static,
    {
        Self::new(connector, endpoint, false).ready_oneshot().await
    }

    pub(crate) fn lazy<C>(connector: C, endpoint: Endpoint) -> Self
    where
        C: Service<Uri> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Unpin + Send,
        C::Response: AsyncRead + AsyncWrite + HyperConnection + Unpin + Send + 'static,
    {
        Self::new(connector, endpoint, true)
    }
}

impl Service<Request> for Connection {
    type Response = Response;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.inner, cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        self.inner.call(req)
    }
}

impl Load for Connection {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        0
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

// We must use a wrapper here since we can't implement tower::Service for SendRequest directly.
struct WrappedSendRequest {
    inner: SendRequest<BoxBody>,
}

impl From<SendRequest<BoxBody>> for WrappedSendRequest {
    fn from(inner: SendRequest<BoxBody>) -> Self {
        Self { inner }
    }
}

impl Service<http::Request<BoxBody>> for WrappedSendRequest {
    type Response = http::Response<hyper::Body>;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let fut = self.inner.send_request(req);

        Box::pin(async move { fut.await.map_err(Into::into) })
    }
}

struct MakeSendRequestService<C> {
    connector: C,
    executor: SharedExec,
    settings: Builder,
}

impl<C> MakeSendRequestService<C> {
    fn new(connector: C, executor: SharedExec, settings: Builder) -> Self {
        Self {
            connector,
            executor,
            settings,
        }
    }
}

impl<C> Service<Uri> for MakeSendRequestService<C>
where
    C: Service<Uri> + Send + 'static,
    C::Error: Into<crate::Error> + Send,
    C::Future: Unpin + Send,
    C::Response: AsyncRead + AsyncWrite + HyperConnection + Unpin + Send + 'static,
{
    type Response = WrappedSendRequest;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connector.poll_ready(cx).map_err(|e| {
            let mut status = Status::unavailable("Connection error");
            status.set_source(e.into().into());
            status.into()
        })
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let fut = self.connector.call(req);
        let builder = self.settings.clone();
        let executor = self.executor.clone();

        Box::pin(async move {
            let io = fut.await.map_err(|e| {
                let mut status = Status::unavailable("Connection error");
                status.set_source(e.into().into());
                status
            })?;
            let (send_request, conn) = builder.handshake(io).await?;

            Executor::execute(
                &executor,
                Box::pin(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!("connection error: {:?}", e);
                    }
                }),
            );

            Ok(WrappedSendRequest::from(send_request))
        })
    }
}
