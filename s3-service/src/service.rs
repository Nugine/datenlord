use futures::future::BoxFuture;
use hyper::Body;
use rusoto_s3::S3;
use std::sync::Arc;
use std::task::{Context, Poll};

type Request = hyper::Request<hyper::Body>;
type Response = hyper::Response<hyper::Body>;

#[derive(Debug, Clone)]
pub struct S3Service<T> {
    inner: Arc<T>,
}

impl<T> S3Service<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<T> AsRef<T> for S3Service<T> {
    fn as_ref(&self) -> &T {
        &*self.inner
    }
}

impl<T: S3> S3Service<T> {
    fn hyper_call(&mut self, req: Request) -> BoxFuture<'static, Result<Response, anyhow::Error>> {
        let _inner = Arc::clone(&self.inner);

        Box::pin(async move {
            dbg!(req);
            Ok(Response::new(Body::from("S3Service hyper call\n"))) // TODO: impl
        })
    }
}

impl<T: S3> hyper::service::Service<Request> for S3Service<T> {
    type Response = Response;
    type Error = anyhow::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // TODO: back pressue
    }
    fn call(&mut self, req: Request) -> Self::Future {
        self.hyper_call(req)
    }
}
