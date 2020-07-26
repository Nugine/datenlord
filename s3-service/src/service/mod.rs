mod s3_output;
mod s3_path;

use self::s3_output::S3Output;
use self::s3_path::S3Path;

use crate::storage::S3Storage;
use crate::typedef::{Request, Response};

use futures::future::BoxFuture;
use futures::stream::StreamExt as _;
use hyper::Method;
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

use rusoto_s3::*;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
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

impl<T> Clone for S3Service<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> AsRef<T> for S3Service<T> {
    fn as_ref(&self) -> &T {
        &*self.inner
    }
}

impl<T> hyper::service::Service<Request> for S3Service<T>
where
    T: S3Storage + Send + Sync + 'static,
{
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

impl<T> S3Service<T>
where
    T: S3Storage + Send + Sync + 'static,
{
    fn hyper_call(&mut self, req: Request) -> BoxFuture<'static, Result<Response, anyhow::Error>> {
        let service = self.clone();
        Box::pin(async move {
            let method = req.method().clone();
            let uri = req.uri().clone();
            log::debug!("{} \"{:?}\" request:\n{:#?}", method, uri, req);
            let result = service.handle(req).await;
            match &result {
                Ok(resp) => log::debug!("{} \"{:?}\" => response:\n{:#?}", method, uri, resp),
                Err(err) => log::debug!("{} \"{:?}\" => error:\n{:#?}", method, uri, err),
            }
            result
        })
    }

    async fn handle(&self, req: Request) -> anyhow::Result<Response> {
        match *req.method() {
            Method::GET => self.handle_get(req).await,
            Method::POST => self.handle_post(req).await,
            Method::PUT => self.handle_put(req).await,
            Method::DELETE => self.handle_delete(req).await,
            Method::HEAD => self.handle_head(req).await,
            _ => anyhow::bail!("NotSupported"),
        }
    }

    async fn handle_get(&self, req: Request) -> anyhow::Result<Response> {
        let s3path = S3Path::new(req.uri().path());

        match s3path {
            S3Path::Root => self.inner.list_buckets().await.try_into_response(),
            S3Path::Bucket { bucket } => {
                let input = GetBucketLocationRequest {
                    bucket: bucket.into(),
                };
                self.inner
                    .get_bucket_location(input)
                    .await
                    .try_into_response()
            }
            S3Path::Object { bucket, key } => {
                let input = GetObjectRequest {
                    bucket: bucket.into(),
                    key: key.into(),
                    ..GetObjectRequest::default()
                };
                self.inner.get_object(input).await.try_into_response()
            }
        }
    }

    async fn handle_post(&self, req: Request) -> anyhow::Result<Response> {
        dbg!(req);
        todo!()
    }

    async fn handle_put(&self, req: Request) -> anyhow::Result<Response> {
        let s3path = S3Path::new(req.uri().path());

        match s3path {
            S3Path::Root => anyhow::bail!("NotSupported"),
            S3Path::Bucket { bucket } => {
                let input: CreateBucketRequest = CreateBucketRequest {
                    bucket: bucket.into(),
                    ..CreateBucketRequest::default()
                };
                self.inner.create_bucket(input).await.try_into_response()
            }
            S3Path::Object { bucket, key } => {
                let bucket = bucket.into();
                let key = key.into();
                let body = req.into_body().map(|try_chunk| {
                    try_chunk.map(|c| c).map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            format!("Error obtaining chunk: {}", e),
                        )
                    })
                });

                let input: PutObjectRequest = PutObjectRequest {
                    bucket,
                    key,
                    body: Some(rusoto_core::ByteStream::new(body)),
                    ..PutObjectRequest::default()
                };

                self.inner.put_object(input).await.try_into_response()
            }
        }
    }

    async fn handle_delete(&self, req: Request) -> anyhow::Result<Response> {
        let s3path = S3Path::new(req.uri().path());

        match s3path {
            S3Path::Root => anyhow::bail!("NotSupported"),
            S3Path::Bucket { bucket } => {
                let input: DeleteBucketRequest = DeleteBucketRequest {
                    bucket: bucket.into(),
                };
                self.inner.delete_bucket(input).await.try_into_response()
            }
            S3Path::Object { bucket, key } => {
                let input: DeleteObjectRequest = DeleteObjectRequest {
                    bucket: bucket.into(),
                    key: key.into(),
                    ..DeleteObjectRequest::default()
                };

                self.inner.delete_object(input).await.try_into_response()
            }
        }
    }

    async fn handle_head(&self, req: Request) -> anyhow::Result<Response> {
        let s3path = S3Path::new(req.uri().path());
        match s3path {
            S3Path::Root => anyhow::bail!("NotSupported"),
            S3Path::Bucket { bucket } => {
                let input = HeadBucketRequest {
                    bucket: bucket.into(),
                };
                self.inner.head_bucket(input).await.try_into_response()
            }
            S3Path::Object { .. } => anyhow::bail!("NotSupported"),
        }
    }
}
