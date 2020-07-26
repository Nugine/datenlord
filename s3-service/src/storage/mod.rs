mod fs;
pub use self::fs::FileSystem;

pub(self) mod stream;

use anyhow::Result;
use async_trait::async_trait;
use rusoto_s3::*;

#[async_trait]
pub trait S3Storage {
    async fn get_object(&self, input: GetObjectRequest) -> Result<GetObjectOutput>;
    async fn put_object(&self, input: PutObjectRequest) -> Result<PutObjectOutput>;
    async fn delete_object(&self, input: DeleteObjectRequest) -> Result<DeleteObjectOutput>;
    async fn create_bucket(&self, input: CreateBucketRequest) -> Result<CreateBucketOutput>;
    async fn delete_bucket(&self, input: DeleteBucketRequest) -> Result<()>;
    async fn list_buckets(&self) -> Result<ListBucketsOutput>;
}
