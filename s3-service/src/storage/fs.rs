use super::stream;
use super::S3Storage;

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use path_absolutize::Absolutize;
use rusoto_s3::*;
use std::convert::TryInto;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::stream::StreamExt as _;

#[derive(Debug)]
pub struct FileSystem {
    root: PathBuf,
}

impl FileSystem {
    pub fn new(root: impl AsRef<Path>) -> Result<Self> {
        let root = std::env::current_dir()?.join(root).canonicalize()?;
        Ok(Self { root })
    }

    fn get_object_path(&self, bucket: &str, key: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        let file_path = Path::new(&key);
        let ans = dir
            .join(&file_path)
            .absolutize_virtually(&self.root)?
            .into();
        Ok(ans)
    }

    fn get_bucket_path(&self, bucket: &str) -> Result<PathBuf> {
        let dir = Path::new(&bucket);
        let ans = dir.absolutize_virtually(&self.root)?.into();
        Ok(ans)
    }
}

#[async_trait]
impl S3Storage for FileSystem {
    async fn get_object(&self, input: GetObjectRequest) -> Result<GetObjectOutput> {
        let path = self.get_object_path(&input.bucket, &input.key)?;

        let file = File::open(&path).await?;
        let content_length = file.metadata().await?.len();
        let stream = rusoto_core::ByteStream::new(stream::ByteStream::new(file, 4096));

        let output: GetObjectOutput = GetObjectOutput {
            body: Some(stream),
            content_length: Some(content_length.try_into()?),
            ..GetObjectOutput::default()
        };
        Ok(output)
    }
    async fn put_object(&self, input: PutObjectRequest) -> Result<PutObjectOutput> {
        let path = self.get_object_path(&input.bucket, &input.key)?;

        if let Some(body) = input.body {
            let mut reader = tokio::io::stream_reader(body);
            let file = File::create(&path).await?;
            let mut writer = tokio::io::BufWriter::new(file);
            let _ = tokio::io::copy(&mut reader, &mut writer).await?;
        }

        let output = PutObjectOutput::default();
        Ok(output)
    }
    async fn delete_object(&self, input: DeleteObjectRequest) -> Result<DeleteObjectOutput> {
        let path = self.get_object_path(&input.bucket, &input.key)?;

        tokio::fs::remove_file(path).await?;

        let output = DeleteObjectOutput::default();
        Ok(output)
    }
    async fn create_bucket(&self, input: CreateBucketRequest) -> Result<CreateBucketOutput> {
        let path = self.get_bucket_path(&input.bucket)?;

        tokio::fs::create_dir(&path)
            .await
            .with_context(|| format!("{:?}", path))?;
        let output = CreateBucketOutput::default();
        Ok(output)
    }
    async fn delete_bucket(&self, input: DeleteBucketRequest) -> Result<()> {
        let path = self.get_bucket_path(&input.bucket)?;
        tokio::fs::remove_dir_all(path).await?;
        Ok(())
    }
    async fn list_buckets(&self) -> Result<ListBucketsOutput> {
        let mut buckets = Vec::new();

        let mut iter = tokio::fs::read_dir(&self.root).await?;
        while let Some(entry) = iter.next().await {
            let entry = entry?;
            if entry.file_type().await?.is_dir() {
                let name: String = entry.file_name().to_string_lossy().into();
                buckets.push(Bucket {
                    creation_date: None,
                    name: Some(name),
                })
            }
        }
        let output = ListBucketsOutput {
            buckets: Some(buckets),
            owner: None,
        };
        Ok(output)
    }
    async fn get_bucket_location(
        &self,
        input: GetBucketLocationRequest,
    ) -> Result<GetBucketLocationOutput> {
        dbg!(input);
        let output = GetBucketLocationOutput::default();
        Ok(output)
    }
    async fn head_bucket(&self, input: HeadBucketRequest) -> Result<()> {
        let path = self.get_bucket_path(&input.bucket)?;
        if path.exists() {
            Ok(())
        } else {
            anyhow::bail!("TODO") // TODO
        }
    }
}
