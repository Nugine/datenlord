use std::cmp::{Eq, PartialEq};

#[derive(Debug)]
pub(super) enum S3Path<'a> {
    Root,
    Bucket { bucket: &'a str },
    Object { bucket: &'a str, key: &'a str },
}

impl PartialEq for S3Path<'_> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (S3Path::Root, S3Path::Root) => true,
            (S3Path::Bucket { bucket: lhs }, S3Path::Bucket { bucket: rhs }) => lhs == rhs,
            (
                S3Path::Object {
                    bucket: lhs_bucket,
                    key: lhs_key,
                },
                S3Path::Object {
                    bucket: rhs_bucket,
                    key: rhs_key,
                },
            ) => lhs_bucket == rhs_bucket && lhs_key == rhs_key,
            _ => false,
        }
    }
}

impl Eq for S3Path<'_> {}

impl<'a> S3Path<'a> {
    pub(super) fn new(path: &'a str) -> Self {
        assert!(path.starts_with('/'));

        let mut iter = path.split('/');
        let _ = iter.next().unwrap();

        let bucket = match iter.next().unwrap() {
            "" => return S3Path::Root,
            s => s,
        };

        let key = match iter.next() {
            None | Some("") => return S3Path::Bucket { bucket },
            Some(_) => &path[bucket.len() + 2..],
        };

        Self::Object { bucket, key }
    }
}

#[test]
fn test_s3_path() {
    assert_eq!(S3Path::new("/"), S3Path::Root);
    assert_eq!(S3Path::new("/bucket"), S3Path::Bucket { bucket: "bucket" });
    assert_eq!(
        S3Path::new("/bucket/dir/object"),
        S3Path::Object {
            bucket: "bucket",
            key: "dir/object"
        }
    )
}
