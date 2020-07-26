#[derive(Debug, PartialEq, Eq)]
pub(crate) enum S3Path<'a> {
    Root,
    Bucket { bucket: &'a str },
    Object { bucket: &'a str, key: &'a str },
}

impl<'a> S3Path<'a> {
    pub fn new(path: &'a str) -> Self {
        assert!(path.starts_with('/'));

        let mut iter = path.split('/');
        iter.next().unwrap();

        let bucket = match iter.next().unwrap() {
            "" => return S3Path::Root,
            s @ _ => s,
        };

        let key = match iter.next() {
            None => return S3Path::Bucket { bucket },
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
