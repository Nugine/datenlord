#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

pub mod compat;

pub mod storage;
pub use self::storage::S3Storage;

mod service;
pub use self::service::S3Service;

pub(crate) mod typedef {
    pub type Request = hyper::Request<hyper::Body>;
    pub type Response = hyper::Response<hyper::Body>;
}
