#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]

pub mod compat;

mod service;
pub use service::S3Service;
