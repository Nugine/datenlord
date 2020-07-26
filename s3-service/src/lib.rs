#![forbid(unsafe_code)]
#![deny(
    // The following are allowed by default lints according to
    // https://doc.rust-lang.org/rustc/lints/listing/allowed-by-default.html
    anonymous_parameters,
    bare_trait_objects,
    // box_pointers,
    elided_lifetimes_in_paths,
    missing_copy_implementations,
    missing_debug_implementations,
    // missing_docs, // TODO: add documents
    single_use_lifetimes,
    trivial_casts,
    trivial_numeric_casts,
    unreachable_pub,
    unstable_features,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    unused_results,
    variant_size_differences,

    // Deny all Clippy lints even Clippy allow some by default
    // https://rust-lang.github.io/rust-clippy/master/
    clippy::all,
    clippy::restriction,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo,
)]
#![allow(
    // Some explicitly allowed Clippy lints, must have clear reason to allow
    clippy::implicit_return, // actually omitting the return keyword is idiomatic Rust code
    clippy::missing_inline_in_public_items, // In general, it is not bad
    clippy::must_use_candidate, // It's not bad at all

    clippy::wildcard_imports, // rusoto_s3 types is so many that they can't be imported by hand
    
    clippy::missing_docs_in_private_items, // TODO: deny it
    clippy::missing_errors_doc, // TODO: deny it
    clippy::match_same_arms, // TODO: deny it
    clippy::todo, // TODO: deny it
    
    clippy::expect_used, // TODO: should we deny it?
    clippy::panic, // TODO: should we deny it?
)]

pub mod compat;

pub mod storage;
pub use self::storage::S3Storage;

mod service;
pub use self::service::S3Service;

pub(crate) mod typedef {
    pub(super) type Request = hyper::Request<hyper::Body>;
    pub(super) type Response = hyper::Response<hyper::Body>;
}
