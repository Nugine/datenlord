//! async-fuse IO proactor

#![allow(
    clippy::todo,
    dead_code,
    unused_imports,
    clippy::restriction,
    box_pointers
)] // TODO: remove this

mod global;
mod storage;
pub mod v0;
