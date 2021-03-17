use super::global::{IoRequest, Operation};
use super::storage::InlineStorage;

use std::io;
use std::mem::MaybeUninit;
use std::os::unix::io::RawFd;

use ring_io::sqe::PrepareSqe;

mod tool {
    #[repr(transparent)]
    pub struct AssertSend<T>(T);
    unsafe impl<T> Send for AssertSend<T> {}
    pub unsafe fn assert_send<T>(x: T) -> AssertSend<T> {
        AssertSend(x)
    }
}

use self::tool::assert_send;

pub async fn read(
    fd: RawFd,
    buf: Vec<u8>,
    len: usize,
    offset: isize,
) -> (io::Result<usize>, RawFd, Vec<u8>) {
    struct Read {
        fd: RawFd,
        buf: Vec<u8>,
        len: usize,
        offset: isize,
    }

    impl Operation for Read {
        unsafe fn prepare(
            &mut self,
            storage: &mut InlineStorage,
            sqe: &mut MaybeUninit<ring_io::sqe::SQE>,
        ) {
            let iov_ptr = storage.put(assert_send([libc::iovec {
                iov_base: self.buf.as_mut_ptr().cast(),
                iov_len: self.len,
            }]));
            sqe.prep_readv(self.fd, iov_ptr.cast(), 1, self.offset);
        }
    }

    assert!(len <= buf.len());

    let io = IoRequest::new(Read {
        fd,
        buf,
        len,
        offset,
    });

    let (result, data) = io.await;

    (result.map(|nread| nread as usize), data.fd, data.buf)
}

pub async fn write(
    fd: RawFd,
    buf: Vec<u8>,
    len: usize,
    offset: isize,
) -> (io::Result<usize>, RawFd, Vec<u8>) {
    struct Write {
        fd: RawFd,
        buf: Vec<u8>,
        len: usize,
        offset: isize,
    }

    impl Operation for Write {
        unsafe fn prepare(
            &mut self,
            storage: &mut InlineStorage,
            sqe: &mut MaybeUninit<ring_io::sqe::SQE>,
        ) {
            let iov_ptr = storage.put(assert_send([libc::iovec {
                iov_base: self.buf.as_mut_ptr().cast(),
                iov_len: self.len,
            }]));
            sqe.prep_writev(self.fd, iov_ptr.cast(), 1, self.offset);
        }
    }

    assert!(len <= buf.len());

    let io = IoRequest::new(Write {
        fd,
        buf,
        len,
        offset,
    });

    let (result, data) = io.await;

    (result.map(|nread| nread as usize), data.fd, data.buf)
}

#[test]
fn proactor_v0_test() -> io::Result<()> {
    use std::ffi::OsStr;
    use std::fs;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::io::AsRawFd;

    fs::write("/tmp/hello", "world").unwrap();

    let buf = vec![0; 64];

    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open("/tmp/hello")
        .unwrap();

    let fd = file.as_raw_fd();

    let mut buf = smol::block_on(async move {
        let (ret, _, mut buf) = read(fd, buf, 64, 0).await;
        let nread = ret.unwrap();
        assert_eq!(&buf[..nread], b"world");
        buf.truncate(nread);
        buf
    });

    buf.reverse();

    smol::block_on(async move {
        let len = buf.len();
        let (ret, _, _) = write(fd, buf, len, 0).await;
        let nwritten = ret.unwrap();
        assert_eq!(nwritten, len);
    });

    assert_eq!(fs::read_to_string("/tmp/hello")?, "dlrow");
    Ok(())
}
