use bytes::Bytes;
use futures::stream::Stream;
use pin_project_lite::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncRead;

pin_project! {
    pub struct ByteStream<R>{
        #[pin]
        reader: R,
        buf_size: usize,
    }
}

impl<R> ByteStream<R> {
    pub fn new(reader: R, buf_size: usize) -> Self {
        Self { reader, buf_size }
    }
}

impl<R: AsyncRead> Stream for ByteStream<R> {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut buf = vec![0u8; self.buf_size];

        let this = self.project();

        let ret = futures::ready!(this.reader.poll_read(cx, &mut buf));
        let ans = match ret {
            Err(e) => Some(Err(e)),
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some(Ok(buf.into()))
            }
        };

        Poll::Ready(ans)
    }
}
