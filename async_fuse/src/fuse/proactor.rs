//! async-fuse IO proactor

#![allow(clippy::todo, dead_code, clippy::restriction, box_pointers)] // TODO: remove this

use crate::util::{nix_to_io_error, unblock};

use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::{io, mem, thread};

use aligned_utils::bytes::AlignedBytes;
use crossbeam_queue::ArrayQueue;
use event_listener::Event;
use futures::{SinkExt, StreamExt};
use futures_channel::mpsc;
use log::debug;
use mem::ManuallyDrop;
use once_cell::sync::Lazy;
use ring_io::cq::CompletionQueue;
use ring_io::cqe::CQE;
use ring_io::ring::RingBuilder;
use ring_io::sq::SubmissionQueue;
use ring_io::sqe::PrepareSqe;

pub struct Proactor {
    op_chan: mpsc::Sender<Box<Operation>>,
    object_pool: ObjectPool,
}

type ObjectPool = ArrayQueue<(
    mpsc::Sender<tool::AssertSend<*mut Operation>>,
    mpsc::Receiver<tool::AssertSend<*mut Operation>>,
    Box<Operation>,
)>;

enum Operation {
    Nop,
    Read {
        fd: RawFd,
        buf: AlignedBytes,
        len: usize,
        offset: isize,

        iovecs: [libc::iovec; 1],

        res: i32,

        tx: mpsc::Sender<tool::AssertSend<*mut Operation>>,
    },
    Write {
        fd: RawFd,
        buf: AlignedBytes,
        nbytes: usize,
        offset: isize,

        iovecs: [libc::iovec; 1],

        res: i32,

        tx: mpsc::Sender<tool::AssertSend<*mut Operation>>,
    },
}

unsafe impl Send for Operation {}

mod tool {
    pub struct AssertSend<T>(T);
    unsafe impl<T> Send for AssertSend<T> {}
    impl<T> AssertSend<T> {
        pub const unsafe fn new(t: T) -> Self {
            Self(t)
        }

        pub const fn get_ref(&self) -> &T {
            &self.0
        }

        #[allow(clippy::missing_const_for_fn)]
        pub fn into_inner(self) -> T {
            self.0
        }
    }
}

impl Proactor {
    const RING_ENTRIES: u32 = 32;

    /// Gets the global proactor
    pub fn global() -> &'static Self {
        static GLOBAL_PROACTOR: Lazy<Proactor> =
            Lazy::new(|| Proactor::start_driver().expect("failed to start global proactor driver"));
        &*GLOBAL_PROACTOR
    }

    /// Starts the proactor's driver thread
    fn start_driver() -> io::Result<Self> {
        let ring = RingBuilder::new(Self::RING_ENTRIES).build()?;
        let ring = Box::leak(Box::new(ring));
        let (mut sq, mut cq, _) = ring.split();

        let (tx, mut rx) = mpsc::channel(4);

        let complete_event = Arc::new(Event::new());
        let inflight_count = Arc::new(AtomicU32::new(0));

        {
            let complete_event = Arc::clone(&complete_event);
            let inflight_count = Arc::clone(&inflight_count);
            smol::Task::spawn(async move {
                Self::submitter(&mut sq, &mut rx, &*complete_event, &*inflight_count).await
            })
            .detach();
        }
        thread::spawn(move || Self::completer(&mut cq, &*complete_event, &*inflight_count));

        let object_pool = ArrayQueue::new(Self::RING_ENTRIES as usize);
        Ok(Self {
            op_chan: tx,
            object_pool,
        })
    }

    async fn submitter(
        sq: &mut SubmissionQueue<'static>,
        op_chan: &mut mpsc::Receiver<Box<Operation>>,
        complete_event: &Event,
        inflight_count: &AtomicU32,
    ) {
        loop {
            while inflight_count.load(Ordering::Acquire) >= Self::RING_ENTRIES {
                complete_event.listen().await;
            }

            let mut ops_cnt = 0;
            loop {
                let available_sqes = sq.space_left();
                while ops_cnt < available_sqes {
                    match op_chan.try_next() {
                        Ok(Some(op)) => Self::prepare(sq, op),
                        Ok(None) => panic!("proactor failed"),
                        Err(_) => break,
                    }
                    ops_cnt += 1;
                }
                if ops_cnt > 0 {
                    break;
                }
                debug!("submitter is waiting an operation");
                if let Some(op) = op_chan.next().await {
                    Self::prepare(sq, op);
                    ops_cnt = 1;
                }
            }

            debug!("submitter is submitting");
            let on_err = |err| panic!("proactor failed: {}", err);
            let n_submitted = sq.submit().unwrap_or_else(on_err);
            debug!("submitter submitted {} sqes", n_submitted);
            inflight_count.fetch_add(n_submitted, Ordering::SeqCst);
        }
    }

    fn completer(
        cq: &mut CompletionQueue<'static>,
        complete_event: &Event,
        inflight_count: &AtomicU32,
    ) {
        loop {
            debug!("completer enters loop");
            if cq.ready() == 0 {
                debug!("completer is waiting a cqe");
                cq.wait_cqes(1)
                    .unwrap_or_else(|err| panic!("proactor failed: {}", err));
            }
            let mut cqes_cnt = 0;
            while let Some(cqe) = cq.peek_cqe() {
                Self::complete(cqe);
                cqes_cnt += 1;
                unsafe { cq.advance_unchecked(1) };
            }
            debug!("completer completed {} cqes", cqes_cnt);
            let inflight_count = inflight_count
                .fetch_sub(cqes_cnt, Ordering::SeqCst)
                .wrapping_sub(cqes_cnt);
            if inflight_count < Self::RING_ENTRIES {
                complete_event.notify(1);
            }
        }
    }

    fn prepare(sq: &mut SubmissionQueue<'static>, op: Box<Operation>) {
        debug!("submitter is fetching a sqe");
        let sqe = loop {
            if let Some(sqe) = sq.get_sqe() {
                break sqe;
            }
        };
        
        let addr = better_as::pointer::to_address(&*op);
        let op = ManuallyDrop::new(op);
        match **op {
            Operation::Read {
                fd,
                offset,
                ref iovecs,
                ..
            } => unsafe {
                debug!("submitter is preparing a sqe: readv");
                sqe.prep_readv(fd, iovecs.as_ptr(), 1, offset)
                    .set_user_data(addr as u64)
            },
            Operation::Write {
                fd,
                offset,
                ref iovecs,
                ..
            } => unsafe {
                debug!("submitter is preparing a sqe: writev");
                sqe.prep_writev(fd, iovecs.as_ptr(), 1, offset)
                    .set_user_data(addr as u64)
            },
            Operation::Nop => panic!("proactor bug"),
        }

        debug!("submitter prepared a sqe");
    }

    fn complete(cqe: &CQE) {
        unsafe {
            let op_ptr = cqe.user_data() as *mut Operation;
            match &mut *op_ptr {
                Operation::Read { res, tx, .. } | Operation::Write { res, tx, .. } => {
                    *res = cqe.raw_result();
                    tx.try_send(tool::AssertSend::new(op_ptr)).unwrap();
                }
                Operation::Nop => {}
            }
        }
    }

    async fn call(
        &self,
        op: Box<Operation>,
        rx: &mut mpsc::Receiver<tool::AssertSend<*mut Operation>>,
    ) -> Box<Operation> {
        self.op_chan
            .clone()
            .send(op)
            .await
            .unwrap_or_else(|err| panic!("proactor failed: {}", err));

        let op_ptr = rx
            .next()
            .await
            .unwrap_or_else(|| panic!("proactor failed"))
            .into_inner();
        unsafe { Box::from_raw(op_ptr) }
    }

    fn resultify(res: i32) -> io::Result<i32> {
        if res >= 0 {
            Ok(res)
        } else {
            Err(io::Error::from_raw_os_error(-res))
        }
    }

    fn create_from_pool(
        &self,
    ) -> (
        mpsc::Sender<tool::AssertSend<*mut Operation>>,
        mpsc::Receiver<tool::AssertSend<*mut Operation>>,
        Box<Operation>,
    ) {
        self.object_pool.pop().unwrap_or_else(|| {
            let (tx, rx) = mpsc::channel(1);
            let op = Box::new(Operation::Nop);
            (tx, rx, op)
        })
    }

    #[allow(clippy::cast_sign_loss)]
    pub async fn read(
        &self,
        fd: RawFd,
        mut buf: AlignedBytes,
        len: usize,
        offset: isize,
    ) -> (RawFd, AlignedBytes, io::Result<usize>) {
        let (tx, mut rx, mut op) = self.create_from_pool();
        let len = len.min(buf.len());
        *op = Operation::Read {
            iovecs: [libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: len,
            }],

            fd,
            buf,
            len,
            offset,

            res: 0,

            tx,
        };
        let mut op = self.call(op, &mut rx).await;
        if let Operation::Read {
            fd, buf, res, tx, ..
        } = mem::replace(&mut *op, Operation::Nop)
        {
            drop(self.object_pool.push((tx, rx, op)));
            let result = Self::resultify(res).map(|nread| nread as usize);
            (fd, buf, result)
        } else {
            panic!("proactor bug")
        }
    }

    pub async fn read_in_thread_pool(
        &self,
        fd: RawFd,
        mut buf: AlignedBytes,
    ) -> (RawFd, AlignedBytes, io::Result<usize>) {
        unblock(move || {
            let mut iov = [nix::sys::uio::IoVec::from_mut_slice(&mut *buf)];
            let ret = nix::sys::uio::readv(fd, &mut iov).map_err(nix_to_io_error);
            (fd, buf, ret)
        })
        .await
    }

    #[allow(clippy::cast_sign_loss)]
    pub async fn write(
        &self,
        fd: RawFd,
        mut buf: AlignedBytes,
        nbytes: usize,
        offset: isize,
    ) -> (RawFd, AlignedBytes, io::Result<usize>) {
        let (tx, mut rx, mut op) = self.create_from_pool();
        let nbytes = nbytes.min(buf.len());
        *op = Operation::Write {
            iovecs: [libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: nbytes,
            }],

            fd,
            buf,
            nbytes,
            offset,

            res: 0,

            tx,
        };
        let mut op = self.call(op, &mut rx).await;
        if let Operation::Write {
            fd, buf, res, tx, ..
        } = mem::replace(&mut *op, Operation::Nop)
        {
            drop(self.object_pool.push((tx, rx, op)));
            let result = Self::resultify(res).map(|nread| nread as usize);
            (fd, buf, result)
        } else {
            panic!("proactor bug")
        }
    }
}
