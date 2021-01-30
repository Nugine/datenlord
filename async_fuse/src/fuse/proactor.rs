//! async-fuse IO proactor

#![allow(clippy::todo, dead_code, clippy::restriction, box_pointers)] // TODO: remove this

use crate::util::{nix_to_io_error, unblock};

use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::{io, thread};

use aligned_utils::bytes::AlignedBytes;
use crossbeam_queue::SegQueue;
use futures_channel::oneshot;
use once_cell::sync::Lazy;
use parking::{Parker, Unparker};
use ring_io::cqe::CQE;
use ring_io::ring::{Ring, RingBuilder};
use ring_io::sq::SubmissionQueue;
use ring_io::sqe::PrepareSqe;
use log::debug;

/// IO proactor
#[derive(Debug)]
pub struct Proactor {
    op_queue: Arc<SegQueue<Operation>>,
    unparker: Unparker,
}

enum Operation {
    Read {
        fd: RawFd,
        buf: AlignedBytes,
        tx: oneshot::Sender<(RawFd, AlignedBytes, io::Result<usize>)>,
    },
    Write {
        fd: RawFd,
        buf: AlignedBytes,
        nbytes: usize,
        tx: oneshot::Sender<(RawFd, AlignedBytes, io::Result<usize>)>,
    },
}

struct State {
    iovecs: [libc::iovec; 1],
    op: Operation,
}

impl Proactor {
    /// Gets the global proactor
    pub fn global() -> &'static Self {
        static GLOBAL_PROACTOR: Lazy<Proactor> =
            Lazy::new(|| Proactor::start_driver().expect("failed to start global proactor driver"));
        &*GLOBAL_PROACTOR
    }

    /// Starts the proactor's driver thread
    fn start_driver() -> io::Result<Self> {
        const RING_ENTRIES: u32 = 32;

        let mut ring = RingBuilder::new(RING_ENTRIES).build()?;

        let op_queue = Arc::new(SegQueue::new());
        let op_queue_1 = Arc::clone(&op_queue);

        let (parker, unparker) = parking::pair();

        thread::spawn(move || Self::driver(&mut ring, &*op_queue_1, &parker));

        Ok(Self { op_queue, unparker })
    }

    #[allow(clippy::as_conversions)]
    fn driver(ring: &mut Ring, op_queue: &SegQueue<Operation>, parker: &Parker) {
        let (mut sq, mut cq, _) = ring.split();

        let mut in_flight_count: usize = 0;

        loop {
            debug!("driver enters loop");

            // We have nothing to do, block until other threads call `unpark`.
            while in_flight_count == 0 && op_queue.is_empty() {
                debug!("driver is parking");
                parker.park();
            }

            debug!("driver starts working");

            // Calculate how many operations can we issue at this round.
            let n_ops = op_queue.len().min(sq.space_left() as usize);

            debug!("driver will issue {} operations", n_ops);

            // Issue operations
            for _ in 0..n_ops {
                let op = op_queue
                    .pop()
                    .unwrap_or_else(|| panic!("driver is in invalid state"));

                Self::issue(&mut sq, op);

                let (cnt, is_overflow) = in_flight_count.overflowing_add(1);
                assert!(!is_overflow);
                in_flight_count = cnt;
            }

            sq.submit()
                .unwrap_or_else(|err| panic!("driver is in invalid state: {}", err));

            debug!("driver in_flight_count = {}", in_flight_count);

            debug!("driver is waiting cqes");

            // Wait at least one CQE
            cq.wait_cqes(1)
                .unwrap_or_else(|err| panic!("driver is in invalid state: {}", err));

            debug!("driver will complete cqes");

            // Complete operations
            while let Some(cqe) = cq.peek_cqe() {
                Self::complete(cqe);
                cq.advance(1);

                let (cnt, is_overflow) = in_flight_count.overflowing_sub(1);
                assert!(!is_overflow);
                in_flight_count = cnt;
            }

            debug!("driver has completed cqes");

            debug!("driver in_flight_count = {}", in_flight_count);
        }
    }

    fn issue(sq: &mut SubmissionQueue<'_>, mut op: Operation) {
        let sqe = sq
            .get_sqe()
            .unwrap_or_else(|| panic!("driver is in invalid state"));

        match &mut op {
            Operation::Read { fd, buf, .. } => {
                dbg!(*fd, buf.len());

                let raw_fd = *fd;
                let buf_ptr = buf.as_mut_ptr();
                let buf_len = buf.len();

                let state = Box::into_raw(Box::new(State {
                    iovecs: [libc::iovec {
                        iov_base: buf_ptr.cast(),
                        iov_len: buf_len,
                    }],
                    op,
                }));

                unsafe {
                    let iovecs = (*state).iovecs.as_ptr();

                    sqe.prep_readv(raw_fd, iovecs, 1, -1)
                        .set_user_data(state as u64)
                }
            }
            Operation::Write {
                fd, buf, nbytes, ..
            } => {
                let raw_fd = *fd;
                let buf_ptr = buf.as_mut_ptr();
                let buf_len = buf.len();

                let state = Box::into_raw(Box::new(State {
                    iovecs: [libc::iovec {
                        iov_base: buf_ptr.cast(),
                        iov_len: (*nbytes).min(buf_len),
                    }],
                    op,
                }));

                unsafe {
                    let iovecs = (*state).iovecs.as_ptr();
                    sqe.prep_writev(raw_fd, iovecs, 1, -1)
                        .set_user_data(state as u64)
                }
            }
        }
    }

    fn complete(cqe: &CQE) {
        let state = unsafe { Box::from_raw(cqe.user_data() as *mut State) };
        let state = *state;

        let result = cqe.io_result();

        match state.op {
            Operation::Read { fd, buf, tx } => {
                let result = result.map(|nread| nread as usize);
                drop(tx.send((fd, buf, result)));
            }
            Operation::Write { fd, buf, tx, .. } => {
                let result = result.map(|nwritten| nwritten as usize);
                drop(tx.send((fd, buf, result)));
            }
        }
    }

    pub async fn read(
        &self,
        fd: RawFd,
        buf: AlignedBytes,
    ) -> (RawFd, AlignedBytes, io::Result<usize>) {
        let (tx, rx) = oneshot::channel();
        let op = Operation::Read { fd, buf, tx };
        self.op_queue.push(op);
        self.unparker.unpark();
        let on_err = |err| panic!("proactor failed: {}", err);
        rx.await.unwrap_or_else(on_err)
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

    pub async fn write(
        &self,
        fd: RawFd,
        buf: AlignedBytes,
        nbytes: usize,
    ) -> (RawFd, AlignedBytes, io::Result<usize>) {
        let (tx, rx) = oneshot::channel();
        let op = Operation::Write {
            fd,
            buf,
            nbytes,
            tx,
        };
        self.op_queue.push(op);
        self.unparker.unpark();
        let on_err = |err| panic!("proactor failed: {}", err);
        rx.await.unwrap_or_else(on_err)
    }
}
