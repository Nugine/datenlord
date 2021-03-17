#![allow(variant_size_differences)]

use super::storage::InlineStorage;

use std::cell::UnsafeCell;
use std::future::Future;
use std::mem::{self, ManuallyDrop, MaybeUninit};
use std::os::unix::io::RawFd;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::{io, thread};

use ring_io::cq::CompletionQueue;
use ring_io::cqe::CQE;
use ring_io::ring::RingBuilder;
use ring_io::sq::SubmissionQueue;
use ring_io::sqe::{PrepareSqe, SQE};

use futures::channel::mpsc;
use futures::task::AtomicWaker;
use futures::{ready, SinkExt, StreamExt};

use aligned_utils::bytes::AlignedBytes;
use crossbeam_queue::ArrayQueue;
use crossbeam_utils::atomic::AtomicCell;
use event_listener::{Event, EventListener};
use log::debug;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

struct Proactor {
    inner: Arc<ProactorInner>,
}

struct ProactorInner {
    chan: mpsc::Sender<SharedState>,
    pool: ArrayQueue<SharedState>,
    available_count: AtomicU32,
    complete_event: Event,
}

type SharedState = Arc<Mutex<State>>;

struct State {
    step: Step,
    waker: Option<Waker>,
    storage: InlineStorage, // TODO: naming
    escape: InlineStorage,
}

enum Step {
    /// Initial step, indicating the `State` structure contains nothing
    Empty,
    ///
    Preparing,
    ///
    InQueue { sqe: SQE },
    ///
    Submitted,
    ///
    Completed { res: i32 },
    /// The `IoRequest` future has been dropped.
    Dropped,
    ///
    Poisoned,
}

pub struct IoRequest<T: Send> {
    state: SharedState,
    listener: Option<EventListener>,
    data: ManuallyDrop<T>,
}

pub trait Operation {
    /// # Safety
    /// TODO:
    unsafe fn prepare(&mut self, storage: &mut InlineStorage, sqe: &mut MaybeUninit<SQE>);
}

impl<T: Operation + Send + Unpin> IoRequest<T> {
    pub fn new(data: T) -> Self {
        let proactor = Proactor::global();
        let handle = proactor.create_shared_state();
        let mut state = handle.lock();
        state.step = Step::Preparing;
        drop(state);
        Self {
            state: handle,
            listener: None,
            data: ManuallyDrop::new(data),
        }
    }
}

impl<T: Operation + Send + Unpin> Future for IoRequest<T> {
    type Output = (io::Result<u32>, T);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this: &mut Self = &mut *self;
        let ProactorInner {
            chan,
            complete_event,
            available_count,
            ..
        } = &*Proactor::global().inner;

        let mut state = this.state.lock();

        match state.step {
            Step::Preparing => 'prepare: loop {
                if let Some(ref mut listener) = this.listener {
                    ready!(Pin::new(listener).poll(cx))
                }
                this.listener = None;

                let mut count = available_count.load(Ordering::Acquire);
                loop {
                    if count == 0 {
                        this.listener = Some(complete_event.listen());
                        continue 'prepare;
                    }
                    match available_count.compare_exchange_weak(
                        count,
                        count - 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(c) => count = c,
                    }
                }

                let sqe = unsafe {
                    let mut sqe = SQE::new_uninit();
                    T::prepare(&mut this.data, &mut state.storage, &mut sqe);
                    sqe.assume_init()
                };

                state.waker = Some(cx.waker().clone());
                state.step = Step::InQueue { sqe };
                chan.clone().try_send(this.state.clone()).unwrap();
                break Poll::Pending;
            },
            Step::InQueue { .. } | Step::Submitted => {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Step::Completed { res } => {
                let result = resultify(res);
                let data = unsafe { ManuallyDrop::take(&mut this.data) };
                state.step = Step::Empty;
                Poll::Ready((result, data))
            }
            Step::Empty | Step::Dropped | Step::Poisoned => panic!("invalid step"),
        }
    }
}

impl<T: Send> Drop for IoRequest<T> {
    fn drop(&mut self) {
        let ProactorInner { pool, .. } = &*Proactor::global().inner;
        let mut state = self.state.lock();
        match mem::replace(&mut state.step, Step::Poisoned) {
            Step::Empty => {}
            Step::InQueue { .. } => {
                state.step = Step::Dropped;
            }
            Step::Submitted => unsafe {
                state.escape.put(ManuallyDrop::take(&mut self.data));
                state.step = Step::Dropped;
            },
            Step::Preparing | Step::Completed { .. } => {
                unsafe { ManuallyDrop::drop(&mut self.data) }
                reset_state(&mut *state);
                let _ = pool.push(self.state.clone());
            }
            Step::Dropped | Step::Poisoned => panic!("invalid step"),
        }
    }
}

fn resultify(res: i32) -> io::Result<u32> {
    if res >= 0 {
        Ok(res as u32)
    } else {
        Err(io::Error::from_raw_os_error(-res))
    }
}

fn reset_state(state: &mut State) {
    state.step = Step::Empty;
    state.storage.clear();
    state.escape.clear();
    state.waker = None;
}

impl Proactor {
    fn global() -> &'static Proactor {
        static GLOBAL_PROACTOR: Lazy<Proactor> =
            Lazy::new(|| Proactor::start_driver().expect("failed to start global proactor driver"));
        &*GLOBAL_PROACTOR
    }

    fn create_shared_state(&self) -> SharedState {
        match self.inner.pool.pop() {
            Some(state) => state,
            None => Arc::new(Mutex::new(State {
                step: Step::Empty,
                storage: InlineStorage::empty(),
                escape: InlineStorage::empty(),
                waker: None,
            })),
        }
    }

    const RING_ENTRIES: u32 = 32;

    fn start_driver() -> io::Result<Self> {
        let ring = RingBuilder::new(Self::RING_ENTRIES).build()?;
        let ring = Box::leak(Box::new(ring));
        let (mut sq, mut cq, _) = ring.split();

        let available = Self::RING_ENTRIES * 2;
        let (tx, mut rx) = mpsc::channel(available as usize);

        let inner = Arc::new(ProactorInner {
            chan: tx,
            pool: ArrayQueue::new(available as usize),
            available_count: AtomicU32::new(available),
            complete_event: Event::new(),
        });

        {
            let inner = inner.clone();
            smol::spawn(async move { Self::submitter(&mut sq, &mut rx, &*inner).await }).detach();
        }
        {
            let inner = inner.clone();
            thread::spawn(move || Self::completer(&mut cq, &*inner));
        }

        Ok(Self { inner })
    }

    async fn submitter(
        sq: &mut SubmissionQueue<'static>,
        chan: &mut mpsc::Receiver<SharedState>,
        inner: &ProactorInner,
    ) {
        loop {
            let mut ops_cnt = 0;
            loop {
                let available_sqes = sq.space_left();
                while ops_cnt < available_sqes {
                    match chan.try_next() {
                        Ok(Some(handle)) => Self::prepare(sq, handle, &inner.pool),
                        Ok(None) => panic!("proactor failed"),
                        Err(_) => break,
                    }
                    ops_cnt += 1;
                }
                if ops_cnt > 0 {
                    break;
                }
                debug!("submitter is waiting an operation");
                if let Some(handle) = chan.next().await {
                    Self::prepare(sq, handle, &inner.pool);
                    ops_cnt = 1;
                }
            }

            debug!("submitter is submitting");
            let on_err = |err| panic!("proactor failed: {}", err);
            let n_submitted = sq.submit().unwrap_or_else(on_err);
            debug!("submitter submitted {} sqes", n_submitted);
        }
    }

    fn completer(cq: &mut CompletionQueue<'static>, inner: &ProactorInner) {
        let ProactorInner {
            available_count,
            complete_event,
            pool,
            ..
        } = inner;
        loop {
            debug!("completer enters loop");
            if cq.ready() == 0 {
                debug!("completer is waiting a cqe");
                cq.wait_cqes(1)
                    .unwrap_or_else(|err| panic!("proactor failed: {}", err));
            }
            let mut cqes_cnt = 0;
            while let Some(cqe) = cq.peek_cqe() {
                Self::complete(cqe, pool);
                cqes_cnt += 1;
                unsafe { cq.advance_unchecked(1) };
            }
            debug!("completer completed {} cqes", cqes_cnt);

            available_count.fetch_add(cqes_cnt, Ordering::AcqRel);
            complete_event.notify(cqes_cnt as usize);
        }
    }

    fn prepare(
        sq: &mut SubmissionQueue<'static>,
        handle: SharedState,
        pool: &ArrayQueue<SharedState>,
    ) {
        let mut state = handle.lock();

        match mem::replace(&mut state.step, Step::Poisoned) {
            Step::InQueue { sqe: prepared_sqe } => {
                debug!("submitter is fetching a sqe");
                let sqe = loop {
                    if let Some(sqe) = sq.get_sqe() {
                        break sqe;
                    }
                };

                let addr = Arc::into_raw(handle.clone());
                *sqe = prepared_sqe;
                sqe.set_user_data(addr as u64);

                state.step = Step::Submitted;

                debug!("submitter prepared a sqe");
            }
            Step::Dropped => {
                reset_state(&mut *state);
                drop(state);
                let _ = pool.push(handle);
            }
            Step::Empty
            | Step::Preparing
            | Step::Poisoned
            | Step::Completed { .. }
            | Step::Submitted => panic!("invalid step"),
        };
    }

    fn complete(cqe: &CQE, pool: &ArrayQueue<SharedState>) {
        unsafe {
            let addr = cqe.user_data() as usize;
            let handle = Arc::from_raw(addr as *const Mutex<State>);
            let mut state = handle.lock();

            match mem::replace(&mut state.step, Step::Poisoned) {
                Step::Submitted => {
                    let res = cqe.raw_result();
                    state.step = Step::Completed { res };
                    if let Some(waker) = state.waker.take() {
                        waker.wake()
                    }
                }
                Step::Dropped => {
                    reset_state(&mut *state);
                    drop(state);
                    let _ = pool.push(handle);
                }
                Step::Empty
                | Step::Preparing
                | Step::InQueue { .. }
                | Step::Completed { .. }
                | Step::Poisoned => panic!("invalid step"),
            }
        }
    }
}
