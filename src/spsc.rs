//!
//! # SPSC #
//!
//! Single producer, single consumer, async-compatible ring buffer / queue / channel

use bitflags::bitflags;
use futures::task::AtomicWaker;
use std::{
    future::Future,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tracing::{debug as d, error as e, info as i, trace as t, warn as w};

bitflags! {
    struct Flags: u8 {
        const READ_DROPPED = 0b0001;
        const WRITE_DROPPED = 0b0010;
    }
}

#[derive(Debug, Clone, Default)]
#[repr(align(64))]
pub struct CachePadded<T> {
    inner: T,
}

unsafe impl<T: Send> Send for CachePadded<T> {}
unsafe impl<T: Sync> Sync for CachePadded<T> {}

impl<T> CachePadded<T> {
    /// Pads a value to the length of a cache line.
    pub fn new(t: T) -> CachePadded<T> {
        CachePadded::<T> { inner: t }
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> DerefMut for CachePadded<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

/// SPSC ring buffer
///
/// Single producer, single consumer queue/channel/ring that is very simple and
/// fast because it is a completely lock-free data structure. This is commonly
/// known as a lamport queue.
#[derive(Debug)]
pub struct Ring<'a, T> {
    /// The underlying buffer, as a boxed array of MaybeUninit's inside
    /// UnsafeCell's. Ideally this allows it to work with uninitialized memory
    /// safely. In theory anything read from the tail, will have already been
    /// written at the head, and properly initialized. TODO: The only wrinkle is
    /// that this needs to be communicated by which union variant the
    /// MaybeUninit is.
    buf: Box<[MaybeUninit<T>]>,
    /// The head of the ringbuffer, which is the writing side of the ring.
    head: CachePadded<AtomicUsize>,
    /// The tail of the ringbuffer, which is the reading side of the ring.
    tail: CachePadded<AtomicUsize>,
    /// Flags used to determining if readers or writers is already dropped or
    /// other various states.
    flags: AtomicU8,
    /// The capacity of the ring. This is fixed and at creation.
    capacity: usize,
    /// Read waker used to wake up readers that are waiting. Only capable of
    /// holding a single waker, so this does not support multiple tasks waiting
    /// for the same result or even different results of teh same ring. Hence:
    /// SPCS.. single producer, single consumer.
    read_waker: AtomicWaker,
    /// Write waker used to wake up writers when space becomes available in the
    /// ring.
    write_waker: AtomicWaker,
    __buffer_lifetime: PhantomData<&'a T>,
}

trait AsFlags {
    type Flags;

    fn into_flags(&self) -> Self::Flags;
}

impl AsFlags for AtomicU8 {
    type Flags = Option<Flags>;

    fn into_flags(&self) -> Self::Flags {
        let bits = self.load(Ordering::SeqCst);
        Flags::from_bits(bits)
    }
}

#[derive(Debug)]
struct Shared<'a, T> {
    inner: Arc<Ring<'a, T>>,
}

impl<T> Clone for Shared<'_, T> {
    fn clone(&self) -> Self {
        Shared {
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug)]
pub struct Tx<'a, T>(Shared<'a, T>);

#[derive(Debug)]
pub struct Rx<'a, T>(Shared<'a, T>);

impl<'a, T> Ring<'_, T> {
    pub fn create(capacity: usize) -> (Tx<'a, T>, Rx<'a, T>) {
        assert!(capacity > 1);
        let mut vec: Vec<MaybeUninit<T>> = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            vec.push(MaybeUninit::uninit());
        }
        let ring = Shared {
            inner: Arc::new(Ring {
                buf: vec.into_boxed_slice(),
                head: CachePadded::new(AtomicUsize::new(0)),
                tail: CachePadded::new(AtomicUsize::new(0)),
                flags: AtomicU8::new(0),
                capacity,
                read_waker: AtomicWaker::new(),
                write_waker: AtomicWaker::new(),
                __buffer_lifetime: Default::default(),
            }),
        };
        (Tx(ring.clone()), Rx(ring))
    }

    /// TODO: Is this even possible? Is this even worth it?
    ///
    /// # Safety #
    ///
    /// I actually don't know that this is ever safe -- more research is needed
    /// for me to know whether or not I can transmute a T ->
    /// UnsafeCell<MaybeUninit<T>>.. It's unknown wether they have the same size
    /// and layout, though they should.
    unsafe fn with_buffer(buffer: &'static mut [T]) -> (Tx<'a, T>, Rx<'a, T>) {
        let vec = Vec::from_raw_parts(buffer.as_mut_ptr(), buffer.len(), buffer.len());
        let capacity = buffer.len();
        let buf = vec.into_boxed_slice();
        let buf = std::mem::transmute(buf);
        let ring = Shared {
            inner: Arc::new(Ring {
                buf,
                head: CachePadded::new(AtomicUsize::new(0)),
                tail: CachePadded::new(AtomicUsize::new(0)),
                flags: AtomicU8::new(0),
                capacity,
                read_waker: AtomicWaker::new(),
                write_waker: AtomicWaker::new(),
                __buffer_lifetime: Default::default(),
            }),
        };
        (Tx(ring.clone()), Rx(ring))
    }

    /// Push a value onto the queue
    fn push(&self, item: T) -> Result<(), T> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Relaxed);
        if head.wrapping_sub(tail) == self.capacity {
            return Err(item);
        }

        // TODO: Change modulus (%) to bit-shift alternative eventually, as it is faster
        unsafe { (self.buf[(head % self.capacity) as usize].as_ptr() as *mut T).write(item) };
        self.head.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Pop's a value off the queue. It starts from the current tail position.
    /// It's potentially safe to clone and use from multiple tasks/threads but
    /// it's currently limited to just 1. To be honest limiting contention could
    /// actually help the performance.
    fn pop(&self) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            if head == tail {
                return None;
            }

            // TODO: Change modulus (%) to bit-shift alternative eventually, as it is faster
            let val = unsafe { (self.buf[(tail % self.capacity) as usize]).as_ptr().read() };
            let cew = self.tail.compare_exchange_weak(
                tail,
                tail.wrapping_add(1),
                Ordering::Release,
                Ordering::Relaxed,
            );
            if cew.is_ok() {
                return Some(val);
            }
        }
    }
}

impl<T> Tx<'_, T> {
    pub fn try_push(&mut self, item: T) -> Result<(), T> {
        self.0.inner.push(item)
    }

    pub fn push_async(&mut self, item: T) -> PushFuture<'_, T> {
        PushFuture::new(item, self.0.clone())
    }
}

impl<T> Drop for Tx<'_, T> {
    fn drop(&mut self) {
        let write_dropped = Flags::WRITE_DROPPED;
        self.0
            .inner
            .flags
            .fetch_add(write_dropped.bits(), Ordering::Relaxed);
        let now = self.0.inner.flags.into_flags();
        d!("Write side of ring dropped. Resulting flags: {:?}", now);
    }
}

impl<T: Unpin + Clone> Future for PushFuture<'_, T> {
    type Output = Result<(), T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.get_mut();
        if let Some(flags) = inner.ring.inner.flags.into_flags() {
            if flags.intersects(Flags::READ_DROPPED) {
                e!("Read side is dropped, cannot push value.");
                let item = inner
                    .item
                    .take()
                    .expect("failed to take value from future struct -- should not happen");
                return Poll::Ready(Err(item));
            }
        }
        let head = inner.ring.inner.head.load(Ordering::Acquire);
        let tail = inner.ring.inner.tail.load(Ordering::Acquire);
        if head.wrapping_sub(tail) == inner.ring.inner.capacity {
            t!("No space available in ring, registering waker and returning pending.");
            inner.ring.inner.write_waker.register(cx.waker());
            return Poll::Pending;
        }

        // TODO: Change modulus (%) to bit-shift alternative eventually, as it is faster
        unsafe {
            (inner.ring.inner.buf[(head % inner.ring.inner.capacity) as usize].as_ptr() as *mut T)
                .write(inner.item.take().unwrap())
        };

        inner
            .ring
            .inner
            .head
            .store(head.wrapping_add(1), Ordering::Release);

        // Wake up a waiting reader, because we have advanced  the head, so we can read
        // more values now.
        t!("Waking reader, if waker registered");
        inner.ring.inner.read_waker.wake();
        Poll::Ready(Ok(()))
    }
}

/// TODO: Should these methods require &mut T ?
impl<T> Rx<'_, T> {
    pub fn try_pop(&self) -> Option<T> {
        self.0.inner.pop()
    }

    /// TODO: Is this worth it? Or should the whole thing just support async,
    /// with no sync/failfast variant?
    pub fn pop_async(&self) -> PopFuture<'_, T> {
        PopFuture {
            ring: self.0.clone(),
        }
    }
}

impl<T> Drop for Rx<'_, T> {
    fn drop(&mut self) {
        let read_dropped = Flags::READ_DROPPED;
        self.0
            .inner
            .flags
            .fetch_add(read_dropped.bits(), Ordering::Relaxed);
        let now = self.0.inner.flags.into_flags();
        d!("Read side of ring dropped. Resulting flags: {:?}", now);
    }
}

impl<T> PushFuture<'_, T> {
    /// Construct a new PushFuture<'_, T> .. It can only be constructed with a
    /// T, even though the field is an option. This is because we need to
    /// utilize Option::take() to move the value out of the struct, while it's
    /// pinned during Future::poll(..).
    const fn new(item: T, ring: Shared<T>) -> PushFuture<'_, T> {
        PushFuture {
            item: Some(item),
            ring,
        }
    }
}

impl<T> Future for PopFuture<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.get_mut();
        loop {
            let head = inner.ring.inner.head.load(Ordering::Acquire);
            let tail = inner.ring.inner.tail.load(Ordering::Acquire);
            if head == tail {
                if let Some(flags) = inner.ring.inner.flags.into_flags() {
                    if flags.intersects(Flags::WRITE_DROPPED) {
                        return Poll::Ready(None);
                    }
                }

                // otherwise, wait for more..
                t!("Ring empty, registering waker and returning pending.");
                inner.ring.inner.read_waker.register(cx.waker());
                return Poll::Pending;
            }

            // TODO: Change modulus (%) to bit-shift alternative eventually, as it is faster
            let val = unsafe {
                inner.ring.inner.buf[(tail % inner.ring.inner.capacity) as usize]
                    .as_ptr()
                    .read()
            };
            let cew = inner.ring.inner.tail.compare_exchange_weak(
                tail,
                tail.wrapping_add(1),
                Ordering::Release,
                Ordering::Relaxed,
            );
            if cew.is_ok() {
                inner.ring.inner.write_waker.wake();
                return Poll::Ready(Some(val));
            }
        }
    }
}

pub struct PopFuture<'a, T> {
    ring: Shared<'a, T>,
}

pub struct PushFuture<'a, T> {
    item: Option<T>,
    ring: Shared<'a, T>,
}

unsafe impl<T> Send for PopFuture<'_, T> {}
unsafe impl<T> Send for PushFuture<'_, T> {}
unsafe impl<T> Send for Rx<'_, T> {}
unsafe impl<T> Sync for Rx<'_, T> {}
unsafe impl<T> Send for Tx<'_, T> {}
unsafe impl<T> Sync for Tx<'_, T> {}
unsafe impl<T> Send for Ring<'_, T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Error;
    use bytes::BytesMut;
    use std::{thread, time::Duration};
    use tokio::{stream::StreamExt, time};
    use tracing::Level;

    fn setup_tracing() {
        tracing_subscriber::fmt()
            .with_target(true)
            .with_level(true)
            .with_max_level(Level::DEBUG)
            .try_init();
    }

    #[tokio::test(core_threads = 4)]
    async fn async_spsc_buffers() -> Result<(), Error> {
        setup_tracing();
        let (mut submission_tx, submission_rx) = Ring::<BytesMut>::create(10);
        let (mut completion_tx, completion_rx) = Ring::<BytesMut>::create(10);
        drop(completion_rx);
        if let Err(t) = completion_tx
            .push_async(BytesMut::with_capacity(1500))
            .await
        {
            tracing::warn!("Could not push entry, rx side dropped.");
        }

        Ok(())
    }

    #[tokio::test]
    pub async fn async_spsc_static() {
        static mut BUF: &mut [u8] = &mut [0, 0, 0, 0];

        let (mut tx, rx) = unsafe { Ring::with_buffer(BUF) };
        let handle = tokio::spawn(async move {
            let resp = rx.pop_async().await;
            assert_eq!(resp.unwrap(), 10);
            let resp = rx.pop_async().await;
            assert_eq!(resp.unwrap(), 11);
        });

        tokio::time::delay_for(Duration::from_secs(1)).await;
        let resp = tx.push_async(10 as u8).await;
        let resp = tx.push_async(11 as u8).await;
        let resp = tx.push_async(12 as u8).await;
        let resp = tx.push_async(13 as u8).await;
        handle.await;
        dbg!(&tx);

        assert_eq!(tx.0.inner.head.load(Ordering::Relaxed), 4);
        assert_eq!(tx.0.inner.tail.load(Ordering::Relaxed), 2);

        // NOTE: if you use the Ring::with_buffer constructor, you must make sure not to
        // drop one of the handles (tx, rx) otherwise we will be dropping the static
        // memory. If this is ever a use-case we want, we can make it handle this case
        // transparently, but for not it's not worth it.
        std::mem::ManuallyDrop::new(tx);
    }

    #[tokio::test]
    pub async fn async_spsc_empty_rx() -> Result<(), Error> {
        let (mut tx, rx) = Ring::create(10);
        let handle = tokio::spawn(async move {
            for i in 0..20 {
                time::timeout(Duration::from_secs(1), rx.pop_async()).await;
                time::delay_for(Duration::from_millis(10)).await;
            }
        });

        time::delay_for(Duration::from_secs(1)).await;
        for i in 0..10 as u32 {
            time::timeout(Duration::from_millis(100), tx.push_async(i as u32)).await?;
            time::delay_for(Duration::from_millis(10)).await;
        }
        handle.await;
        assert_eq!(
            tx.0.inner.head.load(Ordering::Relaxed),
            tx.0.inner.tail.load(Ordering::Relaxed)
        );
        Ok(())
    }

    #[tokio::test]
    pub async fn async_spsc_full_tx() -> Result<(), Error> {
        let (mut tx, rx) = Ring::create(10);
        let handle = tokio::spawn(async move {
            let resp = rx.pop_async().await;
            time::delay_for(Duration::from_secs(4)).await;
        });

        time::delay_for(Duration::from_secs(1)).await;
        for i in 0..20 as u32 {
            let fut = tx.push_async(10 as u32);
            let resp = time::timeout(Duration::from_millis(1), fut).await;
            dbg!(i, &resp);
            if i <= 10 {
                assert!(resp.is_ok());
            } else {
                assert!(resp.is_err());
            }
        }
        handle.await;
        Ok(())
    }

    #[tokio::test]
    pub async fn async_spsc() {
        let (mut tx, rx) = Ring::create(10);
        let handle = tokio::spawn(async move {
            let resp = rx.pop_async().await;
        });

        tokio::time::delay_for(Duration::from_secs(1)).await;
        let resp = tx.push_async(10).await;
        handle.await;
    }

    // #[test]
    // TODO: test broken
    pub fn multi_thread_spsc() {
        let (mut tx, rx) = Ring::create(10);
        let one = tx.try_push(1);

        let handle = thread::spawn(move || {
            let wait = thread::sleep(Duration::from_secs(2));
            let mut last = 0;
            let mut count = 0;
            while last != 20 {
                let temp = rx.try_pop();
                println!("RX: {}: {:?}", count, temp);
                if let Some(x) = temp {
                    if x != 0 {
                        if (last + 1) == x {
                            println!("correct count..");
                        } else {
                            panic!("Got value out of order! X: {}, last: {}", x, last);
                        }
                    }
                    last = x;
                } else {
                    println!("Ring empty, sleeping..");
                    thread::sleep(Duration::from_millis(2));
                }
                count = count + 1;
            }
            dbg!(rx);
        });

        thread::sleep(Duration::from_secs(2));
        let handle2 = thread::spawn(move || {
            for j in 0..20 {
                for i in 0..5 {
                    println!("TX: {}", j);
                    let temp = tx.try_push(j);
                    println!("TX: {}: {:?}", j, temp);
                    if let Err(t) = temp {
                        thread::sleep(Duration::from_millis(2));
                        println!("TX: Retrying: {}", t);
                        tx.try_push(t);
                    } else {
                        break;
                    }
                }
            }
            dbg!(tx);
        });

        handle.join();
        handle2.join();
    }

    #[test]
    pub fn spsc() {
        let (mut tx, rx) = Ring::create(10);
        let one = tx.try_push(1);
        assert!(one.is_ok());
        let two = tx.try_push(2);
        assert!(two.is_ok());
        let three = tx.try_push(3);
        assert!(three.is_ok());
        let four = tx.try_push(4);
        assert!(four.is_ok());
        let one = rx.try_pop();
        assert!(one.is_some());
        let two = rx.try_pop();
        assert!(one.is_some());
    }

    #[test]
    pub fn huge_spsc() {
        let (mut tx, rx) = Ring::<usize>::create(1024 * 1024 * 2);
    }

    #[test]
    pub fn empty_spsc() {
        let (mut tx, rx) = Ring::<usize>::create(5);
        let res = rx.try_pop();
        assert_eq!(res, None);
    }

    #[test]
    pub fn fill_spsc() {
        let (mut tx, rx) = Ring::create(5);
        let tx_one = tx.try_push(1);
        assert!(tx_one.is_ok());
        let tx_two = tx.try_push(2);
        assert!(tx_two.is_ok());
        let tx_three = tx.try_push(3);
        assert!(tx_three.is_ok());
        let tx_four = tx.try_push(4);
        assert!(tx_four.is_ok());
        let tx_five = tx.try_push(5);
        assert!(tx_five.is_ok());
        let res = tx.try_push(6);
        assert!(res.is_err());
        let one = rx.try_pop();
        assert_eq!(one, Some(1));
        let res = tx.try_push(res.unwrap_err());
        assert!(res.is_ok());
        let res = tx.try_push(7);
        assert!(res.is_err());
        let two = rx.try_pop();
        assert_eq!(two, Some(2));
        let three = rx.try_pop();
        assert_eq!(three, Some(3));
    }
}
