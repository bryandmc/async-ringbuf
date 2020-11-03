//!
//! *RinBuf*: Async Ring Buffer
//!
//! Asynchronous ringbuffer implementation for fast transfer of data from one
//! task to another. Generally considered to be SPSC (single producer, single
//! consumer) and will be optimized for this use case.
//!
//! This use case is critical to being able to haul the data to the backend with
//! as few copies as possible, while not needing to synchronize anywhere. It's
//! also critical that it doesn't hold up the current task, because that task
//! will be in charge of the normal flow of traffic.
//!
//! TODO: FUTURE: Finish implementation. Currently not used by proxy.

use atomic::Ordering;
use bitflags::bitflags;
use core::slice;
use futures::{task::AtomicWaker, Future};
use std::{
    cell::Cell,
    io,
    marker::PhantomData,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    sync::{
        atomic::{self, AtomicBool, AtomicU8, AtomicUsize},
        Arc,
    },
    task,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, debug_span, info, instrument, Span};

/// The maximumum size of a single read or write from the ring. It will rarely
/// be this size in practice, but if a buffer of that size is requested, it can
/// be fulfilled, up to that point.
const MAX_BATCH_SIZE: usize = KB(8);

const EXAMPLE_MB: usize = MB(2);

#[allow(non_snake_case)]
pub const fn KB(bytes: usize) -> usize {
    bytes * 1024
}

#[allow(non_snake_case)]
pub const fn MB(bytes: usize) -> usize {
    bytes * 1024 * 1024
}

bitflags! {
    /// Some flags that help us manage certain types of state that are less critical
    /// to be correct. There are other methods for determining most other things.
    /// TODO: Do we still need it? Namely, can we know a reader or writer has been
    /// dropped without this? Check arc counts?
    struct RingFlags: u8 {
        const WRITE_DROP = 0b0001;
        const READ_DROP = 0b0010;
        const WRITE_WRAPPED = 0b0100;
        const RING_FULL = 0b1000;
    }
}

/// Error type for ringbuf operations that can fail. The main ones are that we
/// do not have space to give a buffer for either reads or writes.
#[derive(thiserror::Error, Debug)]
pub enum RingError {
    #[error("no read space available")]
    NoReadSpaceAvailable,

    #[error("no write space available")]
    NoWriteSpaceAvailable,

    #[error("IO error encountered")]
    IoError(#[from] std::io::Error),
}

/// TODO: initialize buffer with uninitialized data, so that we don't have to
/// zero-out all the buffer data before first use.
///
/// # RingBuf #
/// Type used to quickly transfer data from one AsyncRead/AsyncWrite
/// to another, in another task, without cloning the buffer. Useful for
/// situations where you cannot just move the buffer, because you still ALSO
/// need the data in the originating task.
#[derive(Debug)]
pub struct Ring<T> {
    /// The read position of the ring.
    tail: AtomicUsize,

    /// The waker used to wake a reader to tell them there is more data.
    reader: AtomicWaker,

    /// The write position of the ring.
    head: AtomicUsize,

    /// The waker used to wake the writer, to tell them there is more space to
    /// write.
    writer: AtomicWaker,

    /// The underlying data for the ring.
    slab: *mut T,

    /// The max (fixed) capacity of the ring.
    capacity: usize,

    /// Various flags to determine certain kinds of state situations
    flags: AtomicU8,

    /// Determines whether or not the value has wrapped around. It allows you to
    /// differentiate between an empty and full ring.
    wrapped: AtomicBool,
}

/// # RingReader #
///
/// The reader side of the Ring Buffer
#[derive(Debug)]
pub struct Rx<T> {
    ring: Arc<Ring<T>>,
}

/// # RingWriter #
///
/// The write side of a ringbuffer
///
/// # Cases: #
/// ----------------------------------------------------------------------
/// [ head | + | + | + | + | tail | - | - ]
/// -----> -----> -----> -----> ----->
/// This means there are 4 slots to write until we hit tail
///
/// [ + | tail | - | - | head | + | + | + ]
///   -----> -----> -----> -----> ----->
///  This means there are 4 slots to write until we hit tail. This involves
///  wrapping around.
///
/// [ + | tail | - | - | head | + | + | + ]
///   -----> -----> -----> -----> ----->
/// [---]                     [-----------]
///               segment -----^^^^^^^^^^^
///
/// [ + | + | + | tail | - | - | - | head ]
///  -----> -----> -----> -----> ----->
/// [-----------]
///  ^^^^^^^^^^^--------- wrapped
///  This means there are 4 slots to write until we hit tail. This
///  involves wrapping around.
#[derive(Debug)]
pub struct Tx<T> {
    /// Inner shared-reference to the RingBuf
    ring: Arc<Ring<T>>,
}

impl<T: Copy> Rx<T> {
    fn head(&self) -> usize {
        self.ring.head()
    }

    fn tail(&self) -> usize {
        self.ring.tail()
    }

    fn get_ring_slab(&self) -> &[T] {
        self.ring.get_ring_slab()
    }

    fn get_fields(&self) -> (usize, usize, usize, bool) {
        self.ring.get_fields()
    }

    fn get_buffer(&self, length: usize) -> Result<(RxGuard<T>, usize), RingError> {
        let (mut head, mut tail, cap, mut wrapped) = self.get_fields();

        let mut amt_available = |h: usize, t: usize, c: usize| match t < h {
            true => h - t,
            false => (c - t) + h,
        };
        let mut bytes_read = 0;

        match wrapped {
            true => {
                let total_available = amt_available(head, tail, cap);
                // let contiguous_available = (cap - head);
                let batch_size = MAX_BATCH_SIZE.min(total_available).min(length);

                debug!(batch_size, total_available, head, tail, cap, wrapped);
                if batch_size == 0 {
                    return Err(RingError::NoReadSpaceAvailable);
                }

                let slice = self.ring.create_slice(tail, batch_size);
                let ret = RxGuard(slice, self.ring.clone(), 0);
                Ok((ret, batch_size))
            }
            false => {
                let total_available = amt_available(head, tail, cap);
                let mut batch_size = MAX_BATCH_SIZE
                    .min(total_available)
                    .min(length)
                    .min(head - tail);

                debug!(batch_size, total_available, head, tail, cap, wrapped);
                if batch_size == 0 {
                    return Err(RingError::NoReadSpaceAvailable);
                }

                let slice = self.ring.create_slice(tail, batch_size);
                let ret = RxGuard(slice, self.ring.clone(), 0);
                Ok((ret, batch_size))
            }
        }
    }
}

impl<T: Copy> Tx<T> {
    fn head(&self) -> usize {
        self.ring.head()
    }

    fn tail(&self) -> usize {
        self.ring.tail()
    }

    fn reset_to_zero(&self, head: &mut usize) {
        debug!("Resetting head to zero..");
        self.ring.head.store(0, Ordering::SeqCst);
        *head = 0;
        self.ring.wrapped.store(true, Ordering::SeqCst);
    }

    fn get_ring_slab(&self) -> &[T] {
        self.ring.get_ring_slab()
    }

    fn get_fields(&self) -> (usize, usize, usize, bool) {
        self.ring.get_fields()
    }

    fn get_buffer(&self, length: usize) -> Result<(TxGuard<T>, usize), RingError> {
        let (mut head, mut tail, cap, mut wrapped) = self.get_fields();
        let mut amt_available = |h: usize, t: usize, c: usize| match t < h {
            true => h - t,
            false => (c - t) + h,
        };

        match wrapped {
            true => {
                let available = tail - head;
                let batch_size = MAX_BATCH_SIZE.min(available).min(length);

                info!(
                    "head: {}, tail: {}, cap: {}, wrapped: {}, batch_size: {}",
                    head, tail, cap, wrapped, batch_size
                );
                let slice = self.ring.create_slice_mut(head, batch_size);
                return Ok((TxGuard(slice, self.ring.clone(), 0), batch_size));
            }
            false => {
                let available = cap - head;
                let batch_size = MAX_BATCH_SIZE.min(available).min(length);
                info!(
                    "head: {}, tail: {}, cap: {}, wrapped: {}, batch_size: {}",
                    head, tail, cap, wrapped, batch_size
                );
                assert!(batch_size > 0);
                assert!((head + batch_size) <= cap);
                let slice = self.ring.create_slice_mut(head, batch_size);
                return Ok((TxGuard(slice, self.ring.clone(), 0), batch_size));
            }
        }
        Err(RingError::NoWriteSpaceAvailable)
    }

    fn get_ref_count(&self) -> usize {
        Arc::strong_count(&self.ring)
    }
}

/// The future for reading IO from a source and writing it to a ring.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadIOFuture<'a, T, IO: AsyncRead + ?Sized + Unpin> {
    io: &'a mut IO,
    ring: Arc<Ring<T>>,
    try_read: usize,
}

/// The future for writing IO from data from the ring.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct WriteIOFuture<'a, T, IO: AsyncWrite + ?Sized + Unpin> {
    io: &'a mut IO,
    ring: Arc<Ring<T>>,
    try_write: usize,
}

/// The trait that handles asynchronous reading from an IO source, and writing
/// that data to a ring.
pub trait AsyncRingIORead<'a, R: AsyncRead + Unpin> {
    /// The future that represents the read from an IO object and the subsequent
    /// write to the ring. That said, the ring will only ever
    /// asynchronously-block if it is entirely full, otherwise it operates
    /// completely synchronously.
    type ReadFuture;

    /// Reads from an IO object (R), writing it to the ringbuffer.
    ///
    /// ## Arguments ##
    ///
    /// `try_read` gives an upper bound to the amount we will attempt to
    /// transfer from the IO object to the ring. That said, only the amount
    /// actually read will be counted when the cursor is advanced.
    fn read_io(&self, io: &'a mut R, try_read: usize) -> Self::ReadFuture;
}

/// The trait that handles reading from a ring and writing that data to an IO
/// source.
pub trait AsyncRingIOWrite<'a, W: AsyncWrite + Unpin> {
    /// The future that is returned that represents the write operation to the
    /// IO object. The reading from the ring can also block-asynchronously, but
    /// is otherwise a synchronous operation.
    type WriteFuture;

    /// Writes to the IO object (W), taking it's data from the ringbuffer.
    ///
    /// ## Arguments ##
    ///
    /// `try_write` gives an upper bound to the amount we will attempt to read
    /// from the ring, to write to the IO object. That said, even if you take
    /// out more bytes from the ring then you end up writing, you will only
    /// advance the cursor as much as you *ACTUALLY* wrote, not what you
    /// attempted to originally.
    fn write_io(&self, io: &'a mut W, try_write: usize) -> Self::WriteFuture;
}

impl<'a, R: AsyncRead + Unpin + 'a> AsyncRingIORead<'a, R> for Tx<u8> {
    type ReadFuture = ReadIOFuture<'a, u8, R>;

    fn read_io(&self, io: &'a mut R, try_read: usize) -> Self::ReadFuture {
        ReadIOFuture {
            io,
            ring: self.ring.clone(),
            try_read,
        }
    }
}

impl<'a, W: AsyncWrite + Unpin + 'a> AsyncRingIOWrite<'a, W> for Rx<u8> {
    type WriteFuture = WriteIOFuture<'a, u8, W>;

    fn write_io(&self, io: &'a mut W, try_write: usize) -> Self::WriteFuture {
        WriteIOFuture {
            io,
            ring: self.ring.clone(),
            try_write,
        }
    }
}

impl<'a, IO: AsyncWrite + Unpin> Future for WriteIOFuture<'a, u8, IO> {
    type Output = Result<usize, RingError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let inner = self.get_mut();
        let (mut head, mut tail, cap, mut wrapped) = inner.ring.get_fields();
        let mut amt_available = |h: usize, t: usize, c: usize| match t < h {
            true => h - t,
            false => (c - t) + h,
        };
        let mut bytes_read = 0;

        let (rx, amt) = match wrapped {
            true => {
                let total_available = amt_available(head, tail, cap);
                // let contiguous_available = (cap - head);
                let batch_size = MAX_BATCH_SIZE.min(total_available).min(inner.try_write);

                // debug!(batch_size, total_available, head, tail, cap, wrapped);
                if batch_size == 0 {
                    inner.ring.reader.register(cx.waker());
                    return task::Poll::Pending;
                }

                let slice = inner.ring.create_slice(tail, batch_size);
                let ret = RxGuard(slice, inner.ring.clone(), 0);
                (ret, batch_size)
            }
            false => {
                let total_available = amt_available(head, tail, cap);
                let mut batch_size = MAX_BATCH_SIZE
                    .min(total_available)
                    .min(inner.try_write)
                    .min(head - tail);

                // debug!(batch_size, total_available, head, tail, cap, wrapped);
                if batch_size == 0 {
                    inner.ring.reader.register(cx.waker());
                    return task::Poll::Pending;
                }

                let slice = inner.ring.create_slice(tail, batch_size);
                let ret = RxGuard(slice, inner.ring.clone(), 0);
                (ret, batch_size)
            }
        };

        tokio::pin! {
            let write_fut = inner.io.write(&*rx);
        };

        let resp: task::Poll<Result<usize, std::io::Error>> = write_fut.poll(cx);
        match resp {
            task::Poll::Ready(Ok(amt)) => {
                debug!("putting slice: {:#?} in ring..", rx);
                unsafe { inner.ring.advance_tail(amt) };
                debug!("Advanced (tail) ring {} bytes", amt);
                inner.ring.writer.wake();
                task::Poll::Ready(Ok(amt))
            }
            task::Poll::Ready(Err(err)) => task::Poll::Ready(Err(err.into())),
            task::Poll::Pending => task::Poll::Pending,
        }
    }
}

impl<'a, IO: AsyncRead + Unpin> Future for ReadIOFuture<'a, u8, IO> {
    type Output = Result<usize, RingError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let inner = self.get_mut();
        let (mut head, mut tail, cap, mut wrapped) = inner.ring.get_fields();
        let mut amt_available = |h: usize, t: usize, c: usize| match t < h {
            true => h - t,
            false => (c - t) + h,
        };

        let slice = match wrapped {
            true => {
                let available = tail - head;
                let batch_size = MAX_BATCH_SIZE.min(available).min(inner.try_read);

                // info!(
                //     "head: {}, tail: {}, cap: {}, wrapped: {}, batch_size: {}",
                //     head, tail, cap, wrapped, batch_size
                // );
                if batch_size == 0 {
                    inner.ring.writer.register(cx.waker());
                    return task::Poll::Pending;
                }
                inner.ring.create_slice_mut(head, batch_size)
            }
            false => {
                let available = cap - head;
                let batch_size = MAX_BATCH_SIZE.min(available).min(inner.try_read);
                // info!(
                //     "head: {}, tail: {}, cap: {}, wrapped: {}, batch_size: {}",
                //     head, tail, cap, wrapped, batch_size
                // );
                if batch_size == 0 {
                    inner.ring.writer.register(cx.waker());
                    return task::Poll::Pending;
                }
                assert!(batch_size > 0);
                assert!((head + batch_size) <= cap);
                inner.ring.create_slice_mut(head, batch_size)
            }
        };

        tokio::pin! {
            let read_fut = inner.io.read(slice);
        };
        let resp: task::Poll<Result<usize, std::io::Error>> = read_fut.poll(cx);
        // debug!("{:#?}", resp);
        match resp {
            /// The IO source has been closed and returned Ok(0). This is what
            /// we will also return, and not advance the cursor inside the ring
            /// at all.
            task::Poll::Ready(Ok(0)) => task::Poll::Ready(Ok(0)),

            /// The IO source has returned 'amt' bytes and so we need to advance
            /// the ring's cursor by that much as well.
            task::Poll::Ready(Ok(amt)) => {
                debug!("putting slice: {:#?} in ring..", slice);
                unsafe { inner.ring.advance_head(amt) };
                debug!("Advanced (head) ring {} bytes", amt);
                inner.ring.reader.wake();
                task::Poll::Ready(Ok(amt))
            }

            /// An error has occurred doing async IO, and we need to pass that
            /// up..
            task::Poll::Ready(Err(err)) => task::Poll::Ready(Err(err.into())),

            /// The IO source returned Pending, which is what we will also
            /// return..
            task::Poll::Pending => task::Poll::Pending,
        }
    }
}

/// A special tuple-struct for acting as a guard for a mutable slice, that needs
/// to change the index of the tail inside the ring-buffer upon being dropped.
/// This currently has some design deficiencies that are hard to cope with..
/// Namely, if you read less than the buffer size, there's no way to tell it
/// that, and it will attempt to advance the ring by the entire length of the
/// buffer.
#[derive(Debug)]
pub struct TxGuard<'a, T: Copy>(&'a mut [T], Arc<Ring<T>>, usize);

impl<'a, T: Copy> Drop for TxGuard<'a, T> {
    fn drop(&mut self) {
        let (mut head, mut tail, cap, mut wrapped) = self.1.get_fields();
        let write_size = self.2;
        info!(
            "Going to move head to ({}) + write size ({})",
            head, write_size
        );
        let mut resulting = head + write_size;

        // if we are at the end, wrap around
        if (head + write_size) == cap {
            resulting = 0;
            self.1.wrapped.store(true, Ordering::Release);
        }
        self.1.head.store(resulting, Ordering::Relaxed);
    }
}

impl<'a, T: Copy> Deref for TxGuard<'a, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T: Copy> DerefMut for TxGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

/// Guard for protecting the immutable buffer that represents previously written
/// data. It will advance it's position (tail) upon being dropped.
#[derive(Debug)]
pub struct RxGuard<'a, T: Copy>(&'a [T], Arc<Ring<T>>, usize);

impl<'a, T: Copy> Deref for RxGuard<'a, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

impl<'a, T: Copy> Drop for RxGuard<'a, T> {
    fn drop(&mut self) {
        let (mut head, mut tail, cap, mut wrapped) = self.1.get_fields();
        let read_size = self.2;
        info!(
            "Going to move tail to ({}) + read size ({})",
            tail, read_size
        );
        let mut resulting = tail + read_size;
        assert!(tail + read_size <= cap);

        // if we are at the end, wrap around
        if (tail + read_size) == cap {
            resulting = 0;
            self.1.wrapped.store(true, Ordering::Release);
        }
        self.1.tail.store(resulting, Ordering::Relaxed);
    }
}

/// Implementation for the core ringbuffer functionality. Because most of the
/// other structs contain a atomic reference counted reference to this struct,
/// it tends to be the core of everything. Luckily ringbuffers are inherently
/// pretty simple. The only complexity comes from fitting all that into the
/// async ecosystem rust has that contains a number of ways to do things. This
/// is why you can use the AsyncRead/AsyncWrite to the ring directly, or give an
/// IO object and have those IO objects pull from the ring as their source of
/// writes, and destination of reads to said IO objects.
impl<T: Copy> Ring<T> {
    pub(crate) fn new(size: usize) -> Ring<T> {
        let mut backing_vec = Vec::with_capacity(size);
        let cap = backing_vec.capacity();
        let ptr = backing_vec.as_mut_ptr();
        std::mem::forget(backing_vec);
        Ring {
            tail: AtomicUsize::new(0),
            reader: AtomicWaker::new(),
            head: AtomicUsize::new(0),
            writer: AtomicWaker::new(),
            slab: ptr,
            capacity: cap,
            flags: AtomicU8::new(0),
            wrapped: AtomicBool::new(false),
        }
    }

    pub fn create(size: usize) -> (Tx<T>, Rx<T>) {
        let ring = Arc::new(Ring::new(size));
        (Tx { ring: ring.clone() }, Rx { ring })
    }

    fn create_slice(&self, offset: usize, len: usize) -> &[T] {
        assert!(offset < self.capacity);
        assert!(len > 0);

        let ptr = unsafe { self.slab.add(offset) };
        let slice = unsafe { slice::from_raw_parts_mut(ptr, len) };
        slice
    }

    #[allow(clippy::mut_from_ref)]
    fn create_slice_mut(&self, offset: usize, len: usize) -> &mut [T] {
        assert!(offset < self.capacity);
        assert!(len > 0);

        let ptr = unsafe { self.slab.add(offset) };
        let slice = unsafe { slice::from_raw_parts_mut(ptr, len) };
        slice
    }

    fn get_ring_slab(&self) -> &[T] {
        let slice = unsafe { slice::from_raw_parts_mut(self.slab, self.capacity) };
        slice
    }

    fn head(&self) -> usize {
        self.head.load(Ordering::SeqCst)
    }

    fn tail(&self) -> usize {
        self.tail.load(Ordering::SeqCst)
    }

    fn wrapped(&self) -> bool {
        self.wrapped.load(Ordering::Relaxed)
    }

    fn write_region(&self, offset: usize, buf: &[T]) -> usize {
        let mut slice = self.create_slice_mut(offset, buf.len());
        slice.copy_from_slice(buf);
        buf.len()
    }

    fn read_region(&self, offset: usize, buf: &mut [T]) -> usize {
        let size = buf.len();
        let slice = self.create_slice(offset, size);
        buf[..size].copy_from_slice(slice);
        size
    }

    fn get_fields(&self) -> (usize, usize, usize, bool) {
        let mut head = self.head.load(Ordering::Relaxed);
        let mut tail = self.tail.load(Ordering::Relaxed);
        let mut cap = self.capacity;
        let mut wrapped = self.wrapped.load(Ordering::Relaxed);

        (head, tail, cap, wrapped)
    }

    /// Advances the head position
    ///
    /// # Safety #
    ///
    /// It is completely safe to do this operation in a memory-safety sense, but
    /// for it to be logically correct, it is required that it only takes place
    /// in specific locations where the actual head position needs to change.
    ///
    /// Returns whether or not the value has wrapped around, and sets the
    /// related field to indicate that.
    unsafe fn advance_head(&self, amt: usize) -> bool {
        let existing = self.head.load(Ordering::Acquire);
        if existing + amt >= self.capacity {
            self.head.store(0, Ordering::Release);
            self.wrapped.store(true, Ordering::Release);
            return true;
        }
        self.head.store(existing + amt, Ordering::Release);
        false
    }

    /// Advances the tail position
    ///
    /// # Safety #
    ///
    /// It is completely safe do use this method from a memory safety
    /// standpoint. What is not 'safe', though, is using these methods
    /// incorrectly. They can cause incorrect data to be read or incorrect
    /// regions to be written to, if used incorrectly.
    ///
    /// Returns wether or not the value has wrapped around.
    unsafe fn advance_tail(&self, amt: usize) -> bool {
        let existing = self.tail.load(Ordering::Acquire);
        if existing + amt >= self.capacity {
            self.tail.store(0, Ordering::Release);
            self.wrapped.store(false, Ordering::Release);
            return true;
        }
        self.tail.store(existing + amt, Ordering::Release);
        false
    }
}

/// Drop implementation for the entire Ring<T> structure. This is basically the
/// last thing that will be dropped and it will finally free the underlying
/// buffer.
impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let v = unsafe { Vec::from_raw_parts(self.slab, 0, self.capacity) };
    }
}

/// The drop implementation for the RX half of the ringbuffer. All it really
/// does is set some flags that aren't currently even being used.
/// TODO: handle the read_drop flags in the writer.
impl<T> Drop for Rx<T> {
    fn drop(&mut self) {
        self.ring
            .flags
            .fetch_and(RingFlags::READ_DROP.bits, Ordering::SeqCst);
    }
}

/// The drop implementation for the TX half of the ringbuffer. All it does is
/// set some flags that aren't currently used.
/// TODO: handle the write_drop flags in the reader side correctly.
impl<T> Drop for Tx<T> {
    fn drop(&mut self) {
        self.ring
            .flags
            .fetch_and(RingFlags::WRITE_DROP.bits, Ordering::SeqCst);
    }
}

/// AsyncRead implementation for the RX side of the ringbuffer.
impl AsyncRead for Rx<u8> {
    // #[instrument]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        mut buf: &mut [u8],
    ) -> task::Poll<io::Result<usize>> {
        info!("<******* [ [ [ START READ ] ] ] *******>");
        let mut inner = self.get_mut();
        let max_read = buf.len();
        let mut head = inner.ring.head.load(Ordering::Relaxed);
        let mut tail = inner.ring.tail.load(Ordering::Relaxed);
        let mut cap = inner.ring.capacity;
        let mut wrapped = inner.ring.wrapped.load(Ordering::Relaxed);
        let mut bytes_read = 0;
        if let Some(RingFlags::WRITE_DROP) =
            RingFlags::from_bits(inner.ring.flags.load(Ordering::Relaxed))
        {
            // read the rest and then drop..
        }

        let mut amt_available = |h: usize, t: usize, c: usize| match t < h {
            true => h - t,
            false => (c - t) + h,
        };
        let remaining_to_read = max_read - bytes_read;

        match wrapped {
            true => {
                let total_available = amt_available(head, tail, cap);
                let contiguous_available = (cap - tail);
                let batch_size = MAX_BATCH_SIZE
                    .min(contiguous_available)
                    .min(total_available)
                    .min(remaining_to_read);

                if batch_size == 0 {
                    inner.ring.reader.register(cx.waker());
                    return task::Poll::Pending;
                }

                bytes_read += inner.ring.read_region(tail, &mut buf[..batch_size]);
                let mut remaining = (total_available - batch_size).min(max_read - bytes_read);
                if remaining > 0 {
                    bytes_read += inner
                        .ring
                        .read_region(0, &mut buf[batch_size..batch_size + remaining]);
                    inner.ring.wrapped.store(false, Ordering::Relaxed);
                    inner.ring.tail.store(remaining, Ordering::Relaxed);
                } else {
                    let mut new_tail = (tail + bytes_read);
                    if new_tail == cap {
                        new_tail = 0;
                        inner.ring.wrapped.store(false, Ordering::Relaxed);
                    }
                    inner.ring.tail.store(new_tail, Ordering::Relaxed);
                }
            }
            false => {
                let total_available = amt_available(head, tail, cap);
                let remaining_to_read = max_read - bytes_read;
                let mut batch_size = MAX_BATCH_SIZE.min(total_available).min(remaining_to_read);
                if head == tail {
                    batch_size = 0;
                }
                if batch_size == 0 {
                    inner.ring.reader.register(cx.waker());
                    return task::Poll::Pending;
                }
                bytes_read += inner.ring.read_region(tail, &mut buf[..batch_size]);
                let mut new_tail = tail + batch_size;
                if new_tail == cap {
                    new_tail = 0;
                    inner.ring.wrapped.store(false, Ordering::Relaxed);
                }
                inner.ring.tail.store(new_tail, Ordering::Relaxed);
            }
        }

        if bytes_read > 0 {
            inner.ring.writer.wake();
            return task::Poll::Ready(Ok(bytes_read));
        }

        inner.ring.reader.register(cx.waker());
        task::Poll::Pending
    }
}

/// AsyncWrite implementation for the TX side of the ringbuf..
impl AsyncWrite for Tx<u8> {
    // #[instrument]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
        mut buf: &[u8],
    ) -> task::Poll<Result<usize, io::Error>> {
        info!("<******* [ [ [ start write ] ] ] *******>");
        let mut inner = self.get_mut();
        let original_buf_len = buf.len();
        let mut head = inner.ring.head.load(Ordering::SeqCst);
        let mut tail = inner.ring.tail.load(Ordering::SeqCst);
        let mut wrapped = inner.ring.wrapped.load(Ordering::SeqCst);
        let mut cap = inner.ring.capacity;
        let mut bytes_written = 0;

        let mut amt_available = |h: usize, t: usize, c: usize| match t <= h {
            true => (c - h) + t,
            false => t - h,
        };

        match wrapped {
            true => {
                let available = amt_available(head, tail, cap);
                let mut batch_size = MAX_BATCH_SIZE.min(available).min(original_buf_len);
                if head == tail {
                    batch_size = 0;
                }

                if batch_size == 0 {
                    inner.ring.writer.register(cx.waker());
                    return task::Poll::Pending;
                }
                bytes_written += inner.ring.write_region(head, &buf[..batch_size]);
                inner.ring.head.store(head + batch_size, Ordering::Relaxed);
            }
            false => {
                let available = amt_available(head, tail, cap);
                let mut batch_size = MAX_BATCH_SIZE
                    .min(available)
                    .min(original_buf_len)
                    .min(cap - head);

                if batch_size == 0 {
                    inner.ring.writer.register(cx.waker());
                    return task::Poll::Pending;
                }
                bytes_written += inner.ring.write_region(head, &buf[..batch_size]);
                let mut remaining = (original_buf_len - bytes_written);
                remaining = (available - batch_size).min(original_buf_len - bytes_written);
                if remaining > 0 {
                    bytes_written += inner
                        .ring
                        .write_region(0, &buf[batch_size..batch_size + remaining]);
                    inner.ring.wrapped.store(true, Ordering::Relaxed);
                    inner.ring.head.store(remaining, Ordering::Relaxed);
                } else {
                    let mut new_head = head + batch_size;
                    if new_head == cap {
                        new_head = 0;
                        inner.ring.wrapped.store(true, Ordering::Relaxed);
                    }
                    inner.ring.head.store(new_head, Ordering::Relaxed);
                }
            }
        }

        if bytes_written > 0 {
            inner.ring.reader.wake();
            return task::Poll::Ready(Ok(bytes_written));
        }

        inner.ring.writer.register(cx.waker());
        task::Poll::Pending
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), io::Error>> {
        task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Result<(), io::Error>> {
        task::Poll::Ready(Ok(()))
    }
}

unsafe impl<T> Send for Tx<T> {}
unsafe impl<T> Send for Rx<T> {}

unsafe impl<'a, T, IO: AsyncWrite + Unpin + Send> Send for WriteIOFuture<'a, T, IO> {}
unsafe impl<'a, T, IO: AsyncWrite + Unpin + Send> Sync for WriteIOFuture<'a, T, IO> {}

unsafe impl<'a, T, IO: AsyncRead + Unpin + Send> Send for ReadIOFuture<'a, T, IO> {}
unsafe impl<'a, T, IO: AsyncRead + Unpin + Send> Sync for ReadIOFuture<'a, T, IO> {}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Error;
    use std::time::Duration;
    use tokio::{
        join,
        net::{TcpListener, TcpStream},
        prelude::*,
        stream::StreamExt,
        time,
    };
    use tracing::{error, info, info_span, Level};
    use tracing_futures::Instrument;

    fn make_slice<'a>(ptr: *mut u8, offset: usize, len: usize) -> &'a [u8] {
        let ptr = unsafe { ptr.add(offset) };
        let slice = unsafe { slice::from_raw_parts_mut(ptr, len) };
        slice
    }

    fn setup_tracing() {
        tracing_subscriber::fmt()
            .with_target(true)
            .with_level(true)
            .with_max_level(Level::DEBUG)
            .try_init();
    }

    #[tokio::test]
    async fn test_safer_use_case() -> Result<(), Error> {
        setup_tracing();
        const ADDR: &str = "127.0.0.1:5454";

        let (mut wr, mut re) = Ring::<u8>::create(10);
        let mut stream = TcpListener::bind(ADDR).await?;

        tokio::spawn(async {
            let mut conn = TcpStream::connect(ADDR).await.unwrap();
            let buffy = vec![1, 2, 3, 4, 5, 6, 7, 8];
            let res = conn.write(&buffy).await;

            time::delay_for(Duration::from_secs(2)).await;

            let buffy = vec![1, 2, 3, 4, 5, 6, 7, 8];
            let res = conn.write(&buffy).await;

            time::delay_for(Duration::from_secs(2)).await;
        });

        if let Some(Ok(mut conn)) = stream.next().await {
            let (mut rx, mut tx) = conn.split();
            let (mut write_guard, amt) = wr.get_buffer(20)?;

            let rx_amt = rx.read(&mut *write_guard).await?;
            info!("rx_amt: {}", rx_amt);
            write_guard.2 = rx_amt;
            info!("{:#?}", write_guard);

            drop(write_guard);
            // let amt_read = rx.read(&mut *buf).await?;
            // let amt_read = write_guard.read(&mut rx).await?;

            let (mut read_guard, amt) = re.get_buffer(rx_amt)?;
            read_guard.2 = tx.write(&*read_guard).await?;
            info!("{:#?}", read_guard);
            // let amt_wrote = read_guard.write(&mut tx).await?;
            drop(read_guard);

            let (mut write_guard, amt) = wr.get_buffer(20)?;

            info!("{:#?}", write_guard);
            let rx_amt = rx.read(&mut *write_guard).await?;
            info!("rx_amt: {}", rx_amt);
            write_guard.2 = rx_amt;
            info!("{:#?}", write_guard);
            drop(write_guard);

            let (mut write_guard, amt) = wr.get_buffer(20)?;

            info!("{:#?}", write_guard);
            let rx_amt = rx.read(&mut *write_guard).await?;
            info!("rx_amt: {}", rx_amt);
            write_guard.2 = rx_amt;
            info!("{:#?}", write_guard);
            drop(write_guard);

            let (mut read_guard, amt) = re.get_buffer(rx_amt)?;
            read_guard.2 = tx.write(&*read_guard).await?;
            info!("{:#?}", read_guard);
        }
        info!("re: {:#?}", re);
        info!("wr: {:#?}", wr);

        Ok(())
    }

    #[tokio::test]
    async fn ringbuf_get_write_region() -> Result<(), Error> {
        setup_tracing();
        const ADDR: &str = "127.0.0.1:5454";
        let (mut wr, mut re) = Ring::<u8>::create(10);

        let (mut buf, amt) = wr.get_buffer(20)?;
        info!("Allocated {} mutable bytes for use in reads..", amt);
        let mut stream = TcpListener::bind(ADDR).await?;

        debug!(?buf);
        tokio::spawn(async {
            let mut conn = TcpStream::connect(ADDR).await.unwrap();
            let buffy = vec![1, 2, 3, 4, 5, 6, 7, 8];
            let res = conn.write(&buffy).await.unwrap();
            info!("Wrote {} bytes to socket..", res);
        });

        if let Some(Ok(mut conn)) = stream.next().await {
            let (mut rx, mut tx) = conn.split();
            let amt_read = rx.read(&mut *buf).await?;
            // assert_eq!(amt, amt_read);
            info!("Actually read {} bytes.. data: {:#?}", amt_read, buf);

            drop(buf);
            debug!("Re-after-drop: {:#?}", re);

            let (outbuf, amt) = re.get_buffer(amt_read)?;
            info!("Allocated {} bytes for use in writes..", amt);
            let res = tx.write(&*outbuf).await?;
            assert_eq!(amt, res);
            info!("Actually wrote {} bytes.. data: {:#?}", res, &*outbuf);
            drop(outbuf);
            debug!("Re-after-drop: {:#?}", re);
        }
        Ok(())
    }

    // #[tokio::test]
    async fn ringbuf_test_misc() -> Result<(), Error> {
        // setup_tracing();
        let (mut wr, mut re) = Ring::<u8>::create(20);
        let src_buf = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut dst_buf = vec![0; 10];

        // Write 8 entries
        let res = wr
            .write(&src_buf)
            .instrument(info_span!("write", id = 1))
            .await?;
        assert_eq!(res, 8);
        info!(
            ">>> Head: {}, Tail: {}, Wrote: {}",
            wr.head(),
            wr.tail(),
            res
        );

        // Write 5 entries
        let sbuf = vec![66, 66, 66, 66, 66];
        let res = wr
            .write(&sbuf)
            .instrument(info_span!("write", id = 2))
            .await?;
        assert_eq!(res, 5);
        info!(
            ">>> Head: {}, Tail: {}, Wrote: {}",
            wr.head(),
            wr.tail(),
            res
        );

        // Read 10 entries
        let read_amt = re
            .read(&mut dst_buf)
            .instrument(info_span!("read", id = 3))
            .await?;
        assert_eq!(read_amt, 10);
        info!(
            ">>> Head: {}, Tail: {}, Read: {}",
            re.head(),
            re.tail(),
            read_amt
        );

        // Write 4 entries
        let newbuf = vec![12, 12, 12, 12];
        let res = wr
            .write(&newbuf)
            .instrument(info_span!("write", id = 4))
            .await?;
        info!(
            ">>> Head: {}, Tail: {}, Wrote: {}",
            wr.head(),
            wr.tail(),
            res
        );

        // Read 10 entries
        let read_amt = re
            .read(&mut dst_buf)
            .instrument(info_span!("read", id = 5))
            .await?;
        dbg!(read_amt);
        info!(
            ">>> Head: {}, Tail: {}, Read: {}",
            re.head(),
            re.tail(),
            read_amt
        );

        // Write ... 8?
        let res = time::timeout(
            Duration::from_secs(10),
            wr.write(&src_buf).instrument(info_span!("write", id = 6)),
        )
        .await??;
        dbg!(&res);
        assert_eq!(res, 8);
        info!(
            ">>> Head: {}, Tail: {}, Wrote: {}",
            wr.head(),
            wr.tail(),
            res
        );

        let read_amt = re
            .read(&mut dst_buf)
            .instrument(info_span!("read", id = 7))
            .await?;
        // assert_eq!(read_amt, 10);
        info!(
            ">>> head: {}, tail: {}, read: {}",
            re.head(),
            re.tail(),
            read_amt
        );

        let read_amt = re
            .read(&mut dst_buf)
            .instrument(info_span!("read", id = 8))
            .await?;
        // assert_eq!(read_amt, 10);
        info!(
            ">>> head: {}, tail: {}, read: {}",
            re.head(),
            re.tail(),
            read_amt
        );
        tokio::spawn(async move {
            time::delay_for(Duration::from_secs(4)).await;
            let newbuf = vec![88, 88, 88];
            let res = wr
                .write(&newbuf)
                .instrument(info_span!("write", id = 9))
                .await
                .unwrap();
            info!(
                ">>> Head: {}, Tail: {}, Wrote: {}",
                wr.head(),
                wr.tail(),
                res
            );

            dbg!(&wr);
        });

        let read_amt = re
            .read(&mut dst_buf)
            .instrument(info_span!("read", id = 10))
            .await?;
        // assert_eq!(read_amt, 10);
        info!(
            ">>> head: {}, tail: {}, read: {}",
            re.head(),
            re.tail(),
            read_amt
        );

        drop(re);
        Ok(())
    }

    #[tokio::test]
    async fn ring_max_write_read_blocked_write() -> Result<(), Error> {
        // setup_tracing();
        let (mut wr, mut re) = Ring::<u8>::create(10);
        let src_buf = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let mut dst_buf = vec![0; 8];

        // Write 8 entries
        let res = wr
            .write(&src_buf)
            .instrument(info_span!("write", id = 1))
            .await?;
        assert_eq!(res, 10);
        debug!(?wr.ring.head, ?wr.ring.tail, ?wr.ring.wrapped);

        // dbg!(wr.get_ring_slab());
        // dbg!(&src_buf);

        // Write 5 entries, even though it's full
        let handle = tokio::spawn(async move {
            // time::delay_for(Duration::from_secs(4)).await;
            let sbuf = vec![66, 66, 66, 66, 66];
            let res = wr
                .write(&sbuf)
                .instrument(info_span!("write", id = 2))
                .await
                .unwrap();
            assert_eq!(res, 5);
            info!("After 2nd write..");
            debug!(?wr.ring.head, ?wr.ring.tail, ?wr.ring.wrapped);
            info!(
                ">>> Head: {}, Tail: {}, Wrote: {}",
                wr.head(),
                wr.tail(),
                res
            );
            dbg!(wr.get_ring_slab());
        });

        // Read X entries
        time::delay_for(Duration::from_secs(4)).await;
        info!("after delay..");
        let read_amt = re
            .read(&mut dst_buf)
            .instrument(info_span!("read", id = 3))
            .await?;
        assert_eq!(read_amt, 8);
        debug!(?re.ring.head, ?re.ring.tail, ?re.ring.wrapped, read_amt);
        // dbg!(&dst_buf);
        // info!("After read.... ");
        // dbg!(re.get_ring_slab());
        handle.await;

        Ok(())
    }

    #[tokio::test(core_threads = 10)]
    async fn ring_simple_back_forth() -> Result<(), Error> {
        // setup_tracing();
        let (mut wr, mut re) = Ring::<u8>::create(1024);
        let src_buf = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let mut dst_buf = vec![0; 11];

        let handle2 = tokio::spawn(async move {
            let mut tail = 0;
            for a in 0..10000 {
                let res = re.read(&mut dst_buf).await.unwrap();
                dbg!(res);
                debug!("round: {} => read buffer[{}]: {:?}", a, res, &dst_buf);
                tail = re.tail();
            }
            info!("ended with tail @ {}", tail);
            // info!("ring: {:#?}", re.get_ring_slab());
        });

        let handle = tokio::spawn(async move {
            let mut head = 0;
            for a in 0..10000 {
                let res = wr.write(&src_buf).await.unwrap();
                dbg!(res);
                debug!("round: {} => wrote amt: [{}]", a, res);
                head = wr.head();
            }
            info!("ended with head @ {}", head);
            // info!("ring: {:#?}", wr.get_ring_slab());
        });

        // let (res, res2) = join!(handle, handle2);
        handle2.await;

        Ok(())
    }

    #[tokio::test]
    async fn test_simple_ring() -> Result<(), Error> {
        // setup_tracing();
        let (mut wr, mut re) = Ring::<u8>::create(10);
        let src_buf = vec![1, 2, 3, 4, 5, 6];
        let mut dst_buf = vec![0; 4];

        // W + 6
        let res = wr.write(&src_buf).await?;
        dbg!(res);
        debug!("Wrote: {}", res);
        assert_eq!(res, 6);
        assert_eq!(wr.head(), 6);
        assert_eq!(wr.tail(), 0);
        assert!(!wr.ring.wrapped());
        debug!(
            "[head: {}, tail: {}] ring: {:#?}",
            wr.head(),
            wr.tail(),
            &wr.get_ring_slab()
        );

        // re + 4
        let res = re.read(&mut dst_buf).await?;
        debug!("[amt_read: {} ] Read buffer: {:?}", res, &dst_buf);
        assert_eq!(res, 4);
        assert_eq!(re.head(), 6);
        assert_eq!(re.tail(), 4);
        assert!(!re.ring.wrapped());
        assert_eq!(dst_buf, [1, 2, 3, 4]);
        debug!(
            "[head: {}, tail: {}] ring: {:#?}",
            re.head(),
            re.tail(),
            &re.get_ring_slab()
        );

        // re + 4
        let res = re.read(&mut dst_buf).await?;
        debug!("[amt_read: {} ] Read buffer: {:?}", res, &dst_buf);
        assert_eq!(res, 2);
        assert_eq!(re.head(), 6);
        assert_eq!(re.tail(), 6);
        assert!(!re.ring.wrapped());
        assert_eq!(dst_buf, [5, 6, 3, 4]);
        debug!(
            "[head: {}, tail: {}] ring: {:#?}",
            re.head(),
            re.tail(),
            &re.get_ring_slab()
        );

        // W + 6
        let res = wr.write(&src_buf).await?;
        dbg!(res);
        debug!("Wrote: {}", res);
        assert_eq!(res, 6);
        assert_eq!(wr.head(), 2);
        assert_eq!(wr.tail(), 6);
        assert!(wr.ring.wrapped());
        debug!(
            "[head: {}, tail: {}] ring: {:#?}",
            wr.head(),
            wr.tail(),
            &wr.get_ring_slab()
        );

        // R + 4
        let res = re.read(&mut dst_buf).await?;
        debug!("[amt_read: {} ] Read buffer: {:?}", res, &dst_buf);
        assert_eq!(res, 4);
        assert_eq!(re.head(), 2);
        assert_eq!(re.tail(), 0);
        assert!(!re.ring.wrapped());
        assert_eq!(dst_buf, [1, 2, 3, 4]);
        debug!(
            "[head: {}, tail: {}] ring: {:#?}",
            re.head(),
            re.tail(),
            &re.get_ring_slab()
        );
        let res = re.read(&mut dst_buf).await?;
        debug!("[amt_read: {} ] Read buffer: {:?}", res, &dst_buf);
        assert_eq!(res, 2);
        assert_eq!(re.head(), 2);
        assert_eq!(re.tail(), 2);
        assert!(!re.ring.wrapped());
        assert_eq!(dst_buf, [5, 6, 3, 4]);

        debug!(
            "[head: {}, tail: {}] ring: {:#?}",
            re.head(),
            re.tail(),
            &re.get_ring_slab()
        );

        let handle = tokio::spawn(async move {
            time::delay_for(Duration::from_secs(4)).await;
            let res = wr.write(&src_buf).await.unwrap();
            dbg!(res);
            debug!("Wrote: {}", res);
            assert_eq!(res, 6);
            assert_eq!(wr.head(), 8);
            assert_eq!(wr.tail(), 2);
            assert!(!wr.ring.wrapped());
            debug!(
                "[head: {}, tail: {}] ring: {:#?}",
                wr.head(),
                wr.tail(),
                &wr.get_ring_slab()
            );
        });

        let mut last_buf = vec![0; 7];
        let res = re.read(&mut last_buf).await?;
        debug!("**** {:#?} ****", &last_buf);
        assert_eq!(res, 6);
        assert_eq!(re.head(), 8);
        assert_eq!(re.tail(), 8);
        assert!(!re.ring.wrapped());
        debug!(
            "[head: {}, tail: {}] ring: {:#?}",
            re.head(),
            re.tail(),
            &re.get_ring_slab()
        );
        handle.await;
        Ok(())
    }

    #[test]
    fn test_bitwise() {
        // setup_tracing();
        let val = AtomicU8::new(0);
        // let res = val.load(Ordering::SeqCst).into() & RingFlags::RING_FULL;
        let res = RingFlags::from_bits(val.load(Ordering::SeqCst))
            .unwrap()
            .contains(RingFlags::RING_FULL);
        let flag2 = RingFlags::from_bits(0b1101).unwrap();
        let flag1 = RingFlags::RING_FULL;
        let flags = flag1 | flag2;
        // debug!(?flags);
        let flags2 = flag1 & flag2;
        // debug!(?flags2);
        dbg!(flags, flags2, flag1, flag2);
        let res =
            RingFlags::from_bits(0b1111).unwrap() & RingFlags::READ_DROP == RingFlags::READ_DROP;
        // let res2 = RingFlags::READ_DROP & RingFlags::READ_DROP;

        // debug!(?res);
        // debug!(?res2);
    }
}
