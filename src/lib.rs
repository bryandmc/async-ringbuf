//! ringbuf
//!
//! This crate (ringbuf) ...
//!
//! - [Reference](crate::module::Reference) description here....
//!

// NOTE: unused direct-write variant
// mod direct;
mod spsc;

pub use spsc::{PopFuture, PushFuture, Ring, Rx, Tx};

// Direct write variant where the actual bytes are written into chunks
// on the ring, instead of just holding pointers to buffers. Not sure if
// it's a good design or not but it does seem to work OK, but has some
// rough edges. It seems like the tactic take in 'spsc' is the more
// common one.
// pub use direct;

#[cfg(test)]
mod tests {
    use super::*;
    use futures::prelude::*;
    use tokio::prelude::*;

    #[derive(Debug, Clone)]
    struct TestStruct {}

    #[test]
    fn smoke_test() {
        let (mut tx, rx) = Ring::create(10);
        let res = tx.try_push(TestStruct {});
        let rx_res = rx.try_pop();
    }

    #[tokio::test]
    async fn async_smoke_test() {
        let (mut tx, rx) = Ring::create(10);
        let res = tx.push_async(Box::new(TestStruct {})).await;
        let rx_res = rx.pop_async().await;
    }
}
