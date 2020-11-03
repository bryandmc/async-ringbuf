# Async-RingBuf:
[![Actions Status](https://github.com/bryandmc/paxos/workflows/Rust/badge.svg)](https://github.com/bryandmc/async-ringbuf/actions)

## Simple SPSC, async friendly ring buffer:
Very simple design largely inspired by the dpdk ring buffers with additional bookkeeping for waking tasks and being async friendly. This is a very limited, single producer, single consumer ring buffer. It could be modified pretty easily to be a MPSC or potentially even and MPMC but it requires a fair amount more work and affects the performance pretty drastically.

I will likely add other variants as time goes on, as I need them. Feel free to contribute in any way you see fit!
