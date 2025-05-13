# RustIOUringActor

Using the `IOUringAPI` a `IOUringActor` is launched that can be used async from multiple threads via the API. This allows for easy use of io_uring from async environments. This also allows for efficient use of a shared ring.

The API supports `*Direct` commands for usage with direct IO in the form of operating per-block with `AlignedBuffer` (see the `created_aligned_buffer!` macro), as well as non-direct methods. You should use the correct one based on how you set up the file descriptor.

It also uses micro-batching, so when it goes to submit more command entries to the queue it will drain the channel up to a certain amount, submit all of the entries, grab all of the results, and send them over the responder channels.

It's fast, ran many times this is a (visibly checked) median-ish run to read and write `Hello, world!\n` (fsync and not, M3 Max, 128GB ram, VS Code dev container, `O_DSYNC`):

```
2025-01-25T18:52:52.509304Z DEBUG write: src/io_uring.rs:233: close time.busy=2.00µs time.idle=260µs
2025-01-25T18:52:52.509367Z DEBUG read: src/io_uring.rs:209: close time.busy=833ns time.idle=54.4µs

2025-01-25T18:52:52.509532Z DEBUG write: src/io_uring.rs:233: close time.busy=750ns time.idle=156µs
2025-01-25T18:52:52.509577Z DEBUG read: src/io_uring.rs:209: close time.busy=708ns time.idle=38.4µs
```

First write always seems to be slower.

## DirectIO

Direct IO is performant as well (M3 Max, 128GB ram, VS Code dev container, `O_DSYNC`):

```
2025-01-25T18:59:44.143084Z DEBUG write_block: src/io_uring.rs:255: close time.busy=8.75µs time.idle=211µs
2025-01-25T18:59:44.143189Z DEBUG read_block: src/io_uring.rs:288: close time.busy=6.00µs time.idle=71.6µs

2025-01-25T18:59:44.143337Z DEBUG write_block: src/io_uring.rs:255: close time.busy=5.67µs time.idle=121µs
2025-01-25T18:59:44.143412Z DEBUG read_block: src/io_uring.rs:288: close time.busy=4.88µs time.idle=53.9µs

2025-01-25T18:59:44.143518Z DEBUG trim_block: src/io_uring.rs:310: close time.busy=4.88µs time.idle=85.2µs
2025-01-25T18:59:44.143649Z DEBUG read_block: src/io_uring.rs:288: close time.busy=15.7µs time.idle=101µs
```

First write again always seems to be slower.

Probably docker, but sometimes you can get it _insanely fast_:

```
$ cargo test --package io_uring_actor --lib -- io_uring::tests::test_io_uring_direct_read_write --exact --show-output
running 1 test
2025-05-13T02:49:02.394358Z DEBUG src/io_uring.rs:424: Starting actor loop
2025-05-13T02:49:02.394534Z DEBUG write_block: src/io_uring.rs:306: close time.busy=1.75µs time.idle=176µs
2025-05-13T02:49:02.394593Z DEBUG read_block: src/io_uring.rs:351: close time.busy=793ns time.idle=48.2µs
2025-05-13T02:49:02.394699Z DEBUG write_block: src/io_uring.rs:306: close time.busy=666ns time.idle=96.4µs
2025-05-13T02:49:02.394751Z DEBUG get_metadata: src/io_uring.rs:395: close time.busy=708ns time.idle=45.8µs
2025-05-13T02:49:02.394809Z DEBUG read_block: src/io_uring.rs:351: close time.busy=791ns time.idle=46.8µs
2025-05-13T02:49:02.394884Z DEBUG trim_block: src/io_uring.rs:377: close time.busy=499ns time.idle=68.3µs
2025-05-13T02:49:02.394939Z DEBUG read_block: src/io_uring.rs:351: close time.busy=457ns time.idle=49.9µs
test io_uring::tests::test_io_uring_direct_read_write ... ok
```
