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
2025-01-25T18:52:46.313058Z DEBUG write_block: src/io_uring.rs:255: close time.busy=2.21µs time.idle=699µs
2025-01-25T18:52:46.313153Z DEBUG read_block: src/io_uring.rs:288: close time.busy=875ns time.idle=83.0µs

2025-01-25T18:52:46.313323Z DEBUG write_block: src/io_uring.rs:255: close time.busy=751ns time.idle=161µs
2025-01-25T18:52:46.313398Z DEBUG read_block: src/io_uring.rs:288: close time.busy=750ns time.idle=66.7µs

2025-01-25T18:52:46.313505Z DEBUG trim_block: src/io_uring.rs:310: close time.busy=791ns time.idle=101µs
2025-01-25T18:52:46.313656Z DEBUG read_block: src/io_uring.rs:288: close time.busy=708ns time.idle=144µs
```

First write again always seems to be slower.
