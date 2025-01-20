# RustIOUringActor

Using the `IOUringAPI` a `IOUringActor` is launched that can be used async from multiple threads via the API. This allows for easy use of io_uring from async environments. This also allows for efficient use of a shared ring.

The API supports `*Direct` commands for usage with direct IO in the form of operating per-block with `AlignedBuffer` (see the `created_aligned_buffer!` macro), as well as non-direct methods. You should use the correct one based on how you set up the file descriptor.

It also uses micro-batching, so when it goes to submit more command entries to the queue it will drain the channel up to a certain amount, submit all of the entries, grab all of the results, and send them over the responder channels.

It's fast, ran many times this is a (visibly checked) median-ish run to read and write `Hello, world!\n`:
```
2025-01-20T18:43:08.193672Z DEBUG write: src/io_uring.rs:162: close time.busy=2.62µs time.idle=151µs
2025-01-20T18:43:08.193756Z DEBUG read: src/io_uring.rs:131: close time.busy=2.12µs time.idle=77.1µs
```
