# RustIOUringActor

Using the `IOUringAPI` a `IOUringActor` is launched that can be used async from multiple threads via the API. This allows for easy use of io_uring from async environments. This also allows for efficient use of a shared ring.

The API supports `*Direct` commands for usage with direct IO in the form of operating per-block with `AlignedBuffer` (see the `created_aligned_buffer!` macro), as well as non-direct methods. You should use the correct one based on how you set up the file descriptor.

It also uses micro-batching, so when it goes to submit more command entries to the queue it will drain the channel up to a certain amount, submit all of the entries, grab all of the results, and send them over the responder channels.

It's fast, ran many times this is a (visibly checked) median-ish run to read and write `Hello, world!\n` (fsync and not):
```
2025-01-21T17:47:56.349957Z DEBUG src/io_uring.rs:228: Starting actor loop
2025-01-21T17:47:56.350043Z DEBUG write: src/io_uring.rs:175: close time.busy=6.00µs time.idle=81.5µs fsync=false
2025-01-21T17:47:56.350110Z DEBUG read: src/io_uring.rs:142: close time.busy=5.75µs time.idle=55.5µs
Read data (string): Hello, world!

2025-01-21T17:47:56.350254Z DEBUG write: src/io_uring.rs:175: close time.busy=5.13µs time.idle=130µs fsync=true
2025-01-21T17:47:56.350282Z DEBUG read: src/io_uring.rs:142: close time.busy=4.83µs time.idle=18.5µs
Read data (string): Hello, world again!
```
