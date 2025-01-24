# RustIOUringActor

Using the `IOUringAPI` a `IOUringActor` is launched that can be used async from multiple threads via the API. This allows for easy use of io_uring from async environments. This also allows for efficient use of a shared ring.

The API supports `*Direct` commands for usage with direct IO in the form of operating per-block with `AlignedBuffer` (see the `created_aligned_buffer!` macro), as well as non-direct methods. You should use the correct one based on how you set up the file descriptor.

It also uses micro-batching, so when it goes to submit more command entries to the queue it will drain the channel up to a certain amount, submit all of the entries, grab all of the results, and send them over the responder channels.

It's fast, ran many times this is a (visibly checked) median-ish run to read and write `Hello, world!\n` (fsync and not, M3 Max, 128GB ram, VS Code dev container, `O_SYNC`):

```
2025-01-21T17:47:56.349957Z DEBUG src/io_uring.rs:228: Starting actor loop
2025-01-21T17:47:56.350043Z DEBUG write: src/io_uring.rs:175: close time.busy=6.00µs time.idle=81.5µs fsync=false
2025-01-21T17:47:56.350110Z DEBUG read: src/io_uring.rs:142: close time.busy=5.75µs time.idle=55.5µs
Read data (string): Hello, world!

2025-01-21T17:47:56.350254Z DEBUG write: src/io_uring.rs:175: close time.busy=5.13µs time.idle=130µs fsync=true
2025-01-21T17:47:56.350282Z DEBUG read: src/io_uring.rs:142: close time.busy=4.83µs time.idle=18.5µs
Read data (string): Hello, world again!
```

## DirectIO

Direct IO is performant as well (M3 Max, 128GB ram, VS Code dev container, `O_DSYNC`):

```
2025-01-24T02:28:48.331978Z DEBUG write_block: src/io_uring.rs:222: close time.busy=9.21µs time.idle=168µs
2025-01-24T02:28:48.332103Z DEBUG read_block: src/io_uring.rs:251: close time.busy=9.50µs time.idle=89.9µs

2025-01-24T02:28:48.332227Z DEBUG write_block: src/io_uring.rs:222: close time.busy=5.54µs time.idle=89.6µs
2025-01-24T02:28:48.332329Z DEBUG read_block: src/io_uring.rs:251: close time.busy=7.87µs time.idle=77.3µs
```

Because of the nature of aligned buffers, we do have to do 2 extra memory copies (one to copy data into aligned buffer, and one to copy out because a `Box<dyn AlignedBuffer>` is returned). However clearly the penalty is not significant with the benefits that direct IO provides
