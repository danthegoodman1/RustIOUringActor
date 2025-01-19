# RustIOUringActor

Using the `IOUringAPI` a `IOUringActor` is launched that can be used async from multiple threads via the API. This allows for easy use of io_uring from async environments. This also allows for efficient use of a shared ring.

The API supports `*Direct` commands for usage with direct IO in the form of operating per-block with `AlignedBuffer` (see the `created_aligned_buffer!` macro), as well as non-direct methods. You should use the correct one based on how you set up the file descriptor.
