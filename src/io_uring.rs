// Trait for aligned pages to implement
#[cfg(target_os = "linux")]
pub trait AlignedBuffer: Send + Sync {
    fn as_ptr(&self) -> *const u8;
    fn as_mut_ptr(&mut self) -> *mut u8;
    fn len(&self) -> usize;

    /// Copies data from a slice into the buffer starting at offset 0.
    /// Returns the number of bytes copied.
    /// Will truncate the input if it's longer than the buffer.
    fn copy_from_slice(&mut self, data: &[u8]) -> usize {
        let copy_len = std::cmp::min(self.len(), data.len());
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.as_mut_ptr(), copy_len);
        }
        copy_len
    }

    /// Returns a slice of the buffer's contents
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    /// Returns a mutable slice of the buffer's contents
    fn as_slice_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }
}

impl std::fmt::Debug for dyn AlignedBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AlignedBuffer")
    }
}

/**
 * Macro to create an aligned page type for a given size
 *
 * This macro creates a new struct with the given name and alignment,
 * and implements the AlignedBuffer trait for it.
 *
 * You might need to import the AlignedBuffer trait to use this macro.
 */
#[macro_export]
#[cfg(target_os = "linux")]
macro_rules! create_aligned_page {
    ($name:ident, $alignment:expr) => {
        #[repr(align($alignment))]
        pub struct $name<const N: usize>(pub [u8; N]);

        impl<const N: usize> AlignedBuffer for $name<N> {
            fn as_ptr(&self) -> *const u8 {
                self.0.as_ptr()
            }
            fn as_mut_ptr(&mut self) -> *mut u8 {
                self.0.as_mut_ptr()
            }
            fn len(&self) -> usize {
                self.0.len()
            }
        }
    };
}

#[cfg(target_os = "linux")]
mod linux_impl {
    use std::{collections::VecDeque, marker::PhantomData, os::fd::AsRawFd};

    use super::AlignedBuffer;
    use flume::{Receiver, Sender, TryRecvError};
    use io_uring::{opcode, IoUring};
    use tokio::task::yield_now;
    use tracing::{debug, error, info, instrument, trace};

    #[derive(Debug)]
    pub enum IOUringActorCommand {
        // Non-direct
        Read {
            offset: u64,
            size: usize,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },
        Write {
            offset: u64,
            buffer: Vec<u8>,
            fsync: bool,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },

        // Direct
        ReadBlockDirect {
            offset: u64,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },
        WriteBlockDirect {
            offset: u64,
            data: Vec<u8>,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },

        // Other commands
        TrimBlock {
            offset: u64,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },
    }

    impl IOUringActorCommand {
        pub fn sender(&self) -> &Sender<Result<IOUringActorResponse, std::io::Error>> {
            match self {
                IOUringActorCommand::Read { sender, .. } => sender,
                IOUringActorCommand::Write { sender, .. } => sender,
                IOUringActorCommand::ReadBlockDirect { sender, .. } => sender,
                IOUringActorCommand::WriteBlockDirect { sender, .. } => sender,
                IOUringActorCommand::TrimBlock { sender, .. } => sender,
            }
        }
    }

    #[derive(Debug)]
    pub enum IOUringActorResponse {
        // Non-direct
        Read(Vec<u8>),
        Write,

        // Direct
        ReadBlockDirect(Box<dyn AlignedBuffer>),
        WriteBlockDirect,

        // Other responses
        TrimBlock,
    }

    pub struct IOUringAPI<const BLOCK_SIZE: usize, T: AlignedBuffer> {
        sender: Sender<IOUringActorCommand>,
        _phantom: PhantomData<T>,
    }

    impl<const BLOCK_SIZE: usize, T: AlignedBuffer + 'static> IOUringAPI<BLOCK_SIZE, T> {
        pub async fn new(
            fd: std::fs::File,
            ring: IoUring,
            aligned_buffer_factory: impl Fn() -> Box<dyn AlignedBuffer> + Send + Sync + 'static + Copy,
            channel_size: usize,
        ) -> std::io::Result<Self> {
            let (sender, receiver) = match channel_size {
                0 => flume::unbounded(),
                _ => flume::bounded(channel_size),
            };

            let uring_fd = io_uring::types::Fd(fd.as_raw_fd());

            let actor = IOUringActor::<BLOCK_SIZE, T> {
                _fd: fd,
                fd: uring_fd,
                ring,
                receiver,
                _phantom: PhantomData,
            };

            tokio::spawn(actor.run(channel_size, aligned_buffer_factory));

            Ok(Self {
                sender,
                _phantom: PhantomData,
            })
        }

        /// Read uses non-direct IO to read a block from the device.
        #[instrument(skip_all, level = "debug")]
        pub async fn read(&self, offset: u64, size: usize) -> std::io::Result<Vec<u8>> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::Read {
                    offset,
                    size,
                    sender,
                })
                .await
                .unwrap();
            let response = receiver.recv_async().await.unwrap();
            match response {
                Ok(IOUringActorResponse::Read(result)) => Ok(result),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        /// write uses non-direct IO to write a buffer to the device.
        /// write can optionally call fsync after the write operation, which is another io_uring operation.
        /// If either of these operations fail, the operation will return an error, even if the write operation succeeded.
        #[instrument(skip(self, offset, buffer), level = "debug")]
        pub async fn write(
            &self,
            offset: u64,
            buffer: Vec<u8>,
            fsync: bool,
        ) -> std::io::Result<()> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::Write {
                    offset,
                    buffer,
                    fsync,
                    sender,
                })
                .await
                .unwrap();
            let response = receiver.recv_async().await.unwrap();
            match response {
                Ok(IOUringActorResponse::Write) => Ok(()),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        /// write_block uses direct IO to write a block to the device.
        #[instrument(skip_all, level = "debug")]
        pub async fn write_block(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
            if data.len() > BLOCK_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Buffer too large, must be less than or equal to BLOCK_SIZE",
                ));
            }
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::WriteBlockDirect {
                    offset,
                    data: data.to_vec(),
                    sender,
                })
                .await
                .unwrap();
            let response = receiver.recv_async().await.unwrap();
            match response {
                Ok(IOUringActorResponse::WriteBlockDirect) => Ok(()),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        /// read_block uses direct IO to read a block from the device.
        /// Returns a copy of the data read, always a BLOCK_SIZE length.
        #[instrument(skip_all, level = "debug")]
        pub async fn read_block(&self, offset: u64) -> std::io::Result<Vec<u8>> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::ReadBlockDirect { offset, sender })
                .await
                .unwrap();
            let response = receiver.recv_async().await.unwrap();
            match response {
                Ok(IOUringActorResponse::ReadBlockDirect(buffer)) => Ok(buffer.as_slice().to_vec()),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        pub async fn trim_block(&self, offset: u64) -> std::io::Result<()> {
            todo!()
        }
    }

    pub struct IOUringActor<const BLOCK_SIZE: usize, T: AlignedBuffer> {
        _fd: std::fs::File, // Keeps the file descriptor alive
        fd: io_uring::types::Fd,
        ring: IoUring,
        receiver: Receiver<IOUringActorCommand>,
        _phantom: PhantomData<T>,
    }

    impl<const BLOCK_SIZE: usize, T: AlignedBuffer> IOUringActor<BLOCK_SIZE, T> {
        /// run starts the actor loop, that will flip between consuming commands from the receiver,
        /// submitting them to the ring, polling completions, and sending responses back to the caller.
        async fn run(
            mut self,
            queue_size: usize,
            aligned_buffer_factory: impl Fn() -> Box<dyn AlignedBuffer> + Send + Sync + 'static + Copy,
        ) {
            debug!("Starting actor loop");
            let mut responders: VecDeque<(
                IOUringActorCommand,           // Need command to keep buffer in scope
                (IOUringActorResponse, usize), // How many following operations we must wait for, any of which can error, but only the first can provide the successful response
            )> = VecDeque::with_capacity(queue_size);
            loop {
                // TODO: ensure that we don't take if the responders queue is full
                let command = match self.receiver.try_recv() {
                    Ok(command) => Some(command),
                    Err(e) => match e {
                        TryRecvError::Disconnected => {
                            info!("Actor disconnected, exiting");
                            return;
                        }
                        TryRecvError::Empty => {
                            trace!("Empty queue");
                            None
                        }
                    },
                };

                // If we got a command, let's submit it and add it to the responders queue
                if let Some(command) = command {
                    trace!("Submitting command");
                    if let Ok(result) = self.queue_command(&command, aligned_buffer_factory).await {
                        responders.push_back((command, result));
                        self.ring.submit().expect("Failed to submit command");
                        trace!("Command submitted");
                    }
                    // We already sent the error back to the caller, so we can continue
                } else {
                    trace!("No command to submit");
                }

                // If we have responders, let's check if the tasks are completed
                while self.ring.completion().len() > 0 && responders.len() > 0 {
                    trace!(
                        "Checking for completion {:?} {:?}",
                        responders.len(),
                        self.ring.completion().len()
                    );
                    let (command, (response, wait_for_extra)) = responders.pop_front().unwrap();
                    let sender = command.sender();
                    // We finally got an entry, let's take it
                    let result = self.ring.completion().next();
                    trace!("result: {:?}", result);
                    match result {
                        Some(cqe) => {
                            let result = if cqe.result() < 0 {
                                trace!("Completion error: {:?}", cqe.result());
                                Err(std::io::Error::from_raw_os_error(-cqe.result()))
                            } else {
                                trace!("Completion success: {:?}", cqe.result());
                                Ok(response)
                            };

                            // Definitely a better way to write this
                            if wait_for_extra > 0 {
                                trace!("Waiting for extra completion");
                                for _ in 0..wait_for_extra {
                                    // First we wait for a completion in the ring
                                    while self.ring.completion().is_empty() {
                                        yield_now().await; // yield to the scheduler to avoid busy-waiting
                                    }

                                    let extra_result = self.ring.completion().next();
                                    let extra_result = match extra_result {
                                        Some(cqe) => {
                                            let result = if cqe.result() < 0 {
                                                Err(std::io::Error::from_raw_os_error(
                                                    -cqe.result(),
                                                ))
                                            } else {
                                                Ok(())
                                            };
                                            result
                                        }
                                        None => {
                                            error!(
                                                "No completion queue entry found for extra operation"
                                            );
                                            Err(std::io::Error::new(
                                                std::io::ErrorKind::Other,
                                                "No completion queue entry found for extra operation",
                                            ))
                                        }
                                    };
                                    if let Err(e) = extra_result {
                                        let _ = sender.send_async(Err(e)).await;
                                    }
                                }
                            }

                            // Now we can await after we're done with the completion queue since it's not Send,
                            // and we don't care about the result of the send
                            let _ = sender.send_async(result).await;
                            // match sender.try_send(result) {
                            //     Ok(()) => debug!("Sent response"),
                            //     Err(e) => error!("Failed to send response: {:?}", e),
                            // }
                        }
                        None => {
                            // TODO: better log on this
                            error!("No completion queue entry found");
                            let _ = sender
                                .send_async(Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "No completion queue entry found",
                                )))
                                .await;
                        }
                    }
                }
                yield_now().await; // so we don't lock up the thread
            }
        }

        async fn queue_command(
            &mut self,
            command: &IOUringActorCommand,
            aligned_buffer_factory: impl Fn() -> Box<dyn AlignedBuffer> + Send + Sync + 'static,
        ) -> std::io::Result<(
            IOUringActorResponse,
            usize, // How many following operations we must wait for, any of which can error, but only the first can provide the successful response
        )> {
            match command {
                IOUringActorCommand::Read {
                    offset,
                    size,
                    sender,
                } => {
                    trace!("Read: {:?}", offset);
                    match self.handle_read(*offset, *size).await {
                        Ok(result) => Ok((IOUringActorResponse::Read(result), 0)),
                        Err(e) => {
                            debug!("handle_read error: {:?}", e);
                            let _ = sender.send_async(Err(e)).await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "handle_read error",
                            ))
                        }
                    }
                }

                IOUringActorCommand::Write {
                    offset,
                    buffer,
                    fsync,
                    sender,
                } => {
                    trace!("Write: {:?}", offset);
                    match self.handle_write(*offset, &buffer, *fsync).await {
                        Ok(()) => Ok((
                            IOUringActorResponse::Write,
                            match fsync {
                                true => 1,
                                false => 0,
                            },
                        )),
                        Err(e) => {
                            debug!("handle_write error: {:?}", e);
                            let _ = sender.send_async(Err(e)).await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "handle_write error",
                            ))
                        }
                    }
                }

                IOUringActorCommand::TrimBlock { offset, sender } => {
                    debug!("TrimBlock: {:?}", offset);
                    todo!()
                }

                IOUringActorCommand::ReadBlockDirect { offset, sender } => {
                    trace!("ReadBlockDirect: {:?}", offset);
                    let mut buffer = aligned_buffer_factory();

                    match self.handle_read_direct(*offset, buffer.as_mut()).await {
                        Ok(()) => Ok((IOUringActorResponse::ReadBlockDirect(buffer), 0)),
                        Err(e) => {
                            debug!("handle_read_direct error: {:?}", e);
                            let _ = sender.send_async(Err(e)).await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "handle_read_direct error",
                            ))
                        }
                    }
                }

                IOUringActorCommand::WriteBlockDirect {
                    offset,
                    data,
                    sender,
                } => {
                    trace!("WriteBlockDirect: {:?}", offset);
                    let mut buffer = aligned_buffer_factory();
                    // Copy the data into the aligned buffer
                    buffer.copy_from_slice(&data);

                    match self.handle_write_direct(*offset, buffer.as_mut()).await {
                        Ok(()) => Ok((IOUringActorResponse::WriteBlockDirect, 0)),
                        Err(e) => {
                            debug!("handle_write_direct error: {:?}", e);
                            let _ = sender.send_async(Err(e)).await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "handle_write_direct error",
                            ))
                        }
                    }
                }
            }
        }

        async fn handle_read(&mut self, offset: u64, size: usize) -> std::io::Result<Vec<u8>> {
            let mut buffer = vec![0u8; size];
            let read_e = opcode::Read::new(self.fd, buffer.as_mut_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
                .user_data(0x42);

            unsafe {
                self.ring
                    .submission()
                    .push(&read_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            Ok(buffer)
        }

        /// Reads a block from the device into the given buffer.
        async fn handle_read_direct(
            &mut self,
            offset: u64,
            buffer: &mut dyn AlignedBuffer,
        ) -> std::io::Result<()> {
            let read_e = opcode::Read::new(self.fd, buffer.as_mut_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
                .user_data(0x42);

            unsafe {
                self.ring
                    .submission()
                    .push(&read_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            Ok(())
        }

        /// Writes data, returning after submission
        async fn handle_write(
            &mut self,
            offset: u64,
            buffer: &[u8],
            fsync: bool,
        ) -> std::io::Result<()> {
            // Submit the write
            let write_e = opcode::Write::new(self.fd, buffer.as_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
                .user_data(0x43);

            unsafe {
                self.ring
                    .submission()
                    .push(&write_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            if !fsync {
                return Ok(());
            }

            // Add an fsync operation
            let fsync_e = opcode::Fsync::new(self.fd).build().user_data(0x44);

            unsafe {
                self.ring
                    .submission()
                    .push(&fsync_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            Ok(())
        }

        /// Writes a block to the device from the given buffer.
        async fn handle_write_direct(
            &mut self,
            offset: u64,
            buffer: &mut dyn AlignedBuffer,
        ) -> std::io::Result<()> {
            let write_e = opcode::Write::new(self.fd, buffer.as_ptr(), buffer.len() as _)
                .offset(offset)
                .build()
                .user_data(0x43);

            unsafe {
                self.ring
                    .submission()
                    .push(&write_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            Ok(())
        }

        /// Deallocates the block at the given offset using `FALLOC_FL_PUNCH_HOLE`, which creates a hole in the file
        /// and releases the associated storage space. On SSDs this triggers the TRIM command for better performance
        /// and wear leveling.
        async fn handle_trim(&mut self, offset: u64) -> std::io::Result<()> {
            // FALLOC_FL_PUNCH_HOLE (0x02) | FALLOC_FL_KEEP_SIZE (0x01)
            const PUNCH_HOLE: i32 = 0x02 | 0x01;

            let trim_e = opcode::Fallocate::new(self.fd, BLOCK_SIZE as u64)
                .offset(offset)
                .mode(PUNCH_HOLE)
                .build()
                .user_data(0x44);

            unsafe {
                self.ring
                    .submission()
                    .push(&trim_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            Ok(())
        }
    }
}

#[cfg(target_os = "linux")]
pub use linux_impl::*;

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use io_uring::IoUring;
    use linux_impl::IOUringAPI;
    use tracing::Level;
    use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, Layer};

    use super::*;
    use std::{os::unix::fs::OpenOptionsExt, sync::Once};

    static LOGGER_ONCE: Once = Once::new();

    const BLOCK_SIZE: usize = 4096;

    fn create_logger() {
        LOGGER_ONCE.call_once(|| {
            let subscriber = tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .compact()
                    .with_file(true)
                    .with_line_number(true)
                    .with_span_events(FmtSpan::CLOSE)
                    .with_target(false)
                    .with_filter(
                        tracing_subscriber::filter::Targets::new().with_default(Level::DEBUG),
                    ),
            );

            tracing::subscriber::set_global_default(subscriber).unwrap();
        });
    }

    // Test to ensure AlignedBuffer implements Send + Sync trait
    #[tokio::test]
    async fn test_aligned_buffer_is_send() {
        create_aligned_page!(Page4K, 4096);
        fn assert_send<T: Send + Sync>() {}
        assert_send::<Page4K<4096>>();
    }

    #[tokio::test]
    async fn test_io_uring_read_write() -> Result<(), Box<dyn std::error::Error>> {
        create_logger();
        // Create a shared io_uring instance
        let ring = IoUring::new(128)?;

        // Create a temporary file path
        // let temp_file = tempfile::NamedTempFile::new()?;
        let temp_path = "blah.test";
        println!("temp_path: {:?}", temp_path);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            // .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
            .open(temp_path)?;

        println!("fd: {:?}", file);

        create_aligned_page!(Page4K, 4096); // test creating an aliged page with a macro

        // Create a new device instance
        let api = IOUringAPI::<BLOCK_SIZE, Page4K<4096>>::new(
            file,
            ring,
            || Box::new(Page4K([0u8; 4096])),
            128,
        )
        .await?;

        // Test data
        // let mut write_data = [0u8; 1033];
        let hello = b"Hello, world!\n";
        // write_data[..hello.len()].copy_from_slice(hello);

        // Write test (without fsync)
        api.write(0, hello.to_vec(), false).await.unwrap();

        // Read test
        println!("Reading");
        let result = api.read(0, 14).await.unwrap();

        // Verify the contents
        let buffer_slice = result.as_slice();
        println!("Read data: {:?}", &buffer_slice[..hello.len()]);
        println!(
            "Read data (string): {}",
            String::from_utf8_lossy(&buffer_slice[..hello.len()])
        );
        assert_eq!(&buffer_slice[..hello.len()], hello);

        // Write test (with fsync)
        let hello = b"Hello, world again!\n";
        api.write(0, hello.to_vec(), true).await.unwrap();

        // Read test
        let result = api.read(0, 20).await.unwrap();

        // Verify the contents
        let buffer_slice = result.as_slice();
        println!("Read data: {:?}", &buffer_slice[..hello.len()]);
        println!(
            "Read data (string): {}",
            String::from_utf8_lossy(&buffer_slice[..hello.len()])
        );
        assert_eq!(&buffer_slice[..hello.len()], hello);

        Ok(())
    }

    #[tokio::test]
    async fn test_io_uring_direct_read_write() -> Result<(), Box<dyn std::error::Error>> {
        create_logger();
        // Create a shared io_uring instance
        let ring = IoUring::new(128)?;

        // Create a temporary file path
        // let temp_file = tempfile::NamedTempFile::new()?;
        let temp_path = "blah.test";
        println!("temp_path: {:?}", temp_path);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_DIRECT)
            .open(temp_path)?;

        println!("fd: {:?}", file);

        create_aligned_page!(Page4K, 4096); // test creating an aliged page with a macro

        // Create a new device instance
        let api = IOUringAPI::<BLOCK_SIZE, Page4K<4096>>::new(
            file,
            ring,
            || Box::new(Page4K([0u8; 4096])),
            128,
        )
        .await?;

        // Test data
        // let mut write_data = [0u8; 1033];
        let hello = b"Hello, world!\n";
        // write_data[..hello.len()].copy_from_slice(hello);

        // Write test (without fsync)
        api.write_block(0, hello).await.unwrap();

        // Read test
        println!("Reading");
        let result = api.read_block(0).await.unwrap();

        // Verify the contents
        let buffer_slice = result.as_slice();
        println!("Read data: {:?}", &buffer_slice[..hello.len()]);
        println!(
            "Read data (string): {}",
            String::from_utf8_lossy(&buffer_slice[..hello.len()])
        );
        assert_eq!(&buffer_slice[..hello.len()], hello);

        // Write test (with fsync)
        let hello = b"Hello, world again!\n";
        api.write_block(0, hello).await.unwrap();

        // Read test
        let result = api.read_block(0).await.unwrap();

        // Verify the contents
        let buffer_slice = result.as_slice();
        println!("Read data: {:?}", &buffer_slice[..hello.len()]);
        println!(
            "Read data (string): {}",
            String::from_utf8_lossy(&buffer_slice[..hello.len()])
        );
        assert_eq!(&buffer_slice[..hello.len()], hello);

        Ok(())
    }
}
