// Trait for aligned pages to implement
#[cfg(target_os = "linux")]
pub trait AlignedBuffer: Send + Sync {
    fn as_ptr(&self) -> *const u8;
    fn as_mut_ptr(&mut self) -> *mut u8;
    fn len(&self) -> usize;
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }
}

#[cfg(target_os = "linux")]
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

        impl<const N: usize> $name<N> {
            pub fn new_zeroed() -> Self {
                Self([0u8; N])
            }

            pub fn new_with_data(data: &[u8]) -> Self {
                let mut buffer = Self([0u8; N]);
                let len = data.len().min(N);
                buffer.0[..len].copy_from_slice(&data[..len]);
                buffer
            }

            pub fn as_slice(&self) -> &[u8] {
                &self.0
            }
        }

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
    use std::{collections::VecDeque, os::fd::AsRawFd};

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
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },

        // Direct
        ReadBlockDirect {
            offset: u64,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
            buffer: Option<Box<dyn AlignedBuffer>>, // option because we remove it later, not awesome
        },
        WriteBlockDirect {
            offset: u64,
            data: Box<dyn AlignedBuffer>,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },

        // Other commands
        TrimBlock {
            offset: u64,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },

        // Add new Metadata command
        Statx {
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
                IOUringActorCommand::Statx { sender, .. } => sender,
            }
        }
    }

    #[derive(Debug)]
    pub struct statx {
        pub stx_mask: u32,
        pub stx_blksize: u32,
        pub stx_attributes: u64,
        pub stx_nlink: u32,
        pub stx_uid: u32,
        pub stx_gid: u32,
        pub stx_mode: u16,
        __statx_pad1: [u16; 1],
        pub stx_ino: u64,
        pub stx_size: u64,
        pub stx_blocks: u64,
        pub stx_attributes_mask: u64,
        pub stx_atime: statx_timestamp,
        pub stx_btime: statx_timestamp,
        pub stx_ctime: statx_timestamp,
        pub stx_mtime: statx_timestamp,
        pub stx_rdev_major: u32,
        pub stx_rdev_minor: u32,
        pub stx_dev_major: u32,
        pub stx_dev_minor: u32,
        pub stx_mnt_id: u64,
        pub stx_dio_mem_align: u32,
        pub stx_dio_offset_align: u32,
        __statx_pad3: [u64; 12],
    }

    #[derive(Debug)]
    pub struct statx_timestamp {
        pub tv_sec: i64,
        pub tv_nsec: u32,
        __statx_timestamp_pad1: [i32; 1],
    }

    impl From<libc::statx> for statx {
        fn from(src: libc::statx) -> Self {
            Self {
                stx_mask: src.stx_mask,
                stx_blksize: src.stx_blksize,
                stx_attributes: src.stx_attributes,
                stx_nlink: src.stx_nlink,
                stx_uid: src.stx_uid,
                stx_gid: src.stx_gid,
                stx_mode: src.stx_mode,
                __statx_pad1: [0],
                stx_ino: src.stx_ino,
                stx_size: src.stx_size,
                stx_blocks: src.stx_blocks,
                stx_attributes_mask: src.stx_attributes_mask,
                stx_atime: statx_timestamp::from(src.stx_atime),
                stx_btime: statx_timestamp::from(src.stx_btime),
                stx_ctime: statx_timestamp::from(src.stx_ctime),
                stx_mtime: statx_timestamp::from(src.stx_mtime),
                stx_rdev_major: src.stx_rdev_major,
                stx_rdev_minor: src.stx_rdev_minor,
                stx_dev_major: src.stx_dev_major,
                stx_dev_minor: src.stx_dev_minor,
                stx_mnt_id: src.stx_mnt_id,
                stx_dio_mem_align: src.stx_dio_mem_align,
                stx_dio_offset_align: src.stx_dio_offset_align,
                __statx_pad3: [0; 12],
            }
        }
    }

    impl From<libc::statx_timestamp> for statx_timestamp {
        fn from(src: libc::statx_timestamp) -> Self {
            Self {
                tv_sec: src.tv_sec,
                tv_nsec: src.tv_nsec,
                __statx_timestamp_pad1: [0],
            }
        }
    }

    pub enum IOUringActorResponse {
        // Non-direct
        Read(Vec<u8>),
        Write,

        // Direct
        ReadBlockDirect(Box<dyn AlignedBuffer>),
        WriteBlockDirect,

        // Other responses
        TrimBlock,

        // Add new Metadata response
        Statx(libc::statx),
    }

    pub struct IOUringAPI<const BLOCK_SIZE: usize> {
        sender: Sender<IOUringActorCommand>,
    }

    impl<const BLOCK_SIZE: usize> IOUringAPI<BLOCK_SIZE> {
        pub async fn new(
            fd: std::fs::File,
            ring: IoUring,
            channel_size: usize,
        ) -> std::io::Result<Self> {
            let (sender, receiver) = match channel_size {
                0 => flume::unbounded(),
                _ => flume::bounded(channel_size),
            };

            let uring_fd = io_uring::types::Fd(fd.as_raw_fd());

            let actor = IOUringActor::<BLOCK_SIZE> {
                _fd: fd,
                fd: uring_fd,
                ring,
                receiver,
            };

            tokio::spawn(actor.run(channel_size));

            Ok(Self { sender })
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
        pub async fn write(&self, offset: u64, buffer: Vec<u8>) -> std::io::Result<()> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::Write {
                    offset,
                    buffer,
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

        /// write_block uses direct IO to write a block to the device. The buffer must be less than or equal to BLOCK_SIZE.
        /// ```
        /// let hello = b"Hello, world!\n";
        /// let write_page = Page4K::new_with_data(hello);
        /// api.write_block(0, Box::new(write_page)).await?;
        ///
        /// // Prepare a buffer to read into
        /// let read_buffer = Box::new(Page4K([0u8; 4096]));
        ///
        /// // Verify data was written
        /// let read_buffer = api.read_block(0, read_buffer).await?;
        /// assert_eq!(&read_buffer.as_slice()[..hello.len()], hello);
        /// ```
        #[instrument(skip_all, level = "debug")]
        pub async fn write_block(
            &self,
            offset: u64,
            data: Box<dyn AlignedBuffer>,
        ) -> std::io::Result<()> {
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
                    data,
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
        /// ```
        /// let hello = b"Hello, world!\n";
        /// let write_page = Page4K::new_with_data(hello);
        /// api.write_block(0, Box::new(write_page)).await?;
        ///
        /// // Prepare a buffer to read into
        /// let read_buffer = Box::new(Page4K([0u8; 4096]));
        ///
        /// // Verify data was written
        /// let read_buffer = api.read_block(0, read_buffer).await?;
        /// assert_eq!(&read_buffer.as_slice()[..hello.len()], hello);
        /// ```
        #[instrument(skip_all, level = "debug")]
        pub async fn read_block(
            &self,
            offset: u64,
            buffer: Box<dyn AlignedBuffer>,
        ) -> std::io::Result<Box<dyn AlignedBuffer>> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::ReadBlockDirect {
                    offset,
                    sender,
                    buffer: Some(buffer),
                })
                .await
                .unwrap();
            let response = receiver.recv_async().await.unwrap();
            match response {
                Ok(IOUringActorResponse::ReadBlockDirect(result)) => Ok(result),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        /// trim_block uses direct IO to deallocate a block on the device. This reduces wear on SSDs compared to writing zeros.
        #[instrument(skip_all, level = "debug")]
        pub async fn trim_block(&self, offset: u64) -> std::io::Result<()> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::TrimBlock { offset, sender })
                .await
                .unwrap();
            let response = receiver.recv_async().await.unwrap();
            match response {
                Ok(IOUringActorResponse::TrimBlock) => Ok(()),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        /// get_metadata retrieves file metadata using statx
        #[instrument(skip_all, level = "debug")]
        pub async fn get_metadata(&self) -> std::io::Result<libc::statx> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::Statx { sender })
                .await
                .unwrap();
            let response = receiver.recv_async().await.unwrap();
            match response {
                Ok(IOUringActorResponse::Statx(metadata)) => Ok(metadata),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }
    }

    pub struct IOUringActor<const BLOCK_SIZE: usize> {
        _fd: std::fs::File, // Keeps the file descriptor alive
        fd: io_uring::types::Fd,
        ring: IoUring,
        receiver: Receiver<IOUringActorCommand>,
    }

    impl<const BLOCK_SIZE: usize> IOUringActor<BLOCK_SIZE> {
        /// run starts the actor loop, that will flip between consuming commands from the receiver,
        /// submitting them to the ring, polling completions, and sending responses back to the caller.
        async fn run(mut self, queue_size: usize) {
            debug!("Starting actor loop");
            let mut responders: VecDeque<(
                IOUringActorCommand,  // Need command to keep buffer in scope
                IOUringActorResponse, // Response to send back to the caller
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
                    if let Ok(result) = self.queue_command(command).await {
                        responders.push_back(result);
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
                    let (command, response) = responders.pop_front().unwrap();
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

                            // Now we can await after we're done with the completion queue since it's not Send,
                            // and we don't care about the result of the send
                            let _ = sender.send_async(result).await;
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
            command: IOUringActorCommand,
        ) -> std::io::Result<(IOUringActorCommand, IOUringActorResponse)> {
            match command {
                IOUringActorCommand::Read {
                    offset,
                    size,
                    sender,
                } => {
                    trace!("Read: {:?}", offset);
                    match self.handle_read(offset, size).await {
                        Ok(result) => Ok((
                            IOUringActorCommand::Read {
                                // reconstruct isn't awesome, but need it for borrow checker
                                offset,
                                size,
                                sender,
                            },
                            IOUringActorResponse::Read(result),
                        )),
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
                    sender,
                } => {
                    trace!("Write: {:?}", offset);
                    match self.handle_write(offset, &buffer).await {
                        Ok(()) => Ok((
                            IOUringActorCommand::Write {
                                offset,
                                buffer,
                                sender,
                            },
                            IOUringActorResponse::Write,
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
                    trace!("TrimBlock: {:?}", offset);
                    match self.handle_trim(offset).await {
                        Ok(()) => Ok((
                            IOUringActorCommand::TrimBlock { offset, sender },
                            IOUringActorResponse::TrimBlock,
                        )),
                        Err(e) => {
                            debug!("handle_trim error: {:?}", e);
                            let _ = sender.send_async(Err(e)).await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "handle_trim error",
                            ))
                        }
                    }
                }

                IOUringActorCommand::ReadBlockDirect {
                    offset,
                    sender,
                    buffer,
                } => {
                    trace!("ReadBlockDirect: {:?}", offset);
                    let buffer = buffer.unwrap();
                    match self.handle_read_direct(offset, buffer).await {
                        Ok(buffer) => Ok((
                            IOUringActorCommand::ReadBlockDirect {
                                offset,
                                sender,
                                buffer: None,
                            },
                            IOUringActorResponse::ReadBlockDirect(buffer),
                        )),
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
                    match self.handle_write_direct(offset, data.as_ref()).await {
                        Ok(()) => Ok((
                            IOUringActorCommand::WriteBlockDirect {
                                offset,
                                data,
                                sender,
                            },
                            IOUringActorResponse::WriteBlockDirect,
                        )),
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

                IOUringActorCommand::Statx { sender } => {
                    trace!("GetMetadata");
                    match self.handle_metadata().await {
                        Ok(metadata) => Ok((
                            IOUringActorCommand::Statx { sender },
                            IOUringActorResponse::Statx(metadata),
                        )),
                        Err(e) => {
                            debug!("handle_metadata error: {:?}", e);
                            let _ = sender.send_async(Err(e)).await;
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "handle_metadata error",
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
            mut buffer: Box<dyn AlignedBuffer>,
        ) -> std::io::Result<Box<dyn AlignedBuffer>> {
            let read_e = opcode::Read::new(self.fd, buffer.as_mut_ptr(), BLOCK_SIZE as _)
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

        /// Writes data, returning after submission
        async fn handle_write(&mut self, offset: u64, buffer: &[u8]) -> std::io::Result<()> {
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

            Ok(())
        }

        /// Writes a block to the device from the given buffer.
        async fn handle_write_direct(
            &mut self,
            offset: u64,
            buffer: &dyn AlignedBuffer,
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

        /// Gets file metadata using statx
        async fn handle_metadata(&mut self) -> std::io::Result<libc::statx> {
            // Create uninitialized statx buffer
            let mut statx_buf: libc::statx = unsafe { std::mem::zeroed() };

            println!(
                "statx_buf: {:?}",
                &mut statx_buf as *mut libc::statx as *mut _
            );

            let statx_e = opcode::Statx::new(
                self.fd,
                b"\0".as_ptr().cast(),
                &mut statx_buf as *mut libc::statx as *mut _,
            )
            .flags(libc::AT_EMPTY_PATH)
            .mask(libc::STATX_ALL)
            .build()
            .user_data(0x45);

            unsafe {
                self.ring
                    .submission()
                    .push(&statx_e)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            }

            Ok(statx_buf)
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
            .custom_flags(libc::O_DSYNC)
            .open(temp_path)?;

        println!("fd: {:?}", file);

        // Create a new device instance
        let api = IOUringAPI::<BLOCK_SIZE>::new(file, ring, 128).await?;

        // Test data
        // let mut write_data = [0u8; 1033];
        let hello = b"Hello, world!\n";
        // write_data[..hello.len()].copy_from_slice(hello);

        // Write test
        api.write(0, hello.to_vec()).await.unwrap();

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

        // Verify metadata
        let metadata = api.get_metadata().await.unwrap();
        println!("File size: {:?}", metadata.stx_size);
        assert_eq!(metadata.stx_size, hello.len() as u64);

        // Write again
        let hello = b"Hello, world again!\n";
        api.write(0, hello.to_vec()).await.unwrap();

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
        let temp_path = "blah_direct.test";
        println!("temp_path: {:?}", temp_path);

        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
            .open(temp_path)?;

        println!("fd: {:?}", file);

        // Create a 4k aligned buffer
        create_aligned_page!(Page4K, 4096);

        // Create a new device instance
        let api = IOUringAPI::<BLOCK_SIZE>::new(file, ring, 128).await?;

        // Write test data
        // Example: Write to offset 0
        let hello = b"Hello, world!\n";
        let write_page: Page4K<4096> = Page4K::new_with_data(hello);
        api.write_block(0, Box::new(write_page)).await?;

        // Prepare a buffer to read into
        let read_buffer = Box::new(Page4K([0u8; BLOCK_SIZE]));

        // Verify data was written
        let read_buffer = api.read_block(0, read_buffer).await?;
        assert_eq!(&read_buffer.as_slice()[..hello.len()], hello);

        // Write new test data
        let hello2 = b"Hello again, world!\n";
        let write_page2: Page4K<4096> = Page4K::new_with_data(hello2);
        api.write_block(0, Box::new(write_page2)).await?;

        // Verify new data was written
        let read_buffer = api.read_block(0, read_buffer).await?;
        assert_eq!(&read_buffer.as_slice()[..hello2.len()], hello2);

        // Trim the block
        api.trim_block(0).await?;

        // Read again - should now contain zeros
        let read_buffer = api.read_block(0, read_buffer).await?;
        assert_eq!(
            &read_buffer.as_slice()[..hello.len()],
            &vec![0u8; hello.len()]
        );
        println!(
            "Read data after trim (string): {:?}",
            String::from_utf8_lossy(&read_buffer.as_slice()[..hello.len()])
        );
        assert_eq!(
            &read_buffer.as_slice()[..hello.len()],
            &vec![0u8; hello.len()]
        );

        Ok(())
    }
}
