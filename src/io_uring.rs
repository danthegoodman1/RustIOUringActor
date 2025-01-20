// Trait for aligned pages to implement
#[cfg(target_os = "linux")]
pub trait AlignedBuffer: Send {
    fn as_ptr(&self) -> *const u8;
    fn as_mut_ptr(&mut self) -> *mut u8;
    fn len(&self) -> usize;
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
    use std::os::fd::AsRawFd;

    use super::AlignedBuffer;
    use flume::{Receiver, Sender};
    use io_uring::{opcode, IoUring};
    use tracing::{debug, warn};

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
            buffer: Box<dyn AlignedBuffer>,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },
        WriteBlockDirect {
            offset: u64,
            buffer: Box<dyn AlignedBuffer>,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },

        // Other commands
        TrimBlock {
            offset: u64,
            sender: Sender<std::io::Result<IOUringActorResponse>>,
        },
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

            tokio::spawn(actor.run());

            Ok(Self { sender })
        }

        /// Read uses non-direct IO to read a block from the device.
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
            println!("Read response: {:?}", response);
            match response {
                Ok(IOUringActorResponse::Read(result)) => Ok(result),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        pub async fn read_block(
            &self,
            offset: u64,
            buffer: &mut impl AlignedBuffer,
        ) -> std::io::Result<()> {
            todo!()
        }

        /// Write uses non-direct IO to write a buffer to the device.
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
            println!("Write response: {:?}", response);
            match response {
                Ok(IOUringActorResponse::Write) => Ok(()),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        /// write_block uses direct IO to write a block to the device.
        pub async fn write_block(
            &self,
            offset: u64,
            buffer: &mut impl AlignedBuffer,
        ) -> std::io::Result<()> {
            todo!()
        }

        pub async fn trim_block(&self, offset: u64) -> std::io::Result<()> {
            todo!()
        }
    }

    pub struct IOUringActor<const BLOCK_SIZE: usize> {
        _fd: std::fs::File, // Keeps the file descriptor alive
        fd: io_uring::types::Fd,
        ring: IoUring,
        receiver: Receiver<IOUringActorCommand>,
    }

    impl<const BLOCK_SIZE: usize> IOUringActor<BLOCK_SIZE> {
        // TODO: Read
        // TODO: Write
        // TODO: Delete (calls trim)
        async fn run(mut self) {
            debug!("Starting actor loop");
            loop {
                let mut responders = Vec::new();
                let mut keep_alive_buffers = Vec::new(); // FIXME: This is a hack to keep the buffer alive
                                                         // But in theory if we know the number of commands we'll receive, we can just
                                                         // allocate a fixed number of buffers
                match self.receiver.recv_async().await {
                    Ok(IOUringActorCommand::Read {
                        offset,
                        size,
                        sender,
                    }) => {
                        debug!("Read: {:?}", offset);
                        match self.handle_read(offset, size).await {
                            Ok(result) => {
                                responders.push((sender, IOUringActorResponse::Read(result)))
                            }
                            Err(e) => {
                                debug!("handle_read error: {:?}", e);
                                // We don't care if this fails because the channel is closed
                                let _ = sender.send_async(Err(e)).await;
                            }
                        }
                    }

                    Ok(IOUringActorCommand::Write {
                        offset,
                        buffer,
                        sender,
                    }) => {
                        debug!("WriteBlock: {:?}", offset);
                        match self.handle_write(offset, &buffer).await {
                            Ok(()) => {
                                responders.push((sender, IOUringActorResponse::Write));
                                keep_alive_buffers.push(buffer);
                            }
                            Err(e) => {
                                debug!("handle_write error: {:?}", e);
                                // We don't care if this fails because the channel is closed
                                let _ = sender.send_async(Err(e)).await;
                            }
                        }
                    }

                    Ok(IOUringActorCommand::TrimBlock { offset, sender }) => {
                        debug!("TrimBlock: {:?}", offset);
                    }

                    Ok(IOUringActorCommand::ReadBlockDirect {
                        offset,
                        buffer,
                        sender,
                    }) => {
                        debug!("ReadBlockDirect: {:?}", offset);
                    }

                    Ok(IOUringActorCommand::WriteBlockDirect {
                        offset,
                        buffer,
                        sender,
                    }) => {
                        debug!("WriteBlockDirect: {:?}", offset);
                    }

                    Err(e) => {
                        // Disconnected
                        break;
                    }
                }

                // Process completion - Modified to not hold completion queue across await
                for (sender, response) in responders {
                    // First we need to wait for the completion queue to have an entry
                    self.ring.submit_and_wait(1).unwrap();
                    let result = if let Some(cqe) = self.ring.completion().next() {
                        if cqe.result() < 0 {
                            debug!("Completion error: {:?}", cqe.result());
                            Err(std::io::Error::from_raw_os_error(-cqe.result()))
                        } else {
                            Ok(response)
                        }
                    } else {
                        println!("No completion queue entry found");
                        Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "Invalid response",
                        ))
                    };

                    // Now we can await after we're done with the completion queue since it's not Send,
                    // and we don't care about the result of the send
                    let _ = sender.send_async(result).await;
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

            // self.ring.submit()?;

            // TODO: just submit and let caller wait
            // self.ring.submit_and_wait(1)?;

            // // Process completion
            // while let Some(cqe) = self.ring.completion().next() {
            //     if cqe.result() < 0 {
            //         return Err(std::io::Error::from_raw_os_error(-cqe.result()));
            //     }
            // }

            // println!("Read completed {:?}", buffer);

            Ok(buffer)
        }

        /// Reads a block from the device into the given buffer.
        async fn handle_read_direct<T: AlignedBuffer>(
            &mut self,
            offset: u64,
            buffer: &mut T,
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

            // TODO: just submit and let caller wait
            self.ring.submit_and_wait(1)?;

            // Process completion
            while let Some(cqe) = self.ring.completion().next() {
                if cqe.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
            }

            Ok(())
        }

        /// Writes data, returning after submission
        async fn handle_write(&mut self, offset: u64, buffer: &[u8]) -> std::io::Result<()> {
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

            // self.ring.submit()?;

            // unsafe {
            //     self.ring
            //         .submission()
            //         .push(&write_e)
            //         .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            // }

            // self.ring.submit_and_wait(1)?;

            // while let Some(cqe) = self.ring.completion().next() {
            //     if cqe.result() < 0 {
            //         return Err(std::io::Error::from_raw_os_error(-cqe.result()));
            //     }
            // }

            Ok(())
        }

        /// Writes a block to the device from the given buffer.
        async fn handle_write_direct<T: AlignedBuffer>(
            &mut self,
            offset: u64,
            buffer: &mut T,
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

            // TODO: just submit and let caller wait
            self.ring.submit_and_wait(1)?;

            // Process completion
            while let Some(cqe) = self.ring.completion().next() {
                if cqe.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
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

            // TODO: just submit and let caller wait
            self.ring.submit_and_wait(1)?;

            // Process completion
            while let Some(cqe) = self.ring.completion().next() {
                if cqe.result() < 0 {
                    return Err(std::io::Error::from_raw_os_error(-cqe.result()));
                }
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

    // Test to ensure AlignedBuffer implements Send trait
    #[tokio::test]
    async fn test_aligned_buffer_is_send() {
        create_aligned_page!(Page4K, 4096);
        fn assert_send<T: Send>() {}
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

        // let test_data = b"Hello, basic file test!";
        // file.write_all(test_data)?;
        // file.flush()?;

        // // Read back the data
        // let mut file = std::fs::File::open(temp_path)?;
        // let mut contents = Vec::new();
        // file.read_to_end(&mut contents)?;

        // // Verify contents
        // assert_eq!(&contents, test_data);

        // println!("contents: {:?}", contents);

        // Create a new device instance
        let api = IOUringAPI::<BLOCK_SIZE>::new(file, ring, 0).await?;

        // Start by reading existing content
        // Read test
        let result = api.read(0, 3).await.unwrap();
        println!("read result: {:?}", result);

        // Test data
        // let mut write_data = [0u8; 1033];
        let hello = b"Hello, world!\n";
        // write_data[..hello.len()].copy_from_slice(hello);

        // Write test
        api.write(0, hello.to_vec()).await.unwrap();

        // Read test
        let result = api.read(0, 14).await.unwrap();

        // Verify the contents
        println!("Read data: {:?}", &result[..hello.len()]);
        println!(
            "Read data (string): {}",
            String::from_utf8_lossy(&result[..hello.len()])
        );
        assert_eq!(&result[..hello.len()], hello);

        Ok(())
    }

    // #[tokio::test]
    // async fn test_io_uring_sqpoll() -> Result<(), Box<dyn std::error::Error>> {
    //     create_aligned_page!(Page4K, 4096); // test creating an aliged page with a macro

    //     // Create a shared io_uring instance with SQPOLL enabled
    //     let ring = Arc::new(Mutex::new(
    //         IoUring::builder()
    //             .setup_sqpoll(2000) // 2000ms timeout
    //             .build(128)?,
    //     ));

    //     // Create a temporary file path
    //     let temp_file = tempfile::NamedTempFile::new()?;
    //     let temp_path = temp_file.path().to_str().unwrap();

    //     // Create a new device instance
    //     let file = std::fs::File::open(temp_path)?;

    //     // Create a new device instance
    //     let mut api = IOUringAPI::<BLOCK_SIZE>::new(file, ring, 0).await?;

    //     // Test data
    //     let mut write_data = [0u8; BLOCK_SIZE];
    //     let test_data = b"Testing SQPOLL mode!\n";
    //     write_data[..test_data.len()].copy_from_slice(test_data);
    //     let write_page = Page4K(write_data);

    //     // Write test
    //     api.write_block(0, &write_page).await?;

    //     // Read test
    //     let mut read_buffer = Page4K([0u8; BLOCK_SIZE]);
    //     api.read_block(0, &mut read_buffer).await?;

    //     // Verify the contents
    //     assert_eq!(&read_buffer.0[..test_data.len()], test_data);

    //     Ok(())
    // }
}
