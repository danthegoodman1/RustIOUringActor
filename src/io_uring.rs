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
    use tracing::debug;

    #[derive(Debug)]
    pub enum IOUringActorCommand {
        // Non-direct
        ReadBlock(u64, Sender<IOUringActorResponse>),
        WriteBlock(u64, Vec<u8>, Sender<IOUringActorResponse>),

        // Direct
        ReadBlockDirect(u64, Sender<IOUringActorResponse>),
        WriteBlockDirect(u64, Box<dyn AlignedBuffer>, Sender<IOUringActorResponse>),

        // Other commands
        TrimBlock(u64, Sender<IOUringActorResponse>),
    }

    #[derive(Debug)]
    pub enum IOUringActorResponse {
        // Direct
        ReadBlockDirect(u64, Box<dyn AlignedBuffer>),
        WriteBlockDirect(u64),

        // Non-direct
        ReadBlock(u64, Vec<u8>),
        WriteBlock(u64),

        // Other responses
        TrimBlock(u64),
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

            let fd = io_uring::types::Fd(fd.as_raw_fd());

            let actor = IOUringActor::<BLOCK_SIZE> { fd, ring, receiver, in_flight_commands: 0 };

            tokio::spawn(actor.run());

            Ok(Self { sender })
        }


        /// Read uses non-direct IO to read a block from the device.
        pub async fn read(
            &self,
            offset: u64,
        ) -> std::io::Result<Vec<u8>> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::ReadBlock(offset, sender))
                .await
                .unwrap();
            let response = receiver.recv_async().await;
            match response {
                Ok(IOUringActorResponse::ReadBlock(offset, buffer)) => Ok(buffer),
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
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::ReadBlock(offset, sender))
                .await
                .unwrap();
            let response = receiver.recv_async().await;
            match response {
                Ok(IOUringActorResponse::ReadBlock(offset, buffer)) => Ok(()),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid response",
                )),
            }
        }

        /// Write uses non-direct IO to write a buffer to the device.
        pub async fn write(
            &self,
            offset: u64,
            buffer: Vec<u8>,
        ) -> std::io::Result<()> {
            let (sender, receiver) = flume::unbounded();
            self.sender
                .send_async(IOUringActorCommand::WriteBlock(offset, buffer, sender))
                .await
                .unwrap();
            let response = receiver.recv_async().await;
            todo!()
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
        fd: io_uring::types::Fd,
        ring: IoUring,
        receiver: Receiver<IOUringActorCommand>,
        in_flight_commands: usize,
    }

    impl<const BLOCK_SIZE: usize> IOUringActor<BLOCK_SIZE> {
        // TODO: Read
        // TODO: Write
        // TODO: Delete (calls trim)
        async fn run(self) {
            debug!("Starting actor loop");
            loop {
                // TODO: grab a buffer of commands
                match self.receiver.recv_async().await {
                    Ok(IOUringActorCommand::ReadBlock(offset, sender)) => {
                        debug!("ReadBlock: {:?}", offset);
                    }
                    Ok(IOUringActorCommand::WriteBlock(offset, buffer, sender)) => {
                        debug!("WriteBlock: {:?}", offset);
                    }
                    Ok(IOUringActorCommand::TrimBlock(offset, sender)) => {
                        debug!("TrimBlock: {:?}", offset);
                    }
                    Ok(IOUringActorCommand::ReadBlockDirect(offset, sender)) => {
                        debug!("ReadBlockDirect: {:?}", offset);
                    }
                    Ok(IOUringActorCommand::WriteBlockDirect(offset, buffer, sender)) => {
                        debug!("WriteBlockDirect: {:?}", offset);
                    }
                    Err(e) => {
                        // Disconnected
                        break;
                    }
                }
                // TODO: wait for commands to complete, get results, and send back
            }
        }

        // TODO: handle_read
        // TODO: handle_write
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
    use linux_impl::{IOUringAPI, IOUringActor};
    use tokio::sync::Mutex;

    use super::*;
    use std::sync::Arc;

    const BLOCK_SIZE: usize = 4096;

    create_aligned_page!(Page4K, 4096);

    // Test to ensure page4k implements Send trait
    #[tokio::test]
    async fn test_page4k_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Page4K<4096>>();
    }

    // #[tokio::test]
    // async fn test_io_uring_read_write() -> Result<(), Box<dyn std::error::Error>> {
    //     // Create a shared io_uring instance
    //     let ring = IoUring::new(128)?;

    //     // Create a temporary file path
    //     let temp_file = tempfile::NamedTempFile::new()?;
    //     let temp_path = temp_file.path().to_str().unwrap();

    //     let file = std::fs::File::open(temp_path)?;

    //     // Create a new device instance
    //     let mut api = IOUringAPI::<BLOCK_SIZE>::new(file, ring, 0).await?;

    //     // Test data
    //     let mut write_data = [0u8; BLOCK_SIZE];
    //     let hello = b"Hello, world!\n";
    //     write_data[..hello.len()].copy_from_slice(hello);
    //     let write_page = Page4K(write_data);

    //     // Write test
    //     api.write_block(0, &write_page).await?;

    //     // Read test
    //     let mut read_buffer = Page4K([0u8; BLOCK_SIZE]);
    //     api.read_block(0, &mut read_buffer).await?;

    //     // Verify the contents
    //     assert_eq!(&read_buffer.0[..hello.len()], hello);
    //     println!("Read data: {:?}", &read_buffer.0[..hello.len()]);
    //     // As a string
    //     println!(
    //         "Read data (string): {}",
    //         String::from_utf8_lossy(&read_buffer.0[..hello.len()])
    //     );

    //     // Try trimming the block
    //     api.trim_block(0).await?;

    //     // Read the block again
    //     let mut read_buffer = Page4K([0u8; BLOCK_SIZE]);
    //     api.read_block(0, &mut read_buffer).await?;

    //     // Verify the contents are zeroed
    //     assert_eq!(&read_buffer.0, &[0u8; BLOCK_SIZE]);

    //     Ok(())
    // }

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
