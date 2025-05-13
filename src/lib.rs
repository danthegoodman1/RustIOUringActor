pub mod io_uring_actor;
pub mod io_uring_file;

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
