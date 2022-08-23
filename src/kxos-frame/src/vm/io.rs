use crate::prelude::*;
use crate::vm::Pod;

/// A trait that enables reading/writing data from/to a VM object,
/// e.g., `VmSpace`, `VmFrameVec`, and `VmFrame`.
///
/// # Concurrency
///
/// The methods may be executed by multiple concurrent reader and writer
/// threads. In this case, if the results of concurrent reads or writes
/// desire predictability or atomicity, the users should add extra mechanism
/// for such properties.
pub trait VmIo: Send + Sync {
    /// Read a specified number of bytes at a specified offset into a given buffer.
    ///
    /// # No short reads
    ///
    /// On success, the output `buf` must be filled with the requested data
    /// completely. If, for any reason, the requested data is only partially
    /// available, then the method shall return an error.
    fn read_bytes(&self, offset: usize, buf: &mut [u8]) -> Result<()>;

    /// Read a value of a specified type at a specified offset.
    fn read_val<T: Pod>(&self, offset: usize) -> Result<T> {
        let mut val = T::new_uninit();
        self.read_bytes(offset, val.as_bytes_mut())?;
        Ok(val)
    }

    /// Read a slice of a specified type at a specified offset.
    ///
    /// # No short reads
    ///
    /// Similar to `read_bytes`.
    fn read_slice<T: Pod>(&self, offset: usize, slice: &mut [T]) -> Result<()> {
        let buf = unsafe { core::mem::transmute(slice) };
        self.read_bytes(offset, buf)
    }

    /// Write a specified number of bytes from a given buffer at a specified offset.
    ///
    /// # No short writes
    ///
    /// On success, the input `buf` must be written to the VM object entirely.
    /// If, for any reason, the input data can only be written partially,
    /// then the method shall return an error.
    fn write_bytes(&mut self, offset: usize, buf: &[u8]) -> Result<()>;

    /// Write a value of a specified type at a specified offset.
    fn write_val<T: Pod>(&mut self, offset: usize, new_val: &T) -> Result<()> {
        self.write_bytes(offset, new_val.as_bytes())?;
        Ok(())
    }

    /// Write a slice of a specified type at a specified offset.
    ///
    /// # No short write
    ///
    /// Similar to `write_bytes`.
    fn write_slice<T: Pod>(&mut self, offset: usize, slice: &[T]) -> Result<()> {
        let buf = unsafe { core::mem::transmute(slice) };
        self.write_bytes(offset, buf)
    }
}
