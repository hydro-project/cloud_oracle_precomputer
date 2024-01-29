use bytes::{buf::UninitSlice, BufMut};
use futures::sink::Sink;
use hydroflow::futures;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

pub struct ProtobufFileSink {
    write_buffer: Vec<u8>,
    file: File,
    spare_capacity: usize,
}

impl ProtobufFileSink {
    /// Creates a new `ProtobufFileSink` instance with the given file name, capacity, and spare capacity.
    ///
    /// # Arguments
    ///
    /// * `file_name` - The path to the file to create.
    /// * `capacity` - The capacity of the write buffer.
    /// * `spare_capacity` - The amount of extra capacity to maintain.
    /// This should be at least as big as the largest field being written, since fragmentation is not supported!
    ///
    /// # Returns
    ///
    /// A new `io::Result` containing the `ProtobufFileSink` instance if the file was successfully created.
    pub fn new(
        file_name: &Path,
        capacity: usize,
        spare_capacity: usize,
    ) -> io::Result<ProtobufFileSink> {
        let file = File::create(file_name)?;
        let write_buffer = Vec::with_capacity(capacity);
        Ok(ProtobufFileSink {
            write_buffer,
            file,
            spare_capacity,
        })
    }

    /// Flushes the write buffer to the file and clears it.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if there was an error writing to the file.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::io::Write;
    /// use skypie_lib::protobuf_file_sink::ProtobufFileSink;
    ///
    /// let mut sink = ProtobufFileSink::new("file.bin").unwrap();
    /// sink.write_all(b"hello world").unwrap();
    /// sink.flush().unwrap();
    /// ```
    pub fn flush(&mut self) -> io::Result<()> {
        self.file
            .write_all(&self.write_buffer[0..self.write_buffer.len()])?;

        self.write_buffer.clear();
        self.file.flush()
    }
}

impl<Item> Sink<Item> for ProtobufFileSink
where
    Item: prost::Message,
{
    type Error = io::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(self.get_mut().flush())
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(self.get_mut().flush())
    }

    /// This function is called by the stream when the sink is ready to receive a new item.
    /// It encodes the item as a length-delimited protobuf message and returns an error if the encoding fails.
    fn start_send(self: std::pin::Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        let mut_self = self.get_mut();

        let res = item.encode_length_delimited(mut_self);

        if res.is_ok() {
            return Ok(());
        }
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to encode protobuf message",
            ));
        }
    }
}

unsafe impl BufMut for ProtobufFileSink {
    fn remaining_mut(&self) -> usize {
        self.write_buffer.remaining_mut()
    }

    /// Advances the write buffer by `cnt` bytes and flushes the buffer if the remaining capacity is less than the spare capacity.
    ///
    /// # Safety
    ///
    /// This function is marked unsafe because it mutates the internal state of the `protobuf_file_sink` struct.
    ///
    /// # Arguments
    ///
    /// * `self` - mutable reference to the `protobuf_file_sink` struct.
    /// * `cnt` - number of bytes to advance the write buffer by.
    ///
    /// # Examples
    ///
    /// ```
    /// # use skypie_lib::protobuf_file_sink::ProtobufFileSink;
    /// let mut sink = ProtobufFileSink::new("file.bin").unwrap();
    /// unsafe {
    ///     sink.advance_mut(1024);
    /// }
    /// ```
    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.write_buffer.advance_mut(cnt);

        let remaining = self.write_buffer.remaining_mut();
        if self.spare_capacity > remaining {
            self.flush().unwrap();
        }
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        self.write_buffer.chunk_mut()
    }
}
