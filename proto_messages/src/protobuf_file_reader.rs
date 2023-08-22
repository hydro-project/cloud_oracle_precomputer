use std::{fs::File, path::Path, io::{self, Read}};

use bytes::Buf;
use prost::encoding::{self, WireType, DecodeContext};

pub struct ProtobufFileReader {
    buf: Vec<u8>,
    pos: usize,
}

impl ProtobufFileReader {
    pub fn new(file_name: &Path) -> io::Result<Self> {
        let mut buf: Vec<u8> = Vec::new();
        File::open(file_name).unwrap().read_to_end(&mut buf)?;

        Ok(Self {
            buf,
            pos: 0,
        })
    }

    pub fn read_next<M>(&mut self) -> io::Result<M>
    where
        M: prost::Message + Default,
    {
        let mut msg = M::default();

        let r = encoding::message::merge(
            WireType::LengthDelimited,
            &mut msg,
            self,
            DecodeContext::default(),
        );

        if let Err(e) = r {
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }

        Ok(msg)
    }

    pub fn has_data(&self) -> bool {
        (self.buf.len() - self.pos) > 0
    }

    pub fn into_iter_all<M>(self) -> ProtobufFileReaderIterator<M>
    where
        M: prost::Message + Default,
    {
        ProtobufFileReaderIterator::new(self)
    }
}

// Buf wrapper on reader for prost decoding
impl<'a> Buf for ProtobufFileReader {
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn chunk(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    fn advance(&mut self, cnt: usize) {
        self.pos += cnt;
    }
}

// Iterator to load all messages of a given type from a file
pub struct ProtobufFileReaderIterator<M> {
    reader: ProtobufFileReader,
    _marker: std::marker::PhantomData<M>,
}

impl<'a, M> ProtobufFileReaderIterator<M> {
    pub fn new(reader: ProtobufFileReader) -> Self {
        Self {
            reader,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, M> Iterator for ProtobufFileReaderIterator<M>
where
    M: prost::Message + Default,
{
    type Item = M;

    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.has_data() {
            Some(self.reader.read_next().unwrap())
        } else {
            None
        }
    }
}