pub mod dict_decoder;
pub mod dict_slicer;
pub mod rle;

use crate::parquet_write::{ParquetError, ParquetResult};
use parquet2::encoding::delta_bitpacked;
use parquet2::encoding::hybrid_rle::BitmapIter;
use parquet2::error::Error;
use std::mem::size_of;
use std::ptr;

pub trait DataPageSlicer {
    fn next(&mut self) -> &[u8];
    fn next_slice(&mut self, count: usize) -> Option<&[u8]>;
    fn skip(&mut self, count: usize);
    fn count(&self) -> usize;
    fn data_size(&self) -> usize;
    fn result(&self) -> ParquetResult<()>;
}

pub struct DataPageFixedSlicer<'a, const N: usize> {
    data: &'a [u8],
    pos: usize,
    row_count: usize,
}

impl<const N: usize> DataPageSlicer for DataPageFixedSlicer<'_, N> {
    fn next(&mut self) -> &[u8] {
        let res = &self.data[self.pos..self.pos + N];
        self.pos += N;
        res
    }

    fn next_slice(&mut self, count: usize) -> Option<&[u8]> {
        let res = &self.data[self.pos..self.pos + N * count];
        self.pos += N * count;
        Some(res)
    }

    fn skip(&mut self, count: usize) {
        self.pos += N * count;
    }

    fn count(&self) -> usize {
        self.row_count
    }

    fn data_size(&self) -> usize {
        self.row_count * N
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a, const N: usize> DataPageFixedSlicer<'a, N> {
    pub fn new(data: &'a [u8], row_count: usize) -> Self {
        Self { data, pos: 0, row_count }
    }
}

pub struct DeltaLengthArraySlicer<'a> {
    data: &'a [u8],
    row_count: usize,
    index: usize,
    lengths: Vec<i64>,
    pos: usize,
}

impl DataPageSlicer for DeltaLengthArraySlicer<'_> {
    fn next(&mut self) -> &[u8] {
        let len = self.lengths[self.index] as usize;
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        self.index += 1;
        res
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.pos += self.lengths[self.index] as usize;
            self.index += 1;
        }
    }

    fn count(&self) -> usize {
        self.row_count
    }

    fn data_size(&self) -> usize {
        self.data.len()
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a> DeltaLengthArraySlicer<'a> {
    pub fn try_new(data: &'a [u8], row_count: usize) -> ParquetResult<Self> {
        let mut decoder = delta_bitpacked::Decoder::try_new(data)?;

        let lengths: Vec<i64> = decoder.by_ref().collect::<Result<Vec<i64>, Error>>()?;

        let data_offset = decoder.consumed_bytes();
        Ok(Self {
            data: &data[data_offset..],
            row_count,
            index: 0,
            lengths,
            pos: 0,
        })
    }
}

pub struct PlainVarSlicer<'a> {
    data: &'a [u8],
    pos: usize,
    row_count: usize,
}

impl DataPageSlicer for PlainVarSlicer<'_> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        let len =
            unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) } as usize;
        self.pos += size_of::<u32>();
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        res
    }

    #[inline]
    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            let len =
                unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) };
            self.pos += len as usize + size_of::<u32>();
        }
    }

    #[inline]
    fn count(&self) -> usize {
        self.row_count
    }

    #[inline]
    fn data_size(&self) -> usize {
        self.data.len()
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a> PlainVarSlicer<'a> {
    pub fn new(data: &'a [u8], row_count: usize) -> Self {
        Self { data, pos: 0, row_count }
    }
}

// pub struct PlainVarDictSlicer<'a, T: DictDecoder> {
//     data: &'a [u8],
//     dict: T,
//     pos: usize,
//     row_count: usize,
// }
//
// impl<T: DictDecoder> DataPageSlicer for PlainVarDictSlicer<'_, T> {
//     fn next(&mut self) -> &[u8] {
//         let index = unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) };
//         self.pos += size_of::<u32>();
//         self.dict.get_dict_value(index)
//     }
//
//     fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
//         None
//     }
//
//     fn skip(&mut self, count: usize) {
//         for _ in 0..count {
//             let len = unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) };
//             self.pos += len as usize + size_of::<u32>();
//         }
//     }
//
//     fn count(&self) -> usize {
//         self.row_count
//     }
//
//     fn data_size(&self) -> usize {
//         self.row_count * self.dict.avg_key_len() as usize
//     }
//
//     fn result(&self) -> ParquetResult<()> {
//         Ok(())
//     }
// }
//
// impl<'a, T: DictDecoder> PlainVarDictSlicer<'a, T> {
//     pub fn new(data: &'a [u8], dict: T, row_count: usize) -> Self {
//         Self {
//             data,
//             dict,
//             pos: 0,
//             row_count,
//         }
//     }
// }

pub struct BooleanBitmapSlicer<'a> {
    bitmap_iter: BitmapIter<'a>,
    row_count: usize,
    error: ParquetResult<()>,
}

const BOOL_TRUE: [u8; 1] = [1];
const BOOL_FALSE: [u8; 1] = [0];

impl<'a> DataPageSlicer for BooleanBitmapSlicer<'a> {
    fn next(&mut self) -> &[u8] {
        if let Some(val) = self.bitmap_iter.next() {
            if val {
                return &BOOL_TRUE;
            }
            return &BOOL_FALSE;
        }
        self.error = Err(ParquetError::OutOfSpec(
            "not enough bitmap values to iterate".to_string(),
        ));
        &BOOL_FALSE
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.bitmap_iter.next();
        }
    }

    fn count(&self) -> usize {
        self.row_count
    }

    fn data_size(&self) -> usize {
        self.row_count
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

impl<'a> BooleanBitmapSlicer<'a> {
    pub fn new(data: &'a [u8], row_count: usize) -> Self {
        let bitmap_iter = BitmapIter::new(data, 0, row_count);
        Self { bitmap_iter, row_count, error: Ok(()) }
    }
}
