/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/
use crate::col_driver::{ColumnDriver, MappedColumn};
use crate::error::{CoreResult, fmt_err};
use std::intrinsics::transmute;

pub struct VarcharDriver;

impl ColumnDriver for VarcharDriver {
    fn col_sizes_for_row(
        &self,
        col: &MappedColumn,
        row_index: u64,
    ) -> CoreResult<(u64, Option<u64>)> {
        let (data_size, aux_size) = data_and_aux_size_at(col, row_index)?;
        Ok((data_size, Some(aux_size)))
    }
}

#[repr(transparent)]
struct VarcharAuxEntry {
    packed: u128,
}

const VARCHAR_HEADER_FLAG_INLINED: u8 = 1;
const VARCHAR_HEADER_FLAG_NULL: u8 = 4;

impl VarcharAuxEntry {
    fn flags(&self) -> u8 {
        // The flags are stored in the lowest 4 bits.
        (self.packed & 0xf) as u8
    }

    fn is_inlined(&self) -> bool {
        self.flags() & VARCHAR_HEADER_FLAG_INLINED != 0
    }

    fn size(&self) -> u32 {
        if self.is_inlined() {
            // An inlined varchar has at most 4 bits for the size.
            ((self.packed >> 4) & 0xf) as u32
        } else {
            // The size at the offset in the data file.
            (self.packed >> 4) as u32
        }
    }

    fn is_null(&self) -> bool {
        self.flags() & VARCHAR_HEADER_FLAG_NULL != 0
    }

    fn offset(&self) -> u64 {
        (self.packed >> 80) as u64
    }
}

/// Return (data_size, aux_size).
fn data_and_aux_size_at(col: &MappedColumn, row_index: u64) -> CoreResult<(u64, u64)> {
    let aux_mmap = col.aux.as_ref().expect("varchar has aux");
    let data_mmap = &col.data;
    if aux_mmap.len() % size_of::<u128>() != 0 {
        return Err(fmt_err!(
            InvalidColumnData,
            "varchar aux len {} not a multiple of 16 bytes for column {} in {}",
            aux_mmap.len(),
            col.col_name,
            col.parent_path.display()
        ));
    }
    let aux: &[VarcharAuxEntry] = unsafe { transmute(&aux_mmap[..]) };
    // TODO(amunra): Am I even indexing the right value or should this be `row_index - 1`?
    let Some(aux_entry) = aux.get(row_index as usize) else {
        return Err(fmt_err!(
            InvalidColumnData,
            "varchar row index {} not found in aux for column {} in {}",
            row_index,
            col.col_name,
            col.parent_path.display()
        ));
    };

    let aux_size = (row_index + 1) * size_of::<VarcharAuxEntry>() as u64;
    let offset = aux_entry.offset();
    let data_size = if aux_entry.is_inlined() || aux_entry.is_null() {
        offset
    } else {
        offset + aux_entry.size() as u64
    };
    if (data_mmap.len() as u64) < data_size {
        return Err(fmt_err!(
            InvalidColumnData,
            "varchar data size {} exceeds data mmap len {} for column {} in {}",
            data_size,
            data_mmap.len(),
            col.col_name,
            col.parent_path.display()
        ));
    }
    Ok((data_size, aux_size))
}
