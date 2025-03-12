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
use crate::col_type::ColumnType;
use crate::error::{CoreErrorExt, CoreResult, fmt_err};
use memmap2::Mmap;
use std::fs::File;
use std::path::PathBuf;

pub struct MappedColumn {
    pub col_type: ColumnType,
    pub col_name: String,
    pub parent_path: PathBuf,
    pub data: Mmap,
    pub aux: Option<Mmap>,
}

impl MappedColumn {
    pub fn open(
        parent_path: impl Into<PathBuf>,
        col_name: impl Into<String>,
        col_type: ColumnType,
    ) -> CoreResult<Self> {
        let col_name = col_name.into();
        let mut path = parent_path.into();
        path.push(&col_name);

        // Open and map the "data" file which is always present for columns.
        path.set_extension("d");
        let data_file = File::open(&path).with_context(|_| {
            format!(
                "Could not open data file for column: {}, col_type: {}, path: {}",
                col_name,
                col_type,
                path.display()
            )
        })?;
        let data = unsafe { Mmap::map(&data_file) }.with_context(|_| {
            format!(
                "Could not map data file for column: {}, col_type: {}, path: {}",
                col_name,
                col_type,
                path.display()
            )
        })?;

        // Open and map the "aux" file which is present for var-sized types.
        let aux = if col_type.tag().is_var_size() {
            path.set_extension("i");
            let aux_file = File::open(&path).with_context(|_| {
                format!(
                    "Could not open aux file for column: {}, col_type: {}, path: {}",
                    col_name,
                    col_type,
                    path.display()
                )
            })?;
            let aux = unsafe { Mmap::map(&aux_file) }.with_context(|_| {
                format!(
                    "Could not map aux file for column: {}, col_type: {}, path: {}",
                    col_name,
                    col_type,
                    path.display()
                )
            })?;
            Some(aux)
        } else {
            let fixed_size = col_type.tag().fixed_size().expect("fixed size column");
            if data.len() % fixed_size != 0 {
                return Err(fmt_err!(
                    InvalidColumnData,
                    "Bad file size {} for column: {}, col_type: {}, path: {}, expected a multiple of {}",
                    data.len(),
                    col_name,
                    col_type,
                    path.display(),
                    fixed_size
                ));
            }
            None
        };

        // Restore the parent path.
        path.pop();
        Ok(Self {
            col_type,
            col_name,
            parent_path: path,
            data,
            aux,
        })
    }
}
