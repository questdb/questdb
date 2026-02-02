/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
use std::borrow::Cow;
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug)]
pub struct MappedColumn {
    pub col_type: ColumnType,
    pub col_name: String,
    pub parent_path: PathBuf,
    pub data: Mmap,
    pub aux: Option<Mmap>,
}

impl MappedColumn {
    // nightly has add_extension
    pub fn build_file_extension(base_extension: &str, col_version: Option<u64>) -> Cow<'_, str> {
        match col_version {
            Some(version) => Cow::Owned(format!("{}.{}", base_extension, version)),
            _ => Cow::Borrowed(base_extension),
        }
    }

    pub fn open_versioned(
        parent_path: impl Into<PathBuf>,
        col_name: impl Into<String>,
        col_type: ColumnType,
        col_version: Option<u64>,
    ) -> CoreResult<Self> {
        let col_name = col_name.into();
        let mut column_path = parent_path.into();
        column_path.push(&col_name);

        // Open and map the "data" file which is always present for columns.
        let data_extension = Self::build_file_extension("d", col_version);
        let mut column_path_data = column_path.clone();
        column_path_data.set_extension(data_extension.as_ref());
        let data_file = File::open(&column_path_data).with_context(|_| {
            format!(
                "Could not open data file for column: {}, col_type: {}, path: {}",
                col_name,
                col_type,
                column_path_data.display()
            )
        })?;
        let data = unsafe { Mmap::map(&data_file) }.with_context(|_| {
            format!(
                "Could not map data file for column: {}, col_type: {}, path: {}",
                col_name,
                col_type,
                column_path_data.display()
            )
        })?;

        // Open and map the "aux" file which is present for var-sized types.
        let aux = if col_type.tag().is_var_size() {
            let mut column_path_aux = column_path;
            let aux_extension = Self::build_file_extension("i", col_version);
            column_path_aux.set_extension(aux_extension.as_ref());
            let aux_file = File::open(&column_path_aux).with_context(|_| {
                format!(
                    "Could not open aux file for column: {}, col_type: {}, path: {}",
                    col_name,
                    col_type,
                    column_path_aux.display()
                )
            })?;
            let aux = unsafe { Mmap::map(&aux_file) }.with_context(|_| {
                format!(
                    "Could not map aux file for column: {}, col_type: {}, path: {}",
                    col_name,
                    col_type,
                    column_path_aux.display()
                )
            })?;
            Some(aux)
        } else {
            let fixed_size = col_type.tag().fixed_size().expect("fixed size column");
            if data.len() % fixed_size != 0 {
                return Err(fmt_err!(
                    InvalidLayout,
                    "Bad file size {} for column: {}, col_type: {}, path: {}, expected a multiple of {}",
                    data.len(),
                    col_name,
                    col_type,
                    column_path_data.display(),
                    fixed_size
                ));
            }
            None
        };

        column_path_data.pop();
        Ok(Self {
            col_type,
            col_name,
            parent_path: column_path_data,
            data,
            aux,
        })
    }

    pub fn open(
        parent_path: impl Into<PathBuf>,
        col_name: impl Into<String>,
        col_type: ColumnType,
    ) -> CoreResult<Self> {
        Self::open_versioned(parent_path, col_name, col_type, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::col_type::ColumnTypeTag;

    #[test]
    fn test_ok() {
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/primitives");
        let mapped = MappedColumn::open(
            parent_path.clone(),
            "int_col1",
            ColumnTypeTag::Int.into_type(),
        )
        .unwrap();
        assert_eq!(mapped.col_type, ColumnTypeTag::Int.into_type());
        assert_eq!(mapped.col_name, "int_col1");
        assert_eq!(mapped.parent_path, parent_path);
    }

    #[test]
    fn test_no_data_file() {
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/primitives");
        let err = MappedColumn::open(
            parent_path,
            "non_existant_col",
            ColumnTypeTag::Int.into_type(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.starts_with("Could not open data file for column"));
    }

    #[test]
    fn test_no_aux_file() {
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/primitives");
        let err = MappedColumn::open(parent_path, "int_col0", ColumnTypeTag::Varchar.into_type())
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.starts_with("Could not open aux file for column"));
    }

    #[test]
    fn test_empty() {
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/primitives");
        MappedColumn::open(parent_path, "empty.d", ColumnTypeTag::Long.into_type()).unwrap();
    }

    #[test]
    fn test_unaligned() {
        let mut parent_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        parent_path.push("resources/test/col_driver/binary");
        let err =
            MappedColumn::open(parent_path, "b1", ColumnTypeTag::Long256.into_type()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.starts_with("Bad file size 93 for column: b1, col_type: 13"));
        assert!(msg.contains("expected a multiple of 32"));
    }
}
