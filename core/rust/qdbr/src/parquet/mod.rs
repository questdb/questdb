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
pub(crate) mod error;
pub(crate) mod io;
pub(crate) mod qdb_metadata;
pub(crate) mod util;

// Don't expose this in the general API, as it heightens the risk
// of constructing an invalid `ColumnType`, e.g. one without the appropriate
// extra type info for Geo types.
#[cfg(test)]
pub(crate) mod tests {
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};

    pub trait ColumnTypeTagExt {
        fn into_type(self) -> ColumnType;
    }

    impl ColumnTypeTagExt for ColumnTypeTag {
        fn into_type(self) -> ColumnType {
            ColumnType::new(self, 0)
        }
    }
}
