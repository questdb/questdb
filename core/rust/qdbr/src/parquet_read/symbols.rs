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
#![allow(dead_code)]

use std::fs::File;
use std::path::Path;
use memmap2::Mmap;
use anyhow::Context;

// const SYMBOL_MAP_OFFSET_HEADER_SIZE: usize = 64;

pub struct SymbolMapReader {
    offsets: Mmap,
    keys: Mmap,
    values: Mmap,
}

impl SymbolMapReader {
    pub fn new(offsets_path: &Path, values_path: &Path) -> anyhow::Result<Self> {
        let offsets_file = File::open(offsets_path)
            .with_context(|| format!("Cannot open offsets file: {:?}", offsets_path))?;
        let offsets = unsafe { Mmap::map(&offsets_file) }
            .with_context(|| format!("Cannot mmap offsets file: {:?}", offsets_path))?;
        let keys_file = File::open(values_path)
            .with_context(|| format!("Cannot open keys file: {:?}", values_path))?;
        let keys = unsafe { Mmap::map(&keys_file) }
            .with_context(|| format!("Cannot mmap keys file: {:?}", values_path))?;
        let values_file = File::open(values_path)
            .with_context(|| format!("Cannot open values file: {:?}", values_path))?;
        let values = unsafe { Mmap::map(&values_file) }
            .with_context(|| format!("Cannot mmap values file: {:?}", values_path))?;
        Ok(Self {offsets, keys, values})
    }
}
