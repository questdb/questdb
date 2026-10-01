/*+*****************************************************************************
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
 ******************************************************************************/

//! Parquet writer encoders organized by encoding family.
//!
//! Each module owns the per-partition wrapper functions for its encoding
//! family and is parameterized over the column type via generics where
//! possible. The top-level dispatch in `parquet_write/encode.rs` mirrors the
//! decoder side at `parquet_read/decode.rs` by splitting per physical type and
//! constructing the right function for each `(encoding, column_type_tag)`
//! tuple.
//!
//! Multi-partition row groups pass `columns.len() == N`; single-partition
//! row groups pass `columns.len() == 1`. The same function handles both —
//! each encoder materializes the selected logical column chunk and emits a
//! single data page for it. Dictionary encoders add a dict page ahead of that
//! chunk-level data page.

pub mod delta_binary_packed;
pub mod delta_length_array;
pub mod helpers;
pub mod numeric;
pub mod plain;
pub mod rle_dictionary;
pub mod symbol;
