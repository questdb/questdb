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
use crate::error::{CoreResult, fmt_err};
use std::slice;

pub(super) fn cast_slice<T>(data: &[u8]) -> CoreResult<&[T]>
where
    T: Copy + 'static,
{
    if size_of::<T>() == 0 {
        return Err(fmt_err!(InvalidColumnData, "target type has zero size"));
    }

    if data.len() % size_of::<T>() != 0 {
        return Err(fmt_err!(
            InvalidColumnData,
            "size {} is not divisible by target type size of {} bytes",
            data.len(),
            size_of::<T>()
        ));
    }
    if data.as_ptr().align_offset(align_of::<T>()) != 0 {
        return Err(fmt_err!(
            InvalidColumnData,
            "start ptr {:p} is not aligned by target type alignment of {} bytes",
            data.as_ptr(),
            align_of::<T>()
        ));
    }

    Ok(unsafe { slice::from_raw_parts(data.as_ptr() as *const T, data.len() / size_of::<T>()) })
}
