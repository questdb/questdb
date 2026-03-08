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
use crate::error::{CoreResult, fmt_err};
use std::slice;

pub fn cast_slice<T>(data: &[u8]) -> CoreResult<&[T]>
where
    T: Copy + 'static,
{
    if data.is_empty() {
        return Ok(&[]);
    }

    if size_of::<T>() == 0 {
        return Err(fmt_err!(
            InvalidLayout,
            "target type {} has zero size",
            std::any::type_name::<T>()
        ));
    }

    if !data.len().is_multiple_of(size_of::<T>()) {
        return Err(fmt_err!(
            InvalidLayout,
            "size {} is not divisible by target type {} size of {} bytes",
            data.len(),
            std::any::type_name::<T>(),
            size_of::<T>()
        ));
    }

    if data.as_ptr().align_offset(align_of::<T>()) != 0 {
        return Err(fmt_err!(
            InvalidLayout,
            "start ptr {:p} is not aligned to target type {} alignment of {} bytes",
            data.as_ptr(),
            std::any::type_name::<T>(),
            align_of::<T>()
        ));
    }

    Ok(unsafe { slice::from_raw_parts(data.as_ptr() as *const T, data.len() / size_of::<T>()) })
}

#[cfg(test)]
mod tests {
    use crate::byte_util::cast_slice;
    use crate::error::CoreErrorReason;

    #[test]
    fn test_empty_cast_slice() {
        let b1: [u8; 0] = [];
        let u16s: &[u16] = cast_slice(&b1).unwrap();
        let expected: &[u16] = &[];
        assert_eq!(u16s, expected);
    }

    #[test]
    fn test_cast_slice() {
        let b1 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

        let u16s: &[u16] = cast_slice(&b1).unwrap();
        assert_eq!(u16s, &[256, 770, 1284, 1798, 2312, 2826]);

        let u32s: &[u32] = cast_slice(&b1).unwrap();
        assert_eq!(u32s, &[50462976, 117835012, 185207048]);

        let e1 = cast_slice::<()>(&b1).unwrap_err();
        assert!(matches!(e1.reason(), CoreErrorReason::InvalidLayout));
        let e1msg = format!("{e1}");
        assert_eq!(e1msg, "target type () has zero size");

        let e2 = cast_slice::<u64>(&b1).unwrap_err();
        assert!(matches!(e2.reason(), CoreErrorReason::InvalidLayout));
        let e2msg = format!("{e2}");
        assert_eq!(
            e2msg,
            "size 12 is not divisible by target type u64 size of 8 bytes"
        );

        let e3 = cast_slice::<u32>(&b1[1..9]).unwrap_err();
        assert!(matches!(e3.reason(), CoreErrorReason::InvalidLayout));
        let e3msg = format!("{e3}");
        assert!(e3msg.contains("is not aligned to target type u32 alignment of 4 bytes"));
    }
}
