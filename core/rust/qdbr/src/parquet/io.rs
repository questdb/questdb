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
use std::fs::File;

pub trait FromRawFdI32Ext {
    unsafe fn from_raw_fd_i32(raw: i32) -> Self;
}

impl FromRawFdI32Ext for File {
    unsafe fn from_raw_fd_i32(raw: i32) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::io::{FromRawFd, RawFd};
            File::from_raw_fd(raw as RawFd)
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::{FromRawHandle, RawHandle};
            File::from_raw_handle(raw as usize as RawHandle)
        }
    }
}
