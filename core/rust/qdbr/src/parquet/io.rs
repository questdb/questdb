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
use std::fs::File;
use std::io;
use std::mem::ManuallyDrop;

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

/// A file-like object that does not own the file descriptor.
pub struct NonOwningFile {
    file: ManuallyDrop<File>,
}

impl NonOwningFile {
    pub fn new(file: File) -> Self {
        Self { file: ManuallyDrop::new(file) }
    }

    #[cfg(unix)]
    pub fn as_raw_fd_i32(&self) -> i32 {
        use std::os::fd::AsRawFd;
        self.file.as_raw_fd()
    }

    #[cfg(windows)]
    pub fn as_raw_fd_i32(&self) -> i32 {
        use std::os::windows::io::AsRawHandle;
        self.file.as_raw_handle() as i32
    }
}

impl io::Read for NonOwningFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.file.read_vectored(bufs)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.file.read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        self.file.read_to_string(buf)
    }
}

impl io::Seek for NonOwningFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}
