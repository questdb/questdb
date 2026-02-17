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

#[cfg(target_os = "windows")]
#[no_mangle]
pub extern "system" fn Java_io_questdb_std_Files_isDir(
    _env: jni::JNIEnv,
    _class: jni::objects::JClass,
    path: *const std::ffi::c_char,
) -> bool {
    // This is a Windows specific implementation
    // The other implementations are done in C.
    // This implementation avoids the complexity of dealing with reparse points in the Windows API.
    // See https://stackoverflow.com/questions/46383428/get-the-immediate-target-path-from-symlink-reparse-point
    // for more context.
    let path = unsafe { std::ffi::CStr::from_ptr(path) };
    let Ok(path) = path.to_str() else {
        return false;
    };
    let path = std::path::Path::new(path);
    if path.is_dir() {
        return true;
    }
    let Ok(mut resolved) = std::path::Path::new(path).read_link() else {
        return false;
    };
    if resolved.is_dir() {
        return true;
    }
    loop {
        let Ok(newly_resolved) = resolved.read_link() else {
            return false;
        };
        if newly_resolved.is_dir() {
            return true;
        }
        resolved = newly_resolved;
    }
}
