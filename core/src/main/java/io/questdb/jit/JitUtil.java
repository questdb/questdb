/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.jit;

import io.questdb.std.Os;

public final class JitUtil {

    private JitUtil() {
    }

    public static boolean isJitSupported() {
        // TODO: what about FREEBSD_ARM64?
        return Os.type != Os.LINUX_ARM64 &&
                Os.type != Os.OSX_ARM64 &&
                // TODO: excluding OSX_AMD64 as CI is failing on
                //  OS name: "mac os x", version: "11.6.4", arch: "x86_64", family: "mac"
                //  due to NATIVE_JIT_LONG_LIST leak, will revert if CI still fails
                Os.type != Os.OSX_AMD64 &&
                // TODO: excluding WINDOWS too
                //  OS name: "windows server 2022", version: "10.0", arch: "amd64", family: "windows"
                Os.type != Os.WINDOWS &&
                // TODO: excluding LINUX_AMD64 too
                //  OS name: "linux", version: "5.11.0-1028-azure", arch: "amd64", family: "unix"
                Os.type != Os.LINUX_AMD64
                ;
    }
}
