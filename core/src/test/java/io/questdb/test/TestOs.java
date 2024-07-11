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

package io.questdb.test;

import io.questdb.std.Os;

import java.io.InputStream;

public class TestOs {
    public static void init() {
    }

    // This is copy of main Os class to load test jni library
    static {
        String outputLibExt;
        if (Os.type == Os.LINUX || Os.type == Os.FREEBSD) {
            outputLibExt = ".so";
        } else if (Os.type == Os.DARWIN) {
            outputLibExt = ".dylib";
        } else if (Os.type == Os.WINDOWS) {
            outputLibExt = ".dll";
        } else {
            throw new Error("Unsupported OS: " + Os.name);
        }

        String devRustLibRoot = "/io/questdb/rust/";
        final String rustLibName;
        if (Os.type == Os.WINDOWS) {
            rustLibName = "questdbrtest" + outputLibExt;
        } else {
            rustLibName = "libquestdbrtest" + outputLibExt;
        }

        final String devRustLib = devRustLibRoot + rustLibName;
        InputStream libRustStream = TestOs.class.getResourceAsStream(devRustLib);
        String prdLibRoot = "/io/questdb/bin/" + Os.name + '-' + Os.archName + '/';
        if (libRustStream == null) {
            InputStream is = TestOs.class.getResourceAsStream(prdLibRoot + rustLibName);
            Os.loadLib(devRustLib, is);
        } else {
            Os.loadLib(devRustLib, libRustStream);
        }
    }
}
