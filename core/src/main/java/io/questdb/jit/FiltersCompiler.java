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

package io.questdb.jit;

import io.questdb.std.str.StringSink;

final class FiltersCompiler {

    private FiltersCompiler() {
    }

    public static native long callCountOnlyFunction(
            long fnAddress,
            long colsAddress,
            long colsSize,
            long varSizeIndexesAddress,
            long varsAddress,
            long varsSize,
            long rowsCount
    );

    public static native long callFunction(
            long fnAddress,
            long colsAddress,
            long colsSize,
            long varSizeIndexesAddress,
            long varsAddress,
            long varsSize,
            long filteredRowsAddress,
            long rowsCount
    );

    public static native long compileCountOnlyFunction(long filterAddress, long filterSize, int options, JitError error);

    public static native long compileFunction(long filterAddress, long filterSize, int options, JitError error);

    public static native long freeFunction(long fnAddress);

    static class JitError {
        private final StringSink message = new StringSink();
        private int errorCode = 0;

        public int errorCode() {
            return errorCode;
        }

        public CharSequence message() {
            return message.subSequence(0, message.length());
        }

        // We are not going to allocate and convert strings, so instead
        // we fill it char by char (ASCII char) from the C++ side.
        @SuppressWarnings("unused")
        public void put(byte b) {
            message.put((char) b);
        }

        public void reset() {
            errorCode = 0;
            message.clear();
        }
    }
}
