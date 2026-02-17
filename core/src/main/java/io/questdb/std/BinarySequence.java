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

package io.questdb.std;

public interface BinarySequence {

    byte byteAt(long index);

    /**
     * Copies bytes from this binary sequence to buffer.
     *
     * @param address target buffer address
     * @param start   offset in binary sequence to start copying from
     * @param length  number of bytes to copy
     */
    default void copyTo(long address, long start, long length) {
        final long n = Math.min(length() - start, length);
        for (long l = 0; l < n; l++) {
            Unsafe.getUnsafe().putByte(address + l, byteAt(start + l));
        }
    }

    long length();
}
