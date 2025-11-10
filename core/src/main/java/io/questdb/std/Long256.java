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

package io.questdb.std;

/**
 * A 256-bit hash with string representation up to 64 hex digits following a prefix '0x'.
 * (e.g. 0xaba86bf575ba7fde98b6673bb7d85bf489fd71a619cddaecba5de0378e3d22ed)
 */
public interface Long256 extends Long256Acceptor {
    int BYTES = 32;

    static void putLong256(Long256 value, long p) {
        Unsafe.getUnsafe().putLong(p, value.getLong0());
        Unsafe.getUnsafe().putLong(p + 8L, value.getLong1());
        Unsafe.getUnsafe().putLong(p + 16L, value.getLong2());
        Unsafe.getUnsafe().putLong(p + 24L, value.getLong3());
    }

    long getLong0();

    long getLong1();

    long getLong2();

    long getLong3();
}
