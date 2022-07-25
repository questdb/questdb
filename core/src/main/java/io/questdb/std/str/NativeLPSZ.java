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

package io.questdb.std.str;

import io.questdb.std.Unsafe;

/**
 * Represents C LPSZ as Java' CharSequence. Byes in native memory are interpreted as ASCII characters. Multi-byte
 * characters are NOT decoded.
 */
public class NativeLPSZ extends AbstractCharSequence {
    private long address;
    private int len;

    @Override
    public int length() {
        return len;
    }

    @Override
    public char charAt(int index) {
        return (char) Unsafe.getUnsafe().getByte(address + index);
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public NativeLPSZ of(long address) {
        this.address = address;
        long p = address;
        while (Unsafe.getUnsafe().getByte(p++) != 0) ;
        this.len = (int) (p - address - 1);
        return this;
    }
}