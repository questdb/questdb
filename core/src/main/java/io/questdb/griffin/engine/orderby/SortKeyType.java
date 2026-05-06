/*+*****************************************************************************
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

package io.questdb.griffin.engine.orderby;

public enum SortKeyType {
    FIXED_8(8),
    FIXED_16(16),
    FIXED_24(24),
    FIXED_32(32),
    UNSUPPORTED(-1);

    private final int keyLength;

    SortKeyType(int keyLength) {
        this.keyLength = keyLength;
    }

    public static SortKeyType fromKeyLength(int keyLength) {
        assert keyLength >= 0;
        if (keyLength <= 8) {
            return FIXED_8;
        }
        if (keyLength <= 16) {
            return FIXED_16;
        }
        if (keyLength <= 24) {
            return FIXED_24;
        }
        if (keyLength <= 32) {
            return FIXED_32;
        }
        return UNSUPPORTED;
    }

    public int entrySize() {
        return keyLength + 8;
    }

    public int keyLength() {
        return keyLength;
    }

    public int rowIdOffset() {
        return keyLength;
    }
}
