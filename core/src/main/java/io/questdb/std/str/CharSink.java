/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface CharSink extends CharSinkBase<CharSink> {

    default CharSink put(@Nullable Utf8Sequence us) {
        if (us != null) {
            Utf8s.utf8ToUtf16(us, this);
        }
        return this;
    }

    default CharSink put(long lo, long hi) {
        for (long addr = lo; addr < hi; addr += Character.BYTES) {
            put(Unsafe.getUnsafe().getChar(addr));
        }
        return this;
    }

    default CharSink put(char @NotNull [] chars, int start, int len) {
        for (int i = 0; i < len; i++) {
            put(chars[i + start]);
        }
        return this;
    }

    default CharSink putUtf8(long lo, long hi) {
        Utf8s.utf8ToUtf16(lo, hi, this);
        return this;
    }
}
