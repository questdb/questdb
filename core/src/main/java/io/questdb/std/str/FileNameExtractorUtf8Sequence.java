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

package io.questdb.std.str;

import io.questdb.std.Files;
import io.questdb.std.ThreadLocal;
import org.jetbrains.annotations.NotNull;

public class FileNameExtractorUtf8Sequence implements Utf8Sequence {

    private final static ThreadLocal<FileNameExtractorUtf8Sequence> SINGLETON = new ThreadLocal<>(FileNameExtractorUtf8Sequence::new);
    private Utf8Sequence base;
    private int hi;
    private int lo;

    public static Utf8Sequence get(Utf8Sequence that) {
        return SINGLETON.get().of(that);
    }

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte byteAt(int index) {
        return base.byteAt(lo + index);
    }

    @Override
    public boolean isAscii() {
        return base.isAscii();
    }

    public Utf8Sequence of(Utf8Sequence base) {
        this.base = base;
        this.hi = base.size();
        this.lo = 0;
        for (int i = hi - 1; i > -1; i--) {
            if (base.byteAt(i) == Files.SEPARATOR) {
                this.lo = i + 1;
                break;
            }
        }
        return this;
    }

    @Override
    public int size() {
        return hi - lo;
    }

    @Override
    public @NotNull String toString() {
        return Utf8s.stringFromUtf8Bytes(lo, hi);
    }
}
