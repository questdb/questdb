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

import org.jetbrains.annotations.NotNull;

/**
 * A view on top of an ASCII-only {@link Utf8Sequence}.
 */
public class AsciiCharSequence implements CharSequence {
    private int len;
    private Utf8Sequence original;
    private int start;
    private AsciiCharSequence subSequence;

    @Override
    public char charAt(int i) {
        return (char) original.byteAt(i + start);
    }

    @Override
    public int length() {
        return len;
    }

    public AsciiCharSequence of(Utf8Sequence original) {
        this.original = original;
        this.start = 0;
        this.len = original.size();
        return this;
    }

    public AsciiCharSequence of(Utf8Sequence original, int start, int len) {
        this.original = original;
        this.start = start;
        this.len = len;
        return this;
    }

    @Override
    public @NotNull CharSequence subSequence(int start, int end) {
        if (subSequence == null) {
            subSequence = new AsciiCharSequence();
        }
        return subSequence.of(original, start, end - start);
    }

    @Override
    public @NotNull String toString() {
        return original.toString();
    }
}
