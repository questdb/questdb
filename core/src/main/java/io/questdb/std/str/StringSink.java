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

import io.questdb.std.Chars;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class StringSink extends AbstractCharSink implements MutableCharSink, CharSequence, CloneableMutable {

    private char[] buffer;
    private int pos;

    public StringSink() {
        this(16);
    }

    public StringSink(int initialCapacity) {
        this.buffer = new char[initialCapacity];
        this.pos = 0;
    }

    @Override
    public char charAt(int index) {
        return buffer[index];
    }

    public void clear(int pos) {
        this.pos = pos;
    }

    @Override
    public void clear() {
        clear(0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T copy() {
        return (T) new String(buffer, 0, pos);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CharSequence) {
            CharSequence cs = (CharSequence) obj;
            int len = cs.length();
            if (len == pos) {
                for (int i = 0; i < len; i++) {
                    if (buffer[i] != cs.charAt(i)) {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Chars.hashCode(buffer, 0, pos);
    }

    public int indexOf(@NotNull String s) {
        return Chars.indexOf(this, 0, pos, s);
    }

    public int indexOf(@NotNull String s, int fromIndex) {
        return Chars.indexOf(this, Math.min(fromIndex, pos), pos, s);
    }

    public int lastIndexOf(@NotNull String s) {
        return Chars.lastIndexOf(this, 0, pos, s);
    }

    public int lastIndexOf(@NotNull String s, int fromIndex) {
        return Chars.lastIndexOf(this, 0, Math.min(fromIndex + s.length(), pos), s);
    }

    @Override
    public int length() {
        return pos;
    }

    @Override
    public CharSink put(@Nullable CharSequence cs) {
        if (cs != null) {
            int len = cs.length();
            checkSize(len);
            for (int i = 0; i < len; i++) {
                buffer[pos + i] = cs.charAt(i);
            }
            pos += len;
        }
        return this;
    }

    @Override
    public CharSink put(@NotNull CharSequence cs, int lo, int hi) {
        int len = hi - lo;
        checkSize(len);
        for (int i = lo; i < hi; i++) {
            buffer[pos + i - lo] = cs.charAt(i);
        }
        pos += len;
        return this;
    }

    @Override
    public CharSink put(char c) {
        checkSize(1);
        buffer[pos++] = c;
        return this;
    }

    @Override
    public CharSink put(char @NotNull [] chars, int start, int len) {
        checkSize(len);
        System.arraycopy(chars, start, buffer, pos, len);
        pos += len;
        return this;
    }

    public CharSink put(char c, int n) {
        checkSize(n);
        for (int i = 0; i < n; i++) {
            buffer[pos + i] = c;
        }
        pos += n;
        return this;
    }

    public void setCharAt(int index, char ch) {
        buffer[index] = ch;
    }

    @Override
    public @NotNull CharSequence subSequence(int lo, int hi) {
        return new String(buffer, lo, hi - lo);
    }

    /* Either IDEA or FireBug complain, annotation galore */
    @NotNull
    @Override
    public String toString() {
        return new String(buffer, 0, pos);
    }

    public void trimTo(int pos) {
        clear(pos);
    }

    private void checkSize(int extra) {
        int len = pos + extra;
        if (buffer.length > len) {
            return;
        }
        len = Math.max(pos * 2, len);
        final char[] n = new char[len];
        System.arraycopy(buffer, 0, n, 0, pos);
        buffer = n;
    }
}
