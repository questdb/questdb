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

import io.questdb.std.Chars;
import org.jetbrains.annotations.NotNull;

public class StringSink extends AbstractCharSink implements MutableCharSink, CloneableMutable {
    private final StringBuilder builder = new StringBuilder();

    public void clear(int pos) {
        builder.setLength(pos);
    }

    public void clear() {
        builder.setLength(0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T copy() {
        return (T) toString();
    }

    @Override
    public int hashCode() {
        return Chars.hashCode(builder);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CharSequence && Chars.equals(builder, (CharSequence) obj);
    }

    /* Either IDEA or FireBug complain, annotation galore */
    @NotNull
    @Override
    public String toString() {
        return builder.toString();
    }

    @Override
    public int length() {
        return builder.length();
    }

    @Override
    public char charAt(int index) {
        return builder.charAt(index);
    }

    @Override
    public CharSequence subSequence(int lo, int hi) {
        return builder.subSequence(lo, hi);
    }

    @Override
    public CharSink put(CharSequence cs) {
        if (cs != null) {
            builder.append(cs);
        }
        return this;
    }

    @Override
    public CharSink put(CharSequence cs, int lo, int hi) {
        builder.append(cs, lo, hi);
        return this;
    }

    @Override
    public CharSink put(char c) {
        builder.append(c);
        return this;
    }

    @Override
    public CharSink put(char[] chars, int start, int len) {
        builder.append(chars, start, len);
        return this;
    }

    public CharSink put(char c, int n) {
        //noinspection StringRepeatCanBeUsed
        for (int i = 0; i < n; i++) {
            builder.append(c);
        }
        return this;
    }

    public CharSink replace(int start, int end, String str) {
        builder.replace(start, end, str);
        return this;
    }
}
