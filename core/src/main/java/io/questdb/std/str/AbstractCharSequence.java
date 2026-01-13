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

import io.questdb.std.Chars;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract base class for CharSequence implementations.
 */
public abstract class AbstractCharSequence implements CharSequence, CloneableMutable {

    /**
     * Converts a CharSequence to a String.
     *
     * @param cs the CharSequence to convert
     * @return the String representation
     */
    public static String getString(CharSequence cs) {
        final Utf16Sink b = Misc.getThreadLocalSink();
        b.put(cs);
        return b.toString();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T copy() {
        return (T) AbstractCharSequence.getString(this);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof CharSequence && Chars.equals(this, (CharSequence) obj);
    }

    @Override
    public int hashCode() {
        return Chars.hashCode(this);
    }

    @Override
    public final @NotNull CharSequence subSequence(int start, int end) {
        if (start == 0 && end == length()) {
            return this;
        }
        if (start == end) {
            return "";
        }
        return _subSequence(start, end);
    }

    @NotNull
    @Override
    public String toString() {
        return getString(this);
    }

    /**
     * Returns a subsequence of this character sequence.
     *
     * @param start the start index
     * @param end   the end index
     * @return the subsequence
     */
    protected CharSequence _subSequence(int start, int end) {
        throw new UnsupportedOperationException();
    }
}
