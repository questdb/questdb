/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal;

import com.nfsdb.journal.exceptions.JournalException;

import java.io.InputStream;
import java.io.OutputStream;

public interface JournalEntryWriter {
    long getTimestamp();

    void put(int index, byte value);

    void putLong(int index, long value);

    void putDate(int index, long value);

    void putDouble(int index, double value);

    void putStr(int index, CharSequence value);

    void putSym(int index, String value);

    void putBin(int index, InputStream value);

    OutputStream putBin(int index);

    void putNull(int index);

    void putInt(int index, int value);

    void putBool(int index, boolean value);

    void append() throws JournalException;

    void putShort(int index, short value);
}
