/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb;

import com.nfsdb.exceptions.JournalException;

import java.io.InputStream;
import java.io.OutputStream;

public interface JournalEntryWriter {
    void append() throws JournalException;

    long getTimestamp();

    void put(int index, byte value);

    void putBin(int index, InputStream value);

    OutputStream putBin(int index);

    void putBool(int index, boolean value);

    void putDate(int index, long value);

    void putDouble(int index, double value);

    void putFloat(int index, float value);

    void putInt(int index, int value);

    void putLong(int index, long value);

    void putNull(int index);

    void putShort(int index, short value);

    void putStr(int index, CharSequence value);

    void putSym(int index, CharSequence value);
}
