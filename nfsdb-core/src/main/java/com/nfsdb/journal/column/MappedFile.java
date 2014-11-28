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

package com.nfsdb.journal.column;

import com.nfsdb.journal.exceptions.JournalException;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface MappedFile extends Closeable {
    public ByteBuffer getBuffer(long offset, int size);

    long getAddress(long offset, int size);

    int getAddressSize(long offset);

    /**
     * Suppress exception.
     */
    void close();

    long getAppendOffset();

    void setAppendOffset(long offset);

    void compact() throws JournalException;

    void force();
}
