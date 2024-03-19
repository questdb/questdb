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

package io.questdb.cairo.frm;

import java.io.Closeable;

public interface FrameColumn extends Closeable {
    int COLUMN_CONTIGUOUS_FILE = 0;

    void addTop(long value);

    /**
     * Appends source frame to this frame starting at the specific offset in this frame.
     *
     * @param appendOffsetRowCount offset in number of rows after which data is appended
     * @param sourceColumn         the source frame
     * @param sourceLo             low index in the source frame
     * @param sourceHi             high index in the source frame, exclusive
     * @param commitMode           the commit mode, which drives durability of the change.
     */
    void append(long appendOffsetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode);

    void appendNulls(long rowCount, long sourceColumnTop, int commitMode);

    void close();

    int getColumnIndex();

    long getColumnTop();

    int getColumnType();

    int getPrimaryFd();

    int getSecondaryFd();

    int getStorageType();
}
