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

public class DeletedFrameColumn implements FrameColumn {
    public static final FrameColumn INSTANCE = new DeletedFrameColumn();

    @Override
    public void addTop(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void append(long offset, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendNulls(long offset, long count, int commitMode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }

    @Override
    public int getColumnIndex() {
        return -1;
    }

    @Override
    public long getColumnTop() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getColumnType() {
        return -1;
    }

    @Override
    public int getPrimaryFd() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSecondaryFd() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getStorageType() {
        throw new UnsupportedOperationException();
    }
}
