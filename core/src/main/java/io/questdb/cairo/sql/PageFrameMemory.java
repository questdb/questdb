/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.cairo.sql;

import io.questdb.std.LongList;

/**
 * Represents page frame as a set of per column contiguous memory.
 * For native partitions, it's simply a slice of mmapped memory.
 * For Parquet partitions, it's a deserialized in-memory native format.
 */
public interface PageFrameMemory {

    long getAuxPageAddress(int columnIndex);

    LongList getAuxPageAddresses();

    LongList getAuxPageSizes();

    int getColumnCount();

    byte getFrameFormat();

    int getFrameIndex();

    long getPageAddress(int columnIndex);

    LongList getPageAddresses();

    long getPageSize(int columnIndex);

    LongList getPageSizes();

    long getRowIdOffset();

    boolean hasColumnTops();
}
