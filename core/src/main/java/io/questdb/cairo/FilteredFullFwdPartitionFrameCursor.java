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

package io.questdb.cairo;

import io.questdb.cairo.filter.SkipFilterReader;
import io.questdb.cairo.sql.PartitionFrame;
import io.questdb.cairo.sql.RecordCursor;
import org.jetbrains.annotations.Nullable;

public class FilteredFullFwdPartitionFrameCursor extends FullFwdPartitionFrameCursor {
    private final int filterColumnIndex;
    private final long filterKeyHash;

    public FilteredFullFwdPartitionFrameCursor(int filterColumnIndex, long filterKeyHash) {
        this.filterColumnIndex = filterColumnIndex;
        this.filterKeyHash = filterKeyHash;
    }

    @Override
    public void calculateSize(RecordCursor.Counter counter) {
        SkipFilterReader filterReader;
        boolean maybeContains;
        while (partitionIndex < partitionHi) {
            final long hi = reader.openPartition(partitionIndex);
            filterReader = reader.getSkipFilterReader(partitionIndex, filterColumnIndex);
            maybeContains = filterReader.maybeContainsHash(filterKeyHash);
            if (hi > 0 && maybeContains) {
                counter.add(hi);
            }
            partitionIndex++;
        }
    }

    @Override
    public @Nullable PartitionFrame next() {
        SkipFilterReader filterReader;
        boolean maybeContains;
        do {
            super.next();
            filterReader = reader.getSkipFilterReader(partitionIndex - 1, filterColumnIndex);
            maybeContains = filterReader.maybeContainsHash(filterKeyHash);
        } while (!maybeContains && partitionIndex < partitionHi);
        if (maybeContains) {
            return frame;
        } else {
            return null;
        }
    }

    @Override
    public long size() {
        return -1;
    }
}
