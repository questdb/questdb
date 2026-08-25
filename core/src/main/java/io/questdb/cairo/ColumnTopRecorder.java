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

package io.questdb.cairo;

import io.questdb.cairo.frm.ColumnTopSink;
import io.questdb.std.LongList;

/**
 * A {@link ColumnTopSink} that records every {@code (columnIndex, columnTop)} report into a plain
 * {@link LongList} instead of forwarding to a live {@link ColumnVersionWriter}. Lets a reader-based
 * partition build - see {@code PartitionCompactionScanJob} - capture a {@link io.questdb.cairo.frm.Frame}'s
 * self-tracked tops off the writer thread, the same way {@code ColumnVersionWriter} itself does when armed
 * (see {@link ColumnVersionWriter#asColumnTopSink}), then have the values pushed into the
 * real writer only once a later swap actually holds it. Two longs per entry, no boxing.
 */
public class ColumnTopRecorder implements ColumnTopSink {
    private final LongList entries = new LongList();

    public void clear() {
        entries.clear();
    }

    public boolean isEmpty() {
        return entries.size() == 0;
    }

    /**
     * Pushes every recorded pair into {@code sink}, in recorded order. The caller arms the target
     * partition first - e.g. {@link ColumnVersionWriter#asColumnTopSink} - the same
     * contract {@link io.questdb.cairo.frm.Frame#publishColumnTops} relies on for any sink.
     */
    public void pushInto(ColumnTopSink sink) {
        for (int i = 0, n = entries.size(); i < n; i += 2) {
            sink.setColumnTop((int) entries.getQuick(i), entries.getQuick(i + 1));
        }
    }

    @Override
    public void setColumnTop(int columnIndex, long columnTop) {
        entries.add(columnIndex, columnTop);
    }
}
