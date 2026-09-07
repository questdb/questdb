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
 * A {@link ColumnTopSink} that records every {@code (columnIndex, columnTop)} report into a plain {@link LongList}
 * instead of forwarding to a live {@link ColumnVersionWriter}.
 */
public class ColumnTopRecorder implements ColumnTopSink {
    private static final long NOT_REPORTED = -1L;
    // Index = column index, NOT_REPORTED = this column never reported.
    private final LongList tops = new LongList();

    public void clear() {
        tops.clear();
    }

    public boolean isEmpty() {
        for (int i = 0, n = tops.size(); i < n; i++) {
            if (tops.getQuick(i) != NOT_REPORTED) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isThreadSafe() {
        return true;
    }

    @Override
    public void ofColumnCount(int columnCount) {
        tops.setPos(columnCount);
        tops.fill(0, columnCount, NOT_REPORTED);
    }

    /**
     * Pushes every recorded top into {@code sink}, in column order.
     */
    public void pushInto(ColumnTopSink sink) {
        for (int i = 0, n = tops.size(); i < n; i++) {
            final long columnTop = tops.getQuick(i);
            if (columnTop != NOT_REPORTED) {
                sink.setColumnTop(i, columnTop);
            }
        }
    }

    @Override
    public void setColumnTop(int columnIndex, long columnTop) {
        tops.setQuick(columnIndex, columnTop);
    }
}
