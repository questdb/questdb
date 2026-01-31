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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.EmptyRowCursor;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameMemory;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.cairo.sql.RowCursorFactory;
import io.questdb.griffin.PlanSink;

public class LatestByValueIndexedRowCursorFactory implements RowCursorFactory {
    private final boolean cachedIndexReaderCursor;
    private final int columnIndex;
    private final LatestByValueIndexedRowCursor cursor = new LatestByValueIndexedRowCursor();
    private final int symbolKey;

    public LatestByValueIndexedRowCursorFactory(int columnIndex, int symbolKey, boolean cachedIndexReaderCursor) {
        this.columnIndex = columnIndex;
        this.symbolKey = TableUtils.toIndexKey(symbolKey);
        this.cachedIndexReaderCursor = cachedIndexReaderCursor;
    }

    @Override
    public RowCursor getCursor(PageFrame pageFrame, PageFrameMemory pageFrameMemory) {
        final RowCursor indexReaderCursor = pageFrame
                .getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD)
                .getCursor(cachedIndexReaderCursor, symbolKey, pageFrame.getPartitionLo(), pageFrame.getPartitionHi() - 1);

        if (indexReaderCursor.hasNext()) {
            cursor.of(indexReaderCursor.next());
            return cursor;
        }
        return EmptyRowCursor.INSTANCE;
    }

    @Override
    public boolean isEntity() {
        return false;
    }

    @Override
    public boolean isUsingIndex() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index ").type(BitmapIndexReader.NAME_BACKWARD).type(" scan").meta("on").putBaseColumnName(columnIndex);
        sink.attr("filter").putBaseColumnName(columnIndex).val('=').val(symbolKey);
    }
}
