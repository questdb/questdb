/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.table;

import com.questdb.cairo.RecordSink;
import com.questdb.cairo.map.Map;
import com.questdb.cairo.map.MapKey;
import com.questdb.cairo.sql.DataFrame;
import com.questdb.cairo.sql.Function;
import com.questdb.griffin.SqlExecutionContext;
import com.questdb.std.Rows;
import org.jetbrains.annotations.NotNull;

class LatestByAllFilteredRecordCursor extends AbstractTreeSetRecordCursor {

    private final Map map;
    private final RecordSink recordSink;
    private final Function filter;

    public LatestByAllFilteredRecordCursor(
            @NotNull Map map,
            @NotNull LongTreeSet treeSet,
            @NotNull RecordSink recordSink,
            @NotNull Function filter) {
        super(treeSet);
        this.map = map;
        this.recordSink = recordSink;
        this.filter = filter;
    }

    @Override
    public void close() {
        filter.close();
        super.close();
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) {
        map.clear();
        filter.init(this, executionContext);

        while (this.dataFrameCursor.hasNext()) {
            final DataFrame frame = this.dataFrameCursor.next();
            final int partitionIndex = frame.getPartitionIndex();
            final long rowLo = frame.getRowLo();
            final long rowHi = frame.getRowHi() - 1;

            record.jumpTo(frame.getPartitionIndex(), rowHi);
            for (long row = rowHi; row >= rowLo; row--) {
                record.setRecordIndex(row);
                if (filter.getBool(record)) {
                    MapKey key = map.withKey();
                    key.put(record, recordSink);
                    if (key.create()) {
                        treeSet.put(Rows.toRowID(partitionIndex, row));
                    }
                }
            }
        }

        map.clear();
    }
}
