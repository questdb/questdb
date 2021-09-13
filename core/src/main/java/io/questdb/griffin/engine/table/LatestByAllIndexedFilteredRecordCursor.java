/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.sql.Function;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntList;
import io.questdb.std.Rows;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

class LatestByAllIndexedFilteredRecordCursor extends LatestByAllIndexedRecordCursor {
    protected final Function filter;

    public LatestByAllIndexedFilteredRecordCursor(
            int columnIndex,
            @NotNull DirectLongList rows,
            @NotNull Function filter,
            @NotNull IntList columnIndexes,
            @NotNull DirectLongList prefixes
    ) {
        super(columnIndex, rows, columnIndexes, prefixes);
        this.filter = filter;
    }

    @Override
    public void close() {
        filter.close();
        super.close();
    }

    @Override
    protected void buildTreeMap(SqlExecutionContext executionContext) throws SqlException {
        filter.init(this, executionContext);
        super.buildTreeMap(executionContext);
    }

    @Override
    protected void postProcessRows() {
        final long rowCount = aLimit;
        rows.setPos(rowCount);

        for (long r = 0; r < rowCount; ++r) {
            long row = rows.get(r) - 1;
            recordA.jumpTo(Rows.toPartitionIndex(row), Rows.toLocalRowID(row));
            if (!filter.getBool(recordA)) {
                rows.set(r, 0); // clear row id
            }
        }

        Vect.sortULongAscInPlace(rows.getAddress(), rowCount);

        while (rows.get(indexShift) <= 0) {
            indexShift++;
        }

        aLimit = rowCount;
        aIndex = indexShift;
    }

}
