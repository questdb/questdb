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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.ColumnFilter;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.std.IntList;
import io.questdb.std.Misc;

public abstract class AbstractSymbolWrapOverCursor implements NoRandomAccessRecordCursor {
    protected final int masterSlaveSplit;
    protected final long masterTimestampScale;
    protected final long slaveTimestampScale;
    private final ColumnFilter masterTableKeyColumns;
    private final IntList slaveColumnIndex;
    private final int slaveWrappedOverMaster;
    protected RecordCursor masterCursor;
    protected RecordCursor slaveCursor;


    public AbstractSymbolWrapOverCursor(int masterSlaveSplit,
                                        int slaveWrappedOverMaster,
                                        ColumnFilter masterTableKeyColumns,
                                        IntList slaveColumnIndex,
                                        int masterTimestampType,
                                        int slaveTimestampType) {
        this.masterSlaveSplit = masterSlaveSplit;
        this.slaveWrappedOverMaster = slaveWrappedOverMaster;
        this.masterTableKeyColumns = masterTableKeyColumns;
        this.slaveColumnIndex = slaveColumnIndex;
        if (masterTimestampType == slaveTimestampType) {
            masterTimestampScale = slaveTimestampScale = 1L;
        } else {
            masterTimestampScale = ColumnType.getTimestampDriver(masterTimestampType).toNanosScale();
            slaveTimestampScale = ColumnType.getTimestampDriver(slaveTimestampType).toNanosScale();
        }
    }

    @Override
    public void close() {
        masterCursor = Misc.free(masterCursor);
        slaveCursor = Misc.free(slaveCursor);
    }

    @Override
    public SymbolTable getSymbolTable(int columnIndex) {
        if (columnIndex < masterSlaveSplit) {
            return masterCursor.getSymbolTable(columnIndex);
        }
        int slaveCol = columnIndex - masterSlaveSplit;
        if (slaveCol >= slaveWrappedOverMaster) {
            slaveCol -= slaveWrappedOverMaster;
            int masterCol = masterTableKeyColumns.getColumnIndexFactored(slaveCol);
            return masterCursor.getSymbolTable(masterCol);
        }
        slaveCol = slaveColumnIndex.getQuick(slaveCol);
        return slaveCursor.getSymbolTable(slaveCol);
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        // everything before columnSplit is from master. all columns are mapped 1:1.
        if (columnIndex < masterSlaveSplit) {
            return masterCursor.newSymbolTable(columnIndex);
        }

        // everything after is from slave
        int slaveCol = columnIndex - masterSlaveSplit;
        if (slaveCol >= slaveWrappedOverMaster) {
            // ok, this is technically a slave column, but we get the symbol table from a master cursor.
            // why? key symbols from the slave cursor are converted to strings before
            // copying them to the map used as slave record. this means symbol IDs are lost.
            // so let's use the fact it's a join key column and keys are present in both master and slave cursors
            // this is a bit of a hack, but it works.
            // SymbolWrapOverJoinRecord getInt() is aware of this logic.
            slaveCol -= slaveWrappedOverMaster;
            int masterCol = masterTableKeyColumns.getColumnIndexFactored(slaveCol);
            return masterCursor.newSymbolTable(masterCol);
        }
        slaveCol = slaveColumnIndex.getQuick(slaveCol);
        return slaveCursor.newSymbolTable(slaveCol);
    }
}
