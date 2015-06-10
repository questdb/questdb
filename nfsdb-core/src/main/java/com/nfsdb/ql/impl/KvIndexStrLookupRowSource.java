/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.ql.impl;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.ops.VirtualColumn;
import com.nfsdb.storage.IndexCursor;
import com.nfsdb.storage.KVIndex;
import com.nfsdb.storage.VariableColumn;
import com.nfsdb.utils.Hash;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class KvIndexStrLookupRowSource extends AbstractRowSource {

    private final String columnName;
    private final VirtualColumn valueFunction;
    private final boolean newCursor;
    private IndexCursor indexCursor;
    private long lo;
    private long hi;
    private long rowid;
    private CharSequence currentValue;
    private VariableColumn column;
    private int buckets;
    private int columnIndex;
    private boolean hasNext = false;

    public KvIndexStrLookupRowSource(String columnName, VirtualColumn valueFunction) {
        this(columnName, valueFunction, false);
    }

    public KvIndexStrLookupRowSource(String columnName, VirtualColumn valueFunction, boolean newCursor) {
        this.columnName = columnName;
        this.valueFunction = valueFunction;
        this.newCursor = newCursor;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(columnName);
        this.buckets = metadata.getColumn(columnIndex).distinctCountHint;
    }

    @Override
    public boolean hasNext() {

        if (hasNext) {
            return true;
        }

        if (indexCursor != null) {
            while (indexCursor.hasNext()) {
                long r = indexCursor.next();
                if (r > lo && r < hi && column.cmpStr(r, currentValue)) {
                    this.rowid = r;
                    return hasNext = true;
                }
            }
        }

        return false;
    }

    @Override
    public long next() {
        hasNext = false;
        return rowid;
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            this.column = (VariableColumn) slice.partition.getAbstractColumn(columnIndex);
            KVIndex index = slice.partition.getIndexForColumn(columnIndex);
            CharSequence cs = valueFunction.getFlyweightStr(null);
            this.currentValue = cs == null ? null : cs.toString();
            this.indexCursor = newCursor ? index.newFwdCursor(Hash.boundedHash(currentValue, buckets)) : index.fwdCursor(Hash.boundedHash(currentValue, buckets));
            this.lo = slice.lo - 1;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
            this.hasNext = false;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return this;
    }

    @Override
    public void reset() {
        indexCursor = null;
        hasNext = false;
    }
}
