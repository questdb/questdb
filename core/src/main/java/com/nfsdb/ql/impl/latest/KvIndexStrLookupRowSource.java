/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.latest;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.misc.Hash;
import com.nfsdb.ql.PartitionSlice;
import com.nfsdb.ql.RowCursor;
import com.nfsdb.ql.impl.AbstractRowSource;
import com.nfsdb.store.IndexCursor;
import com.nfsdb.store.KVIndex;
import com.nfsdb.store.VariableColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class KvIndexStrLookupRowSource extends AbstractRowSource {

    private final String columnName;
    private final String value;
    private final boolean newCursor;
    private int hash;
    private IndexCursor indexCursor;
    private long lo;
    private long hi;
    private long rowid;
    private VariableColumn column;
    private int columnIndex;
    private boolean hasNext = false;

    public KvIndexStrLookupRowSource(String columnName, String value) {
        this(columnName, value, false);
    }

    public KvIndexStrLookupRowSource(String columnName, String value, boolean newCursor) {
        this.columnName = columnName;
        this.value = value;
        this.newCursor = newCursor;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(columnName);
        this.hash = Hash.boundedHash(value, metadata.getColumnQuick(columnIndex).distinctCountHint);
    }

    @SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            this.column = slice.partition.varCol(columnIndex);
            KVIndex index = slice.partition.getIndexForColumn(columnIndex);
            this.indexCursor = newCursor ? index.newFwdCursor(hash) : index.fwdCursor(hash);
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

    @Override
    public boolean hasNext() {

        if (hasNext) {
            return true;
        }

        if (indexCursor != null) {
            while (indexCursor.hasNext()) {
                long r = indexCursor.next();
                if (r > lo && r < hi && column.cmpStr(r, value)) {
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
}
