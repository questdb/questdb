/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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

package com.questdb.ql.impl.latest;

import com.questdb.ex.JournalException;
import com.questdb.ex.JournalRuntimeException;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.ql.CancellationHandler;
import com.questdb.ql.PartitionSlice;
import com.questdb.ql.RowCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.AbstractRowSource;
import com.questdb.std.CharSink;
import com.questdb.store.IndexCursor;
import com.questdb.store.KVIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class KvIndexSymLookupRowSource extends AbstractRowSource {

    private final String symbol;
    private final String value;
    private final boolean newCursor;
    private int columnIndex;
    private int symbolKey = -2;
    private IndexCursor indexCursor;
    private long lo;
    private long hi;
    private boolean full;
    private long rowid;
    private boolean hasNext = false;

    public KvIndexSymLookupRowSource(String symbol, String value) {
        this(symbol, value, false);
    }

    public KvIndexSymLookupRowSource(String symbol, String value, boolean newCursor) {
        this.symbol = symbol;
        this.value = value;
        this.newCursor = newCursor;
    }

    @Override
    public void configure(JournalMetadata metadata) {
        this.columnIndex = metadata.getColumnIndex(symbol);
    }

    @Override
    public RowCursor prepareCursor(PartitionSlice slice) {
        try {
            KVIndex index = slice.partition.getIndexForColumn(columnIndex);
            this.indexCursor = newCursor ? index.newFwdCursor(symbolKey) : index.fwdCursor(symbolKey);
            this.full = slice.lo == 0 && slice.calcHi;
            this.lo = slice.lo - 1;
            this.hi = slice.calcHi ? slice.partition.open().size() : slice.hi + 1;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
        return this;
    }

    @Override
    public void reset() {
    }

    @Override
    public boolean hasNext() {

        if (hasNext) {
            return true;
        }

        if (indexCursor != null && indexCursor.hasNext()) {
            if (full) {
                this.rowid = indexCursor.next();
                return hasNext = true;
            }

            do {
                long rowid = indexCursor.next();
                if (rowid > lo && rowid < hi) {
                    this.rowid = rowid;
                    return hasNext = true;
                }
            } while (indexCursor.hasNext());
        }

        return false;
    }

    @Override
    public long next() {
        hasNext = false;
        return rowid;
    }

    @Override
    public void prepare(StorageFacade facade, CancellationHandler cancellationHandler) {
        symbolKey = facade.getSymbolTable(symbol).getQuick(value);
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("KvIndexSymLookupRowSource").put(',');
        sink.putQuoted("symbol").put(':').putQuoted(symbol).put(',');
        sink.putQuoted("value").put(':').putQuoted(value);
        sink.put('}');
    }
}
