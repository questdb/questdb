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
 ******************************************************************************/

package com.questdb.ql.impl.analytic.prev;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Numbers;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;
import com.questdb.store.ColumnType;
import com.questdb.store.SymbolTable;

import java.io.OutputStream;

public class PrevRowIdValueNonPartAnalyticFunction implements AnalyticFunction {
    private final RecordColumnMetadataImpl valueMetadata;
    private final int valueIndex;
    private RecordCursor parent;
    private long prevRowId = -1;
    private long currentRowId = -1;
    private StorageFacade storageFacade;
    private ColumnType valueType;

    public PrevRowIdValueNonPartAnalyticFunction(RecordMetadata parentMetadata, String columnName, String alias) {
        this.valueIndex = parentMetadata.getColumnIndex(columnName);
        RecordColumnMetadata m = parentMetadata.getColumn(this.valueIndex);
        // metadata
        this.valueMetadata = new RecordColumnMetadataImpl(alias == null ? columnName : alias, this.valueType = m.getType());
    }

    @Override
    public byte get() {
        return prevRowId == -1 ? 0 : getParentRecord().get(valueIndex);
    }

    @Override
    public void getBin(OutputStream s) {
        if (prevRowId == -1) {
            return;
        }
        getParentRecord().getBin(valueIndex, s);
    }

    @Override
    public DirectInputStream getBin() {
        return prevRowId == -1 ? null : getParentRecord().getBin(valueIndex);
    }

    @Override
    public long getBinLen() {
        return prevRowId == -1 ? 0 : getParentRecord().getBinLen(valueIndex);
    }

    @Override
    public boolean getBool() {
        return prevRowId != -1 && getParentRecord().getBool(valueIndex);
    }

    @Override
    public long getDate() {
        return prevRowId == -1 ? Numbers.LONG_NaN : getParentRecord().getDate(valueIndex);
    }

    @Override
    public double getDouble() {
        return prevRowId == -1 ? Double.NaN : getParentRecord().getDouble(valueIndex);
    }

    @Override
    public float getFloat() {
        return prevRowId == -1 ? Float.NaN : getParentRecord().getFloat(valueIndex);
    }

    @Override
    public CharSequence getFlyweightStr() {
        return prevRowId == -1 ? null : getParentRecord().getFlyweightStr(valueIndex);
    }

    @Override
    public CharSequence getFlyweightStrB() {
        return prevRowId == -1 ? null : getParentRecord().getFlyweightStrB(valueIndex);
    }

    @Override
    public int getInt() {
        if (prevRowId == -1) {
            if (valueType == ColumnType.SYMBOL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            return Numbers.INT_NaN;
        }
        return getParentRecord().getInt(valueIndex);
    }

    @Override
    public long getLong() {
        return prevRowId == -1 ? Numbers.LONG_NaN : getParentRecord().getLong(valueIndex);
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueMetadata;
    }

    @Override
    public short getShort() {
        return prevRowId == -1 ? 0 : getParentRecord().getShort(valueIndex);
    }

    @Override
    public void getStr(CharSink sink) {
        if (prevRowId == -1) {
            sink.put((CharSequence) null);
        } else {
            getParentRecord().getStr(valueIndex, sink);
        }
    }

    @Override
    public CharSequence getStr() {
        return prevRowId == -1 ? null : getParentRecord().getStr(valueIndex);
    }

    @Override
    public int getStrLen() {
        return prevRowId == -1 ? 0 : getParentRecord().getStrLen(valueIndex);
    }

    @Override
    public String getSym() {
        return prevRowId == -1 ? null : getParentRecord().getSym(valueIndex);
    }

    @Override
    public SymbolTable getSymbolTable() {
        return storageFacade.getSymbolTable(this.valueIndex);
    }

    @Override
    public void reset() {
        this.prevRowId = this.currentRowId = -1;
    }

    @Override
    public void scroll(Record record) {
        this.prevRowId = this.currentRowId;
        this.currentRowId = record.getRowId();
    }

    @Override
    public void setParent(RecordCursor cursor) {
        parent = cursor;
    }

    @Override
    public void setStorageFacade(StorageFacade storageFacade) {
        this.storageFacade = storageFacade;
    }

    private Record getParentRecord() {
        return parent.getByRowId(prevRowId);
    }
}
