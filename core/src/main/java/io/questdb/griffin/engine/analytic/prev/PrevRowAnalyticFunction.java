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

package io.questdb.griffin.engine.analytic.prev;


public class PrevRowAnalyticFunction implements AnalyticFunction {
    private final VirtualColumn valueColumn;
    private RecordCursor parent;
    private long prevRowId = -1;
    private long currentRowId = -1;
    private Record record;

    public PrevRowAnalyticFunction(VirtualColumn valueColumn) {
        this.valueColumn = valueColumn;
    }

    @Override
    public void add(Record record) {
    }

    @Override
    public byte get() {
        return prevRowId == -1 ? 0 : valueColumn.get(getParentRecord());
    }

    @Override
    public boolean getBool() {
        return prevRowId != -1 && valueColumn.getBool(getParentRecord());
    }

    @Override
    public long getDate() {
        return prevRowId == -1 ? Numbers.LONG_NaN : valueColumn.getDate(getParentRecord());
    }

    @Override
    public double getDouble() {
        return prevRowId == -1 ? Double.NaN : valueColumn.getDouble(getParentRecord());
    }

    @Override
    public float getFloat() {
        return prevRowId == -1 ? Float.NaN : valueColumn.getFloat(getParentRecord());
    }

    public CharSequence getStr() {
        return prevRowId == -1 ? null : valueColumn.getFlyweightStr(getParentRecord());
    }

    public CharSequence getStrB() {
        return prevRowId == -1 ? null : valueColumn.getFlyweightStrB(getParentRecord());
    }

    @Override
    public int getInt() {
        if (prevRowId == -1) {
            if (valueColumn.getType() == ColumnType.SYMBOL) {
                return SymbolTable.VALUE_IS_NULL;
            }
            return Numbers.INT_NaN;
        }
        return valueColumn.getInt(getParentRecord());
    }

    @Override
    public long getLong() {
        return prevRowId == -1 ? Numbers.LONG_NaN : valueColumn.getLong(getParentRecord());
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return valueColumn;
    }

    @Override
    public short getShort() {
        return prevRowId == -1 ? 0 : valueColumn.getShort(getParentRecord());
    }

    @Override
    public void getStr(CharSink sink) {
        if (prevRowId > -1) {
            valueColumn.getStr(getParentRecord(), sink);
        }
    }

    @Override
    public int getStrLen() {
        return prevRowId == -1 ? -1 : valueColumn.getStrLen(getParentRecord());
    }

    @Override
    public String getSym() {
        return prevRowId == -1 ? null : valueColumn.getSym(getParentRecord());
    }

    @Override
    public SymbolTable getSymbolTable() {
        return valueColumn.getSymbolTable();
    }

    @Override
    public int getType() {
        return AnalyticFunction.STREAM;
    }

    @Override
    public void prepare(RecordCursor cursor) {
        parent = cursor;
        this.record = cursor.newRecord();
        valueColumn.prepare(cursor.getStorageFacade());
    }

    @Override
    public void prepareFor(Record record) {
        this.prevRowId = this.currentRowId;
        this.currentRowId = record.getRowId();
    }

    @Override
    public void reset() {
        this.prevRowId = this.currentRowId = -1;
    }

    @Override
    public void toTop() {
        reset();
    }

    private Record getParentRecord() {
        parent.recordAt(record, prevRowId);
        return record;
    }
}
