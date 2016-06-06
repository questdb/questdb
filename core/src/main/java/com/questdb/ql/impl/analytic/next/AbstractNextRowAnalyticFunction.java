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

package com.questdb.ql.impl.analytic.next;

import com.questdb.factory.configuration.RecordColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.misc.Unsafe;
import com.questdb.ql.Record;
import com.questdb.ql.StorageFacade;
import com.questdb.ql.impl.NullRecord;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.impl.RecordList;
import com.questdb.ql.impl.RecordListRecord;
import com.questdb.ql.impl.analytic.AnalyticFunction;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;
import com.questdb.store.MemoryPages;

import java.io.Closeable;
import java.io.OutputStream;

public abstract class AbstractNextRowAnalyticFunction implements AnalyticFunction, Closeable {

    protected final MemoryPages pages;
    private final RecordColumnMetadata metadata;
    private final int columnIndex;
    private long offset;
    private RecordListRecord record;
    private Record next;
    private StorageFacade storageFacade;

    public AbstractNextRowAnalyticFunction(int pageSize, RecordMetadata parentMetadata, String columnName, String alias) {
        this.pages = new MemoryPages(pageSize);
        this.columnIndex = parentMetadata.getColumnIndex(columnName);
        this.metadata = new RecordColumnMetadataImpl(alias == null ? columnName : alias, parentMetadata.getColumnQuick(this.columnIndex).getType());
    }

    @Override
    public void close() {
        pages.close();
    }

    @Override
    public byte get() {
        return next.get(columnIndex);
    }

    @Override
    public void getBin(OutputStream s) {
        next.getBin(columnIndex, s);
    }

    @Override
    public DirectInputStream getBin() {
        return next.getBin(columnIndex);
    }

    @Override
    public long getBinLen() {
        return next.getBinLen(columnIndex);
    }

    @Override
    public boolean getBool() {
        return next.getBool(columnIndex);
    }

    @Override
    public long getDate() {
        return next.getDate(columnIndex);
    }

    @Override
    public double getDouble() {
        return next.getDouble(columnIndex);
    }

    @Override
    public float getFloat() {
        return next.getFloat(columnIndex);
    }

    @Override
    public CharSequence getFlyweightStr() {
        return next.getFlyweightStr(columnIndex);
    }

    @Override
    public CharSequence getFlyweightStrB() {
        return next.getFlyweightStrB(columnIndex);
    }

    @Override
    public int getInt() {
        return next.getInt(columnIndex);
    }

    @Override
    public long getLong() {
        return next.getLong(columnIndex);
    }

    @Override
    public RecordColumnMetadata getMetadata() {
        return metadata;
    }

    @Override
    public short getShort() {
        return next.getShort(columnIndex);
    }

    @Override
    public void getStr(CharSink sink) {
        next.getStr(columnIndex, sink);
    }

    @Override
    public CharSequence getStr() {
        return next.getStr(columnIndex);
    }

    @Override
    public int getStrLen() {
        return next.getStrLen(columnIndex);
    }

    @Override
    public String getSym() {
        return next.getSym(columnIndex);
    }

    @Override
    public void prepare(RecordList base) {
        this.record = base.newRecord();
        this.record.setStorageFacade(storageFacade);
        this.offset = 0;
    }

    @Override
    public void reset() {
        pages.clear();
    }

    @Override
    public void scroll(Record rec) {
        long rowid = Unsafe.getUnsafe().getLong(pages.addressOf(this.offset));
        if (rowid == -1) {
            next = NullRecord.INSTANCE;
        } else {
            record.of(rowid);
            next = record;
        }
        this.offset += 8;
    }

    @Override
    public void setStorageFacade(StorageFacade storageFacade) {
        this.storageFacade = storageFacade;
    }
}
