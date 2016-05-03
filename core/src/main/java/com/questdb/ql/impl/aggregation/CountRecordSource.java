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

package com.questdb.ql.impl.aggregation;

import com.questdb.ex.JournalException;
import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.io.sink.CharSink;
import com.questdb.ql.*;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.DirectInputStream;
import com.questdb.store.ColumnType;

import java.io.OutputStream;

public class CountRecordSource extends AbstractCombinedRecordSource {
    private final CountRecord record;
    private final PartitionSource partitionSource;
    private PartitionCursor partitionCursor;
    private boolean done = false;

    public CountRecordSource(String columnName, PartitionSource partitionSource) {
        CollectionRecordMetadata metadata = new CollectionRecordMetadata();
        ColumnMetadata m = new ColumnMetadata();
        m.name = columnName;
        m.type = ColumnType.LONG;
        metadata.add(m);
        this.record = new CountRecord(metadata);
        this.partitionSource = partitionSource;
    }

    @Override
    public Record getByRowId(long rowId) {
        return null;
    }

    @Override
    public StorageFacade getStorageFacade() {
        return null;
    }

    @Override
    public RecordMetadata getMetadata() {
        return record.getMetadata();
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory) throws JournalException {
        partitionCursor = partitionSource.prepareCursor(factory);
        computeCount();
        return this;
    }

    @Override
    public void reset() {
        if (partitionCursor != null) {
            partitionCursor.reset();
        }
        done = false;
    }

    @Override
    public boolean supportsRowIdAccess() {
        return false;
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public boolean hasNext() {
        return !done && (done = true);
    }

    @Override
    public Record next() {
        return record;
    }

    private void computeCount() {
        long count = 0;
        while (partitionCursor.hasNext()) {
            PartitionSlice slice = partitionCursor.next();
            long hi;
            if (slice.calcHi) {
                hi = slice.partition.size();
            } else {
                hi = slice.hi;
            }
            count += (hi - slice.lo);
        }
        record.count = count;
    }

    private static class CountRecord extends AbstractRecord {

        private long count;

        public CountRecord(RecordMetadata metadata) {
            super(metadata);
        }

        @Override
        public byte get(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getBin(int col, OutputStream s) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DirectInputStream getBin(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getBinLen(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean getBool(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDate(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getDouble(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getFloat(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getFlyweightStr(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getFlyweightStrB(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getInt(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int col) {
            return count;
        }

        @Override
        public long getRowId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public short getShort(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence getStr(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void getStr(int col, CharSink sink) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getStrLen(int col) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getSym(int col) {
            throw new UnsupportedOperationException();
        }
    }
}
