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

package com.questdb.ql.impl.aggregation;

import com.questdb.factory.JournalReaderFactory;
import com.questdb.factory.configuration.RecordMetadata;
import com.questdb.ql.*;
import com.questdb.ql.impl.CollectionRecordMetadata;
import com.questdb.ql.impl.RecordColumnMetadataImpl;
import com.questdb.ql.ops.AbstractCombinedRecordSource;
import com.questdb.std.CharSink;
import com.questdb.std.DirectInputStream;
import com.questdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.OutputStream;

public class CountRecordSource extends AbstractCombinedRecordSource {
    private final CountRecord record;
    private final PartitionSource partitionSource;
    private final CollectionRecordMetadata metadata = new CollectionRecordMetadata();
    private PartitionCursor partitionCursor;
    private boolean done = false;

    public CountRecordSource(String columnName, PartitionSource partitionSource) {
        metadata.add(new RecordColumnMetadataImpl(columnName, ColumnType.LONG));
        this.record = new CountRecord();
        this.partitionSource = partitionSource;
    }

    @Override
    public void close() {
    }

    @Override
    public RecordMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RecordCursor prepareCursor(JournalReaderFactory factory, CancellationHandler cancellationHandler) {
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
    public StorageFacade getStorageFacade() {
        return null;
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public boolean hasNext() {
        return !done && (done = true);
    }

    @SuppressFBWarnings("IT_NO_SUCH_ELEMENT")
    @Override
    public Record next() {
        return record;
    }

    @Override
    public void toSink(CharSink sink) {
        sink.put('{');
        sink.putQuoted("op").put(':').putQuoted("CountRecordSource").put(',');
        sink.putQuoted("psrc").put(':').put(partitionSource);
        sink.put('}');
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

    private static class CountRecord implements Record {

        private long count;

        @Override
        public String asString(int col) {
            throw new UnsupportedOperationException();
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
