/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.griffin.engine.functions.rnd;

import com.questdb.cairo.CairoConfiguration;
import com.questdb.cairo.GenericRecordMetadata;
import com.questdb.cairo.TableColumnMetadata;
import com.questdb.cairo.sql.*;
import com.questdb.common.ColumnType;
import com.questdb.griffin.Function;
import com.questdb.griffin.FunctionFactory;
import com.questdb.griffin.SqlException;
import com.questdb.griffin.engine.functions.CursorFunction;
import com.questdb.std.Misc;
import com.questdb.std.ObjList;
import com.questdb.std.str.StringSink;

public class RandomCursorFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "random(lV)";
    }

    @Override
    public Function newInstance(ObjList<Function> args, int position, CairoConfiguration configuration) throws SqlException {
        long count = args.getQuick(0).getLong(null);
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        final StringSink b = Misc.getThreadLocalBuilder();
        for (int i = 1, n = args.size(); i < n; i++) {
            Function arg = args.getQuick(i);
            if (arg.getType() != ColumnType.INT) {
                throw SqlException.$(arg.getPosition(), "'TYPE' argument expected");
            }

            if (!arg.isConstant()) {
                throw SqlException.$(arg.getPosition(), "constant argument expected");
            }
            b.put("x").put(i);
            metadata.add(new TableColumnMetadata(b.toString(), arg.getInt(null)));
            b.clear();
        }

        MetadataContainer metadataContainer = new MetadataContainer() {
            @Override
            public void close() {
            }

            @Override
            public RecordMetadata getMetadata() {
                return metadata;
            }
        };

        RandomRecord record = new RandomRecord();

        RecordCursor recordCursor = new RecordCursor() {
            private final long recordCount = count;
            private long recordIndex = 0;

            @Override
            public Record getRecord() {
                return record;
            }

            @Override
            public Record newRecord() {
                return record;
            }

            @Override
            public Record recordAt(long rowId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void recordAt(Record record, long atRowId) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void toTop() {
                recordIndex = 0;
            }

            @Override
            public boolean hasNext() {
                return recordIndex < recordCount;
            }

            @Override
            public Record next() {
                recordIndex++;
                return record;
            }

            @Override
            public void close() {
            }


            @Override
            public RecordMetadata getMetadata() {
                return metadata;
            }


        };

        RecordCursorFactory recordCursorFactory = new RecordCursorFactory() {
            @Override
            public RecordCursor getCursor() {
                return recordCursor;
            }

            @Override
            public MetadataContainer getMetadataContainer() {
                return metadataContainer;
            }
        };


        return new CursorFunction(position) {
            @Override
            public RecordCursorFactory getRecordCursorFactory(Record record) {
                return recordCursorFactory;
            }
        };
    }
}
