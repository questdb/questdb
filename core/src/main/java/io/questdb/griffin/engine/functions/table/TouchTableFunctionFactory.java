/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.table;

import io.questdb.cairo.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;

public class TouchTableFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "touch(C)";
    }

    @Override
    public Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function function = args.get(0);
        final int pos = argPositions.get(0);
        final RecordCursorFactory recordCursorFactory = function.getRecordCursorFactory();
        if (recordCursorFactory == null || !recordCursorFactory.supportPageFrameCursor()) {
            throw SqlException.$(pos, "query is not support page frame cursor");
        }

        final RecordMetadata metadata = recordCursorFactory.getMetadata();
        return new TouchTableFunc(function, metadata, recordCursorFactory, sqlExecutionContext);
    }

    private static class TouchTableFunc extends StrFunction implements UnaryFunction {
        private final Function arg;
        private final RecordMetadata metadata;
        private final RecordCursorFactory recordCursorFactory;
        private final SqlExecutionContext sqlExecutionContext;

        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();

        private long garbage = 0;
        private long dataPages = 0;
        private long indexKeyPages = 0;
        private long indexValuePages = 0;

        public TouchTableFunc(Function arg,
                              RecordMetadata metadata,
                              RecordCursorFactory recordCursorFactory,
                              SqlExecutionContext sqlExecutionContext) {
            this.arg = arg;
            this.metadata = metadata;
            this.recordCursorFactory = recordCursorFactory;
            this.sqlExecutionContext = sqlExecutionContext;
        }

        @Override
        public Function getArg() {
            return this.arg;
        }

        @Override
        public CharSequence getStr(Record rec) {
            sinkA.clear();
            getStr(rec, sinkA);
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            sinkB.clear();
            getStr(rec, sinkB);
            return sinkB;
        }

        @Override
        public void getStr(Record rec, CharSink sink) {
            touchTable();

            sink.put("touched dataPages[")
                    .put(dataPages)
                    .put("],")
                    .put(" indexKeyPages[")
                    .put(indexKeyPages)
                    .put("],")
                    .put(" indexValuePages[")
                    .put(indexValuePages)
                    .put("]");
        }

        private void clearCounters() {
            garbage = 0;
            dataPages = 0;
            indexKeyPages = 0;
            indexValuePages = 0;
        }

        private void touchTable() {
            clearCounters();
            final int pageSize = Unsafe.getUnsafe().pageSize();

            try(PageFrameCursor pageFrameCursor = recordCursorFactory.getPageFrameCursor(sqlExecutionContext)) {
                PageFrame frame;
                while ((frame = pageFrameCursor.next()) != null) {
                    for (int columnIndex = 0, sz = metadata.getColumnCount(); columnIndex < sz; columnIndex++) {

                        final long columnMemorySize = frame.getPageSize(columnIndex);
                        final long columnBaseAddress = frame.getPageAddress(columnIndex);
                        dataPages += touchMemory(pageSize, columnBaseAddress, columnMemorySize);

                        if (metadata.isColumnIndexed(columnIndex)) {
                            //TODO: backward/forward ???
                            final BitmapIndexReader indexReader = frame.getBitmapIndexReader(columnIndex, BitmapIndexReader.DIR_BACKWARD);

                            final long keyBaseAddress = indexReader.getKeyBaseAddress();
                            final long keyMemorySize = indexReader.getKeyMemorySize();
                            indexKeyPages += touchMemory(pageSize, keyBaseAddress, keyMemorySize);

                            final long valueBaseAddress = indexReader.getValueBaseAddress();
                            final long valueMemorySize = indexReader.getValueMemorySize();
                            indexValuePages += touchMemory(pageSize, valueBaseAddress, valueMemorySize);
                        }
                    }
                }
            } catch (SqlException ignored) {
                // do not propagate
            }
        }

        private long touchMemory(int pageSize, long baseAddress, long memorySize) {
            final long pageCount = (memorySize + pageSize - 1) / pageSize;

            for (long i = 0; i < pageCount; i++) {
                final byte v = Unsafe.getUnsafe().getByte(baseAddress + i * pageSize);
                garbage += v;
            }

            return pageCount;
        }
    }
}