/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.idx.BitmapIndexReader;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.StrFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.griffin.engine.table.AsyncFilterAtom;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf16Sink;

import static io.questdb.cairo.sql.PartitionFrameCursorFactory.ORDER_ASC;

public class TouchTableFunctionFactory implements FunctionFactory {

    @Override
    public String getSignature() {
        return "touch(C)";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final Function function = args.get(0);
        final int pos = argPositions.get(0);

        // factory belongs to the function, do not close
        final RecordCursorFactory recordCursorFactory = function.getRecordCursorFactory();
        if (recordCursorFactory == null || !recordCursorFactory.supportsPageFrameCursor()) {
            throw SqlException.$(pos, "query does not support framing execution and cannot be pre-touched");
        }

        return new TouchTableFunc(function);
    }

    private static class TouchTableFunc extends StrFunction implements UnaryFunction {
        private static final Log LOG = LogFactory.getLog(TouchTableFunc.class);

        private final Function arg;
        private final StringSink sinkA = new StringSink();
        private final StringSink sinkB = new StringSink();
        private long dataPages = 0;
        private long indexKeyPages = 0;
        private long indexValuePages = 0;
        private SqlExecutionContext sqlExecutionContext;

        public TouchTableFunc(Function arg) {
            this.arg = arg;
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public String getName() {
            return "touch";
        }

        @Override
        public CharSequence getStrA(Record rec) {
            sinkA.clear();
            getStr(sinkA);
            return sinkA;
        }

        @Override
        public CharSequence getStrB(Record rec) {
            sinkB.clear();
            getStr(sinkB);
            return sinkB;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            arg.init(symbolTableSource, executionContext);
            this.sqlExecutionContext = executionContext;
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        private void clearCounters() {
            dataPages = 0;
            indexKeyPages = 0;
            indexValuePages = 0;
        }

        private void getStr(Utf16Sink utf16Sink) {
            touchTable();
            utf16Sink.put("{\"data_pages\": ")
                    .put(dataPages)
                    .put(", \"index_key_pages\":")
                    .put(indexKeyPages)
                    .put(", \"index_values_pages\": ")
                    .put(indexValuePages).put("}");
        }

        private long touchMemory(long pageSize, long baseAddress, long memorySize) {
            if (baseAddress == 0) {
                return 0;
            }
            final long pageCount = (memorySize + pageSize - 1) / pageSize;

            for (long i = 0; i < pageCount; i++) {
                final byte v = Unsafe.getUnsafe().getByte(baseAddress + i * pageSize);
                // Use the same blackhole as in async offload's column pre-touch.
                AsyncFilterAtom.PRE_TOUCH_BLACK_HOLE.add(v);
            }

            return pageCount;
        }

        private void touchTable() {
            clearCounters();
            final long pageSize = Files.PAGE_SIZE;
            // factory belongs to the function, do not close
            final RecordCursorFactory recordCursorFactory = arg.getRecordCursorFactory();
            try (PageFrameCursor pageFrameCursor = recordCursorFactory.getPageFrameCursor(sqlExecutionContext, ORDER_ASC)) {
                PageFrame frame;
                RecordMetadata metadata = recordCursorFactory.getMetadata();
                while ((frame = pageFrameCursor.next()) != null) {
                    for (int columnIndex = 0, sz = metadata.getColumnCount(); columnIndex < sz; columnIndex++) {

                        final long columnMemorySize = frame.getPageSize(columnIndex);
                        final long columnBaseAddress = frame.getPageAddress(columnIndex);
                        dataPages += touchMemory(pageSize, columnBaseAddress, columnMemorySize);

                        if (metadata.isColumnIndexed(columnIndex)) {
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
            } catch (SqlException e) {
                // do not propagate
                LOG.error().$("cannot acquire page frame cursor: ").$((Sinkable) e).$();
            }
        }
    }
}
