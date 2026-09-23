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

package io.questdb.test.tools;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Registers a synthetic FROM/JOIN table function whose cursor factory counts its own closes.
 * <p>
 * This is the fixture for the cursor-factory OWNERSHIP tests: the optimiser instantiates a table
 * function while parsing the FROM clause and holds it in flight until code generation takes it over
 * ({@code SqlCodeGenerator#generateFunctionQuery}), so every compile path that throws in between has to
 * close it exactly once -- a miss leaks, a double close is a use-after-free. The counter deliberately
 * counts every {@code close()} call rather than every effective release, because the guard in
 * {@code AbstractRecordCursorFactory} swallows repeated closes and would hide the double close these
 * tests exist to catch.
 */
public final class TableFunctionTestUtils {

    private TableFunctionTestUtils() {
    }

    public static void register(
            CairoEngine engine,
            String functionName,
            int executionRequirements,
            @Nullable AtomicInteger constructionCount,
            @Nullable ObjList<CloseCountingRecordCursorFactory> instantiatedFactories
    ) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
        descriptors.add(new FunctionFactoryDescriptor(new FunctionFactory() {
            @Override
            public int getExecutionRequirements() {
                return executionRequirements;
            }

            @Override
            public String getSignature() {
                return functionName + "()";
            }

            @Override
            public boolean isCursor() {
                return true;
            }

            @Override
            public Function newInstance(
                    int position,
                    ObjList<Function> args,
                    IntList argPositions,
                    CairoConfiguration configuration,
                    SqlExecutionContext executionContext
            ) {
                if (constructionCount != null) {
                    constructionCount.incrementAndGet();
                }
                final GenericRecordMetadata metadata = new GenericRecordMetadata();
                metadata.add(new TableColumnMetadata("permission", ColumnType.VARCHAR));
                final CloseCountingRecordCursorFactory factory =
                        new CloseCountingRecordCursorFactory(new EmptyTableRecordCursorFactory(metadata));
                if (instantiatedFactories != null) {
                    instantiatedFactories.add(factory);
                }
                return new CursorFunction(factory);
            }
        }));
        Assert.assertNull(engine.getFunctionFactoryCache().getFactories().get(functionName));
        engine.getFunctionFactoryCache().getFactories().put(functionName, descriptors);
    }

    public static void unregister(CairoEngine engine, String functionName) {
        engine.getFunctionFactoryCache().getFactories().remove(functionName);
    }

    public static class CloseCountingRecordCursorFactory implements RecordCursorFactory {
        private final RecordCursorFactory delegate;
        private int closeCount;

        private CloseCountingRecordCursorFactory(RecordCursorFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public void close() {
            closeCount++;
            delegate.close();
        }

        public int getCloseCount() {
            return closeCount;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
            return delegate.getCursor(executionContext);
        }

        @Override
        public RecordMetadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return delegate.recordCursorSupportsRandomAccess();
        }

        @Override
        public void toPlan(PlanSink sink) {
            delegate.toPlan(sink);
        }
    }
}
