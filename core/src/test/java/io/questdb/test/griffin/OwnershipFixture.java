/*+*****************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.UnaryFunction;
import io.questdb.std.Chars;
import io.questdb.std.FlyweightMessageContainer;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.Assert;

/**
 * Registers SQL functions whose instances count their own closes, so ownership tests can drive the
 * code generator through plain SQL and still observe every input it adopts:
 * <ul>
 *     <li>{@code owned_table(i)} - a table function shaped by the {@link #table(Object...)} spec at index {@code i};</li>
 *     <li>{@code owned_long(v)} - a runtime-constant LONG function returning {@code v}, which may be a bind variable.</li>
 * </ul>
 * The counters count every {@code close()} call, so a double close shows up even where the real
 * factories would swallow it. Most factory constructors allocate native memory lazily, so their
 * failure paths are reached through the table function's metadata instead:
 * {@link #assertMetadataFaultSweep} fails each {@code getColumnType()} call a compilation makes, one
 * at a time; {@link #assertCompileOomSweep} covers the constructors that do allocate.
 */
final class OwnershipFixture implements AutoCloseable {
    private static final String LONG_FUNCTION = "owned_long";
    private static final int OOM_SWEEP_SLACK_MAX = 16 * 1024 * 1024;
    private static final int OOM_SWEEP_STEP = 64;
    private static final String TABLE_FUNCTION = "owned_table";
    private final CairoEngine engine;
    private final ObjList<TrackingLong> longs = new ObjList<>();
    private final ObjList<TableSpec> specs = new ObjList<>();
    private final ObjList<TrackingFactory> tables = new ObjList<>();
    private int columnTypeCalls;
    private int failingColumnTypeCall = -1;
    private RuntimeException longCloseFailure;
    private RuntimeException metadataFailure;

    OwnershipFixture(CairoEngine engine) {
        this.engine = engine;
        register(new FunctionFactory() {
            @Override
            public String getSignature() {
                return TABLE_FUNCTION + "(i)";
            }

            @Override
            public boolean isCursor() {
                return true;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                        CairoConfiguration configuration, SqlExecutionContext context) {
                final TrackingFactory factory = new TrackingFactory(OwnershipFixture.this, specs.getQuick(args.getQuick(0).getInt(null)));
                tables.add(factory);
                return new CursorFunction(factory);
            }
        }, TABLE_FUNCTION);
        register(new FunctionFactory() {
            @Override
            public String getSignature() {
                return LONG_FUNCTION + "(L)";
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                        CairoConfiguration configuration, SqlExecutionContext context) {
                final TrackingLong function = new TrackingLong(args.getQuick(0));
                function.closeFailure = longCloseFailure;
                longs.add(function);
                return function;
            }
        }, LONG_FUNCTION);
    }

    static boolean hasSuppressed(Throwable thrown, Throwable expected) {
        for (Throwable suppressed : thrown.getSuppressed()) {
            if (suppressed == expected || hasSuppressed(suppressed, expected)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compiles {@code sql} once to count the {@code getColumnType()} calls it makes on
     * {@code owned_table} metadata, then compiles it again failing each of those calls in turn. Every
     * injected failure must reach the caller unmasked, carry any armed close failure as a suppressed
     * exception, and leave every instantiated owner closed exactly once.
     */
    void assertMetadataFaultSweep(SqlCompiler compiler, SqlExecutionContext context, String sql) throws SqlException {
        reset();
        compileAndClose(compiler, context, sql);
        assertAllClosedOnce();
        final int callCount = columnTypeCalls;
        Assert.assertTrue("the compilation never read owned_table metadata", callCount > 0);
        try {
            for (int call = 1; call <= callCount; call++) {
                reset();
                failingColumnTypeCall = call;
                metadataFailure = new RuntimeException("metadata fault " + call);
                final Throwable thrown = Assert.assertThrows(Throwable.class, () -> compiler.compile(sql, context));
                if (thrown != metadataFailure) {
                    throw new AssertionError("injected failure was masked at call " + call, thrown);
                }
                assertCloseFailuresSuppressed(thrown);
                assertAllClosedOnce();
            }
        } finally {
            failingColumnTypeCall = -1;
            metadataFailure = null;
        }
        reset();
    }

    /**
     * Compiles {@code sql} under an RSS ceiling that walks up from zero until the compilation
     * succeeds, so each native allocation the compilation makes fails in turn. Most factories
     * allocate lazily, on cursor open; this reaches the ones that allocate in their constructors.
     * At every point the out-of-memory error must reach the caller unmasked, carry any armed close
     * failure as a suppressed exception, and leave every instantiated owner closed exactly once.
     */
    void assertCompileOomSweep(SqlCompiler compiler, SqlExecutionContext context, String sql) throws SqlException {
        reset();
        compileAndClose(compiler, context, sql);
        int maxOomSlack = -1;
        boolean hasCompiledUnderLimit = false;
        for (int slack = 0; !hasCompiledUnderLimit; slack += OOM_SWEEP_STEP) {
            Assert.assertTrue("the sweep never compiled under an armed ceiling", slack <= OOM_SWEEP_SLACK_MAX);
            reset();
            RecordCursorFactory factory = null;
            Unsafe.setRssMemLimit(Unsafe.getRssMemUsed() + slack);
            try {
                factory = compiler.compile(sql, context).getRecordCursorFactory();
                hasCompiledUnderLimit = true;
            } catch (CairoException e) {
                if (!e.isOutOfMemory()) {
                    throw new AssertionError("expected an out-of-memory error", e);
                }
                maxOomSlack = slack;
                assertCloseFailuresSuppressed(e);
            } finally {
                Unsafe.setRssMemLimit(0);
            }
            if (factory != null) {
                closeExpectingArmedFailure(factory);
            }
            assertClosedOnce();
        }
        Assert.assertTrue("compilation only failed at the zero-slack endpoint", maxOomSlack > 0);
        reset();
    }

    /**
     * Compiles {@code sql}, expecting it to fail with {@code message}. The failure must reach the caller
     * unmasked by any armed close failure, which it must carry as a suppressed exception instead, and
     * every instantiated owner must be closed exactly once.
     */
    Throwable assertCompileFails(SqlCompiler compiler, SqlExecutionContext context, String sql, String message) {
        reset();
        final Throwable thrown = Assert.assertThrows(Throwable.class, () -> compiler.compile(sql, context));
        if (!(thrown instanceof FlyweightMessageContainer container)
                || !Chars.contains(container.getFlyweightMessage(), message)) {
            throw new AssertionError("expected failure containing [" + message + ']', thrown);
        }
        assertCloseFailuresSuppressed(thrown);
        assertAllClosedOnce();
        return thrown;
    }

    void assertAllClosedOnce() {
        Assert.assertTrue("no owned function was instantiated", tables.size() + longs.size() > 0);
        assertClosedOnce();
    }

    void assertNoneClosed() {
        Assert.assertTrue("no owned function was instantiated", tables.size() + longs.size() > 0);
        for (int i = 0, n = tables.size(); i < n; i++) {
            Assert.assertEquals("owned_table closes", 0, tables.getQuick(i).closeCount);
        }
        for (int i = 0, n = longs.size(); i < n; i++) {
            Assert.assertEquals("owned_long closes", 0, longs.getQuick(i).closeCount);
        }
    }

    @Override
    public void close() {
        engine.getFunctionFactoryCache().getFactories().remove(TABLE_FUNCTION);
        engine.getFunctionFactoryCache().getFactories().remove(LONG_FUNCTION);
    }

    void failLongClose(RuntimeException failure) {
        longCloseFailure = failure;
    }

    int longCount() {
        return longs.size();
    }

    void reset() {
        tables.clear();
        longs.clear();
        columnTypeCalls = 0;
    }

    int tableCount() {
        return tables.size();
    }

    /**
     * Declares the shape of {@code owned_table(i)} where {@code i} is the returned spec's index;
     * columns follow as name/type pairs.
     */
    TableSpec table(Object... columns) {
        final TableSpec spec = new TableSpec();
        for (int i = 0; i < columns.length; i += 2) {
            spec.metadata.add(new TableColumnMetadata((String) columns[i], ((Number) columns[i + 1]).intValue()));
        }
        specs.add(spec);
        return spec;
    }

    private void assertCloseFailuresSuppressed(Throwable thrown) {
        for (int i = 0, n = tables.size(); i < n; i++) {
            final TrackingFactory table = tables.getQuick(i);
            if (table.closeFailure != null && table.closeCount > 0) {
                Assert.assertTrue("close failure was not suppressed", hasSuppressed(thrown, table.closeFailure));
            }
        }
        for (int i = 0, n = longs.size(); i < n; i++) {
            final TrackingLong function = longs.getQuick(i);
            if (function.closeFailure != null && function.closeCount > 0) {
                Assert.assertTrue("close failure was not suppressed", hasSuppressed(thrown, function.closeFailure));
            }
        }
    }

    private void assertClosedOnce() {
        for (int i = 0, n = tables.size(); i < n; i++) {
            Assert.assertEquals("owned_table closes", 1, tables.getQuick(i).closeCount);
        }
        for (int i = 0, n = longs.size(); i < n; i++) {
            Assert.assertEquals("owned_long closes", 1, longs.getQuick(i).closeCount);
        }
    }

    private void closeExpectingArmedFailure(RecordCursorFactory factory) {
        try {
            factory.close();
        } catch (RuntimeException e) {
            boolean isArmed = false;
            for (int i = 0, n = tables.size(); i < n; i++) {
                isArmed |= tables.getQuick(i).closeFailure == e;
            }
            for (int i = 0, n = longs.size(); i < n; i++) {
                isArmed |= longs.getQuick(i).closeFailure == e;
            }
            if (!isArmed) {
                throw e;
            }
        }
    }

    private void compileAndClose(SqlCompiler compiler, SqlExecutionContext context, String sql) throws SqlException {
        closeExpectingArmedFailure(compiler.compile(sql, context).getRecordCursorFactory());
    }

    private void register(FunctionFactory factory, String name) {
        final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
        try {
            descriptors.add(new FunctionFactoryDescriptor(factory));
        } catch (SqlException e) {
            throw new AssertionError(e);
        }
        Assert.assertNull(engine.getFunctionFactoryCache().getFactories().get(name));
        engine.getFunctionFactoryCache().getFactories().put(name, descriptors);
    }

    static final class TableSpec {
        private final GenericRecordMetadata metadata = new GenericRecordMetadata();
        RuntimeException closeFailure;
        boolean isRandomAccess;
        int scanDirection = RecordCursorFactory.SCAN_DIRECTION_FORWARD;

        TableSpec timestamp(int index) {
            metadata.setTimestampIndex(index);
            return this;
        }
    }

    static final class TrackingFactory implements RecordCursorFactory {
        private final RuntimeException closeFailure;
        private final EmptyTableRecordCursorFactory delegate;
        private final boolean isRandomAccess;
        private final int scanDirection;
        private int closeCount;

        private TrackingFactory(OwnershipFixture fixture, TableSpec spec) {
            final GenericRecordMetadata metadata = new GenericRecordMetadata() {
                @Override
                public int getColumnType(int columnIndex) {
                    if (++fixture.columnTypeCalls == fixture.failingColumnTypeCall) {
                        throw fixture.metadataFailure;
                    }
                    return super.getColumnType(columnIndex);
                }
            };
            GenericRecordMetadata.copyColumns(spec.metadata, metadata);
            metadata.setTimestampIndex(spec.metadata.getTimestampIndex());
            this.delegate = new EmptyTableRecordCursorFactory(metadata);
            this.closeFailure = spec.closeFailure;
            this.isRandomAccess = spec.isRandomAccess;
            this.scanDirection = spec.scanDirection;
        }

        @Override
        public void close() {
            closeCount++;
            delegate.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
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
        public int getScanDirection() {
            return scanDirection;
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return isRandomAccess;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.type("owned_table");
        }
    }

    static final class TrackingLong extends LongFunction implements UnaryFunction {
        private final Function arg;
        private int closeCount;
        private RuntimeException closeFailure;

        private TrackingLong(Function arg) {
            this.arg = arg;
        }

        @Override
        public void close() {
            closeCount++;
            arg.close();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public Function getArg() {
            return arg;
        }

        @Override
        public long getLong(Record rec) {
            return arg.getLong(rec);
        }

        @Override
        public String getName() {
            return LONG_FUNCTION;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }
    }

}
