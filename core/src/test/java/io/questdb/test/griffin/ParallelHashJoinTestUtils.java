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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.table.AsyncHashJoinAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinRecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

/**
 * Differential checks of the parallel hash join against the ordinary plan, the plan that the same
 * SQL compiles to with the operator off. Each side compiles under its own setting of the context's
 * flag, and the two results compare in order, with their types, every SYMBOL column's symbol tables
 * and the B views of the var-size columns, since order parity is part of the operator's contract.
 */
public final class ParallelHashJoinTestUtils {
    // The operator's EXPLAIN node, "Async Hash Join" or, with a compiled probe filter, "Async JIT
    // Hash Join", both followed by their worker count, which the fused GROUP BY's node is not.
    private static final String OPERATOR = "Hash Join workers:";

    private ParallelHashJoinTestUtils() {
    }

    /**
     * Compiles the query with the operator on, asserts that its plan runs the operator, and compares
     * two executions of that factory, in order, with the ordinary plan's result. Returns the
     * operator's plan.
     */
    public static String assertParallelHashJoin(String sql, SqlExecutionContext context) throws SqlException {
        final boolean wasEnabled = context.isParallelHashJoinEnabled();
        try {
            final String expected = ordinary(sql, context);
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
                final String plan = plan(factory, context);
                Assert.assertTrue(sql + '\n' + plan, plan.contains(OPERATOR));
                final String actual = result(factory, context);
                Assert.assertEquals(sql, expected, actual);
                // A second execution of the same factory reads the same rows.
                Assert.assertEquals(sql, expected, result(factory, context));
                return plan;
            }
        } finally {
            context.setParallelHashJoinEnabled(wasEnabled);
        }
    }

    /**
     * Asserts that the query keeps the ordinary plan with the operator on: its plan names no
     * operator and equals the plan with the operator off, and so does its result.
     */
    public static void assertOrdinaryPlan(String sql, SqlExecutionContext context) throws SqlException {
        final boolean wasEnabled = context.isParallelHashJoinEnabled();
        try {
            context.setParallelHashJoinEnabled(false);
            final String expectedPlan;
            final String expected;
            try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
                expectedPlan = plan(factory, context);
                expected = result(factory, context);
            }
            context.setParallelHashJoinEnabled(true);
            try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
                final String plan = plan(factory, context);
                Assert.assertFalse(sql + '\n' + plan, plan.contains(OPERATOR));
                Assert.assertEquals(sql, expectedPlan, plan);
                Assert.assertEquals(sql, expected, result(factory, context));
            }
        } finally {
            context.setParallelHashJoinEnabled(wasEnabled);
        }
    }

    /**
     * Asserts that the probe tasks and continuation chunks of the join's last execution, since its
     * cursor opened, ran the stages' hidden stepper copies alone, {@code isCopy}, or the shared
     * template alone, and that some ran. A factory runs its copies from the first task once
     * {@link AsyncHashJoinRecordCursorFactory#setStepperWarmUpRowsForTesting(long)} set no warm-up
     * before it compiled, and the template while copies are off.
     */
    public static void assertStepperRuns(AsyncHashJoinRecordCursorFactory join, boolean isCopy, String label) {
        Assert.assertEquals(label, isCopy, join.getStepperClass(0).isHidden());
        final AsyncHashJoinAtom atom = join.getAtom();
        long runs = 0;
        for (int kind : new int[]{AsyncHashJoinAtom.STEPPER_RUN_TASK, AsyncHashJoinAtom.STEPPER_RUN_CONTINUATION}) {
            runs += atom.getStepperRunCount(kind, isCopy);
            Assert.assertEquals(label, 0, atom.getStepperRunCount(kind, !isCopy));
        }
        Assert.assertTrue(label, runs > 0);
    }

    /**
     * Executes the query, which runs the operator, reads every row, and asserts which steppers its
     * tasks ran, see {@link #assertStepperRuns(AsyncHashJoinRecordCursorFactory, boolean, String)}.
     */
    public static void assertStepperRuns(String sql, SqlExecutionContext context, boolean isCopy) throws SqlException {
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            final AsyncHashJoinRecordCursorFactory join = find(factory);
            Assert.assertNotNull(sql, join);
            try (RecordCursor cursor = factory.getCursor(context)) {
                //noinspection StatementWithEmptyBody
                while (cursor.hasNext()) {
                }
                assertStepperRuns(join, isCopy, sql);
            }
        }
    }

    /**
     * The operator inside a compiled query, or null when the query does not run it.
     */
    @Nullable
    public static AsyncHashJoinRecordCursorFactory find(RecordCursorFactory factory) {
        for (RecordCursorFactory current = factory; current != null; current = current.getBaseFactory()) {
            if (current instanceof AsyncHashJoinRecordCursorFactory join) {
                return join;
            }
        }
        return null;
    }

    /**
     * The result of the query's ordinary plan, which runs no operator.
     */
    public static String ordinary(String sql, SqlExecutionContext context) throws SqlException {
        final boolean wasEnabled = context.isParallelHashJoinEnabled();
        context.setParallelHashJoinEnabled(false);
        try (RecordCursorFactory factory = context.getCairoEngine().select(sql, context)) {
            Assert.assertFalse(sql, plan(factory, context).contains(OPERATOR));
            return result(factory, context);
        } finally {
            context.setParallelHashJoinEnabled(wasEnabled);
        }
    }

    public static String plan(RecordCursorFactory factory, SqlExecutionContext context) {
        final TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        return sink.getSink().toString();
    }

    /**
     * Restores the stepper copies and their warm-up to the defaults, see {@link #useSteppers}.
     */
    public static void restoreSteppers() {
        AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(true);
        AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(AsyncHashJoinAtom.STEPPER_WARM_UP_ROWS);
    }

    /**
     * The factory's metadata and rows: every value with its type, then, for every SYMBOL column,
     * the value that the cursor's shared and fresh symbol tables give the row's key, and the B view
     * of every STRING, VARCHAR and SYMBOL column, read after the A view.
     */
    public static String result(RecordCursorFactory factory, SqlExecutionContext context) throws SqlException {
        final StringSink sink = new StringSink();
        final RecordMetadata metadata = factory.getMetadata();
        sink.put("timestamp: ").put(metadata.getTimestampIndex()).put('\n');
        CursorPrinter.println(metadata, sink);
        final ObjList<SymbolTable> freshTables = new ObjList<>();
        try (RecordCursor cursor = factory.getCursor(context)) {
            final int columnCount = metadata.getColumnCount();
            final ObjList<SymbolTable> sharedTables = new ObjList<>();
            for (int c = 0; c < columnCount; c++) {
                if (ColumnType.isSymbol(metadata.getColumnType(c))) {
                    sharedTables.extendAndSet(c, cursor.getSymbolTable(c));
                    freshTables.extendAndSet(c, cursor.newSymbolTable(c));
                }
            }
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                CursorPrinter.println(record, metadata, sink, true);
                for (int c = 0; c < columnCount; c++) {
                    final int type = metadata.getColumnType(c);
                    if (ColumnType.isSymbol(type)) {
                        final CharSequence value = record.getSymA(c);
                        sink.put(" sym ").put(c).put(':');
                        if (value != null) {
                            final int key = record.getInt(c);
                            sink.put(sharedTables.getQuick(c).valueOf(key)).put('/').put(freshTables.getQuick(c).valueOf(key));
                        }
                        sink.put('/').put(record.getSymB(c));
                    } else if (ColumnType.isString(type)) {
                        sink.put(" strB ").put(c).put(':').put(record.getStrB(c)).put('/').put(record.getStrA(c));
                    } else if (ColumnType.isVarchar(type)) {
                        sink.put(" varcharB ").put(c).put(':').put(record.getVarcharB(c)).put('/').put(record.getVarcharA(c));
                    }
                }
                sink.put('\n');
            }
        } finally {
            Misc.freeObjListIfCloseable(freshTables);
        }
        return sink.toString();
    }

    /**
     * Makes the factories that compile from here on run the stages' hidden stepper copies from their
     * first task, {@code isCopy}, or the shared template, as a JVM without hidden classes runs it;
     * {@link #restoreSteppers()} undoes it.
     */
    public static void useSteppers(boolean isCopy) {
        AsyncHashJoinRecordCursorFactory.setStepperCopiesEnabledForTesting(isCopy);
        AsyncHashJoinRecordCursorFactory.setStepperWarmUpRowsForTesting(isCopy ? 0 : AsyncHashJoinAtom.STEPPER_WARM_UP_ROWS);
    }
}
