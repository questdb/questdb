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
import io.questdb.griffin.PreparedFunctions;
import io.questdb.griffin.engine.functions.columns.BindableColumn;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.CursorExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.Subquery;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

public class PreparedFunctionsTest {
    @Test
    public void testEmptyAndClosedLeavesNeedNoDescription() {
        final PreparedFunctions prepared = new PreparedFunctions(4);
        final PreparedFunctions.Entry entry = prepared.begin();
        Assert.assertTrue(PreparedFunctions.hasOnlyReadLeaves(entry));
        final BindableColumn first = leaf(0);
        entry.addColumn(first);
        Assert.assertFalse(PreparedFunctions.hasOnlyReadLeaves(entry));
        first.close();
        Assert.assertTrue(PreparedFunctions.hasOnlyReadLeaves(entry));
        final BindableColumn second = leaf(1);
        second.close();
        entry.addColumn(second);
        Assert.assertTrue(PreparedFunctions.hasOnlyReadLeaves(entry));
        prepared.clear();
    }

    @Test
    public void testLeafMembershipTracksReplacementAndDuplicates() {
        final PreparedFunctions prepared = new PreparedFunctions(4);
        final PreparedFunctions.Entry entry = prepared.begin();
        entry.addColumn(leaf(0));
        entry.addColumn(leaf(Integer.MAX_VALUE));
        entry.addColumn(leaf(Integer.MAX_VALUE));
        prepared.describe(entry, call(column(0), call(column(Integer.MAX_VALUE), column(0))));
        Assert.assertTrue(PreparedFunctions.hasOnlyReadLeaves(entry));
        prepared.describe(entry, column(0));
        Assert.assertFalse(PreparedFunctions.hasOnlyReadLeaves(entry));
        entry.leaves.getQuick(1).close();
        entry.leaves.getQuick(2).close();
        Assert.assertTrue(PreparedFunctions.hasOnlyReadLeaves(entry));
        prepared.describe(entry, new ConstantExpression().ofInt(7, 0));
        Assert.assertFalse(PreparedFunctions.hasOnlyReadLeaves(entry));
        entry.leaves.getQuick(0).close();
        Assert.assertTrue(PreparedFunctions.hasOnlyReadLeaves(entry));
        prepared.clear();
    }

    @Test
    public void testMissingOpenLeavesFailRelocationAndRetarget() {
        final PreparedFunctions prepared = new PreparedFunctions(4);
        final PreparedFunctions.Entry entry = prepared.begin();
        entry.addColumn(leaf(0));
        entry.addColumn(leaf(1));
        prepared.describe(entry, column(0));
        final ScanPlan scan = new ScanPlan();
        scan.getOutput().add(0, "a", ColumnType.INT, true).add(1, "b", ColumnType.INT, true);
        final ProjectPlan projection = new ProjectPlan().of(scan, 0);
        projection.getOutput().copyFrom(scan.getOutput());
        projection.getExpressions().add(column(0));
        projection.getExpressions().add(column(1));
        if (PreparedFunctions.class.desiredAssertionStatus()) {
            Assert.assertThrows(AssertionError.class, () -> entry.isRelocatable(scan.getOutput(), null));
            Assert.assertThrows(AssertionError.class, () -> prepared.retarget(entry, column(0), projection));
        } else {
            Assert.assertTrue(entry.isRelocatable(scan.getOutput(), null));
            prepared.retarget(entry, column(0), projection);
        }
        entry.leaves.getQuick(1).close();
        Assert.assertTrue(entry.isRelocatable(scan.getOutput(), null));
        prepared.retarget(entry, column(0), projection);
        Assert.assertFalse(entry.isRebuildRequired);
        prepared.clear();
    }

    @Test
    public void testNestedCallsIgnoreCursorAndFoldedSources() {
        final PreparedFunctions prepared = new PreparedFunctions(4);
        final PreparedFunctions.Entry entry = prepared.begin();
        entry.addColumn(leaf(0));
        final BindableColumn missing = leaf(1);
        entry.addColumn(missing);
        final ProjectPlan subquery = new ProjectPlan().of(new ScanPlan(), 0);
        subquery.getExpressions().add(column(1));
        subquery.getOutput().add(1, "b", ColumnType.INT, true);
        final CursorExpression cursor = new CursorExpression().of(new Subquery().of(subquery, 1, true), 0);
        final ConstantExpression folded = new ConstantExpression().ofInt(7, 0).withSource(call(column(1), column(1)));
        prepared.describe(entry, call(column(0), call(cursor, call(folded, new OuterColumnExpression().of(1, ColumnType.INT, 0)))));
        Assert.assertFalse(PreparedFunctions.hasOnlyReadLeaves(entry));
        missing.close();
        Assert.assertTrue(PreparedFunctions.hasOnlyReadLeaves(entry));
        prepared.clear();
    }

    @Test
    public void testReadLeafValidationVisitsWideExpressionLinearly() {
        for (int count = 64; count <= 128; count *= 2) {
            final PreparedFunctions prepared = new PreparedFunctions(4);
            final CountingArguments arguments = new CountingArguments();
            final PreparedFunctions.Entry entry = prepare(prepared, wideExpression(arguments, count, 0));
            arguments.reads = 0;
            Assert.assertTrue(PreparedFunctions.hasOnlyReadLeaves(entry));
            assertLinearReads(arguments, count);
            prepared.clear();
        }
    }

    @Test
    public void testRelocationVisitsWideExpressionLinearly() {
        for (int count = 64; count <= 128; count *= 2) {
            final PreparedFunctions prepared = new PreparedFunctions(4);
            final CountingArguments arguments = new CountingArguments();
            final PreparedFunctions.Entry entry = prepare(prepared, wideExpression(arguments, count, 0));
            final OutputSchema input = new OutputSchema();
            for (int i = 0; i < count; i++) {
                input.add(i, "c" + i, ColumnType.INT, true);
            }
            arguments.reads = 0;
            Assert.assertTrue(entry.isRelocatable(input, null));
            assertAssertionReads(arguments, count);
            prepared.clear();
        }
    }

    @Test
    public void testRetargetVisitsWideExpressionLinearly() {
        for (int count = 64; count <= 128; count *= 2) {
            final PreparedFunctions prepared = new PreparedFunctions(4);
            final CountingArguments arguments = new CountingArguments();
            final CountingArguments replacementArguments = new CountingArguments();
            final FunctionExpression replacement = wideExpression(replacementArguments, count, count);
            final PreparedFunctions.Entry entry = prepare(prepared, wideExpression(arguments, count, 0));
            final ScanPlan scan = new ScanPlan();
            final ProjectPlan projection = new ProjectPlan().of(scan, 0);
            for (int i = 0; i < count; i++) {
                scan.getOutput().add(count + i, "c" + i, ColumnType.INT, true);
                projection.getOutput().add(i, "c" + i, ColumnType.INT, true);
                projection.getExpressions().add(column(count + i));
            }
            arguments.reads = 0;
            prepared.retarget(entry, replacement, projection);
            Assert.assertSame(replacement, entry.getExpression());
            Assert.assertFalse(entry.isRebuildRequired);
            for (int i = 0; i < count; i++) {
                Assert.assertEquals(count + i, entry.leaves.getQuick(i).getColumnId());
            }
            assertAssertionReads(arguments, count);
            assertAssertionReads(replacementArguments, count);
            prepared.clear();
        }
    }

    private static void assertAssertionReads(CountingArguments arguments, int count) {
        if (PreparedFunctions.class.desiredAssertionStatus()) {
            assertLinearReads(arguments, count);
        } else {
            Assert.assertEquals(0, arguments.reads);
        }
    }

    private static void assertLinearReads(CountingArguments arguments, int count) {
        Assert.assertTrue("argument reads: " + arguments.reads, arguments.reads > 0 && arguments.reads <= 2 * count);
    }

    private static FunctionExpression call(BoundExpression first, BoundExpression second) {
        final FunctionExpression expression = new FunctionExpression();
        expression.getArguments().add(first);
        expression.getArguments().add(second);
        return expression;
    }

    private static ColumnExpression column(int id) {
        return new ColumnExpression().of(id, ColumnType.INT, 0);
    }

    private static BindableColumn leaf(int id) {
        return BindableColumn.newInstance(id, ColumnType.INT, false);
    }

    private static PreparedFunctions.Entry prepare(PreparedFunctions prepared, FunctionExpression expression) {
        final PreparedFunctions.Entry entry = prepared.begin();
        for (int i = 0; i < expression.getArgumentCount(); i++) {
            entry.addColumn(leaf(((ColumnExpression) expression.argumentAt(i)).getColumnId()));
        }
        prepared.describe(entry, expression);
        return entry;
    }

    private static FunctionExpression wideExpression(CountingArguments arguments, int count, int firstId) {
        final FunctionExpression expression = new FunctionExpression(arguments);
        for (int i = 0; i < count; i++) {
            arguments.add(column(firstId + i));
        }
        return expression;
    }

    private static class CountingArguments extends ObjList<BoundExpression> {
        private int reads;

        @Override
        public BoundExpression getQuick(int index) {
            reads++;
            return super.getQuick(index);
        }
    }
}
