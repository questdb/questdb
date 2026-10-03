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

package io.questdb.test.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.griffin.CharacterStore;
import io.questdb.griffin.CharacterStoreEntry;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.engine.functions.bool.AndFunctionFactory;
import io.questdb.griffin.engine.functions.bool.OrFunctionFactory;
import io.questdb.griffin.engine.functions.groupby.CountGroupByFunctionFactory;
import io.questdb.griffin.plan.logical.AggregatePlan;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.ConstantExpression;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.JoinInput;
import io.questdb.griffin.plan.logical.JoinKind;
import io.questdb.griffin.plan.logical.JoinPlan;
import io.questdb.griffin.plan.logical.LimitPlan;
import io.questdb.griffin.plan.logical.LogicalPlanPrinter;
import io.questdb.griffin.plan.logical.OuterColumnExpression;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.griffin.plan.logical.ProjectPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.griffin.plan.logical.SortDirection;
import io.questdb.griffin.plan.logical.SortPlan;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class LogicalPlanTest {
    @Test
    public void testBooleanBindingClassificationKeepsOverridesSeparate() throws Exception {
        FunctionFactoryDescriptor overload = new FunctionFactoryDescriptor(new AndFunctionFactory());
        Assert.assertTrue(overload.isAnd());
        Assert.assertFalse(overload.isOr());
        overload = new FunctionFactoryDescriptor(new OrFunctionFactory());
        Assert.assertFalse(overload.isAnd());
        Assert.assertTrue(overload.isOr());
        overload = new FunctionFactoryDescriptor(new AndFunctionFactory() {
        });
        Assert.assertFalse(overload.isAnd());
        Assert.assertFalse(overload.isOr());
        overload = new FunctionFactoryDescriptor(new OrFunctionFactory() {
        });
        Assert.assertFalse(overload.isAnd());
        Assert.assertFalse(overload.isOr());
    }

    @Test
    public void testAggregatePoolResetPreservesBorrowedExpressionAndInput() throws Exception {
        final ObjectPool<AggregatePlan> aggregates = new ObjectPool<>(AggregatePlan.FACTORY, 1);
        final ScanPlan source = scan();
        source.getOutput().add(11, "value", ColumnType.INT, true);
        final FunctionFactoryDescriptor overload = new FunctionFactoryDescriptor(new CountGroupByFunctionFactory());
        final FunctionExpression count = new FunctionExpression().of(overload, new ObjList<>(), new IntList(), ColumnType.LONG, 0, 7);
        final AggregatePlan aggregate = aggregates.next().of(source, 3);
        aggregate.getAggregates().add(count);
        final ColumnExpression key = new ColumnExpression().of(11, ColumnType.INT, 5);
        aggregate.getGroupingExpressions().add(key);
        aggregate.getOutput().add(20, "value", ColumnType.INT, true);
        aggregate.getOutput().add(21, "count", ColumnType.LONG, true);
        final ObjList<FunctionExpression> expressions = aggregate.getAggregates();
        final OutputSchema output = aggregate.getOutput();
        Assert.assertTrue(aggregate instanceof AggregatePlan);
        Assert.assertEquals(1, aggregate.inputCount());
        Assert.assertSame(source, aggregate.inputAt(0));
        Assert.assertSame(count, expressions.getQuick(0));
        Assert.assertSame(key, aggregate.getGroupingExpressions().getQuick(0));
        Assert.assertEquals(count.getDataType(), output.getColumnType(1));
        Assert.assertEquals(-1, output.getTimestampIndex());

        final ScanPlan replacement = scan();
        aggregate.replaceInput(0, replacement);
        Assert.assertSame(replacement, aggregate.getInput());
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> aggregate.replaceInput(1, source));
        aggregates.clear();
        Assert.assertSame(aggregate, aggregates.next());
        Assert.assertSame(expressions, aggregate.getAggregates());
        Assert.assertEquals(0, expressions.size());
        Assert.assertEquals(0, aggregate.getGroupingExpressions().size());
        Assert.assertEquals(11, key.getColumnId());
        Assert.assertSame(output, aggregate.getOutput());
        Assert.assertEquals(0, output.getColumnCount());
        Assert.assertNull(aggregate.getInput());
        Assert.assertEquals(-1, aggregate.getPosition());
        Assert.assertEquals(1, source.getOutput().getColumnCount());
        Assert.assertSame(overload, count.getOverload());
        Assert.assertEquals(ColumnType.LONG, count.getDataType());
        Assert.assertEquals(7, count.getPosition());
    }

    @Test
    public void testClearingConsumerDoesNotClearSharedInputsOrExpressions() {
        final ScanPlan source = scan();
        source.getOutput().add(21, "value", ColumnType.LONG, true);
        source.getSourceColumnIndexes().add(0);
        final ConstantExpression predicate = new ConstantExpression().ofBoolean(true, 8);
        final FilterPlan first = new FilterPlan().of(source, predicate, 4);
        final FilterPlan second = new FilterPlan().of(source, predicate, 5);
        first.getOutput().copyFrom(source.getOutput());
        second.getOutput().copyFrom(source.getOutput());

        first.clear();
        Assert.assertTrue(first instanceof FilterPlan);
        Assert.assertNull(first.getInput());
        Assert.assertNull(first.getPredicate());
        Assert.assertEquals(0, first.getOutput().getColumnCount());
        Assert.assertSame(source, second.inputAt(0));
        Assert.assertSame(predicate, second.getPredicate());
        Assert.assertEquals(1, source.getOutput().getColumnCount());
        Assert.assertEquals(21, second.getOutput().getColumnId(0));
        Assert.assertEquals(ColumnType.BOOLEAN, predicate.getDataType());
        Assert.assertEquals(1, predicate.getLongValue());

        final ScanPlan replacement = scan();
        second.replaceInput(0, replacement);
        Assert.assertSame(replacement, second.getInput());
        Assert.assertEquals(1, source.getOutput().getColumnCount());
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> second.replaceInput(1, source));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> source.inputAt(0));
    }

    @Test
    public void testColumnIdentitySurvivesChangedPhysicalPosition() {
        final OutputSchema schema = new OutputSchema();
        schema.add(31, "value", ColumnType.LONG, true);
        schema.add(47, "ts", ColumnType.TIMESTAMP, false);
        schema.setTimestampIndex(1);
        final ColumnExpression timestamp = new ColumnExpression().of(47, ColumnType.TIMESTAMP, 12);
        Assert.assertEquals(1, schema.getColumnIndexById(timestamp.getColumnId()));
        Assert.assertFalse(schema.isVisible(1));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("ts"));
        Assert.assertEquals(47, schema.getTimestampColumnId());

        schema.clear();
        schema.add(47, "ts", ColumnType.TIMESTAMP, true);
        schema.setTimestampIndex(0);
        Assert.assertEquals(0, schema.getColumnIndexById(timestamp.getColumnId()));
        Assert.assertEquals(-1, schema.getColumnIndexById(31));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("value"));
        Assert.assertEquals(47, schema.getTimestampColumnId());
        Assert.assertEquals(12, timestamp.getPosition());
        Assert.assertTrue(schema.isVisible(0));
    }

    @Test
    public void testDependentStepPrintsOuterReferencesOfItsInput() {
        final ScanPlan outer = scan();
        outer.getOutput().add(1, "k", ColumnType.INT, true);
        final ScanPlan source = scan();
        source.getOutput().add(2, "v", ColumnType.INT, true);
        final ProjectPlan body = new ProjectPlan().of(source, 4);
        body.getExpressions().add(new OuterColumnExpression().of(1, ColumnType.INT, 5));
        body.getExpressions().add(new ColumnExpression().of(2, ColumnType.INT, 6));
        body.getOutput().add(3, "ok", ColumnType.INT, true);
        body.getOutput().add(7, "v", ColumnType.INT, true);
        final JoinPlan join = new JoinPlan().of(8);
        join.getInputs().add(new JoinInput().of(outer, JoinKind.CROSS, "t", 0));
        final JoinInput step = new JoinInput().of(body, JoinKind.CROSS, "l", 9);
        step.setDependent(true);
        join.getInputs().add(step);
        join.getOutput().add(1, "k", ColumnType.INT, null, true, "t");
        join.getOutput().add(3, "ok", ColumnType.INT, null, true, "l");
        join.getOutput().add(7, "v", ColumnType.INT, null, true, "l");
        TestUtils.assertEquals("""
                Join
                  Master t
                    Scan
                      table: trades
                      columns: [k]
                  DEPENDENT CROSS l
                    Project
                      columns: [outer(t.k) AS ok, v]
                      Scan
                        table: trades
                        columns: [v]
                """, new LogicalPlanPrinter().print(join));
    }

    @Test
    public void testOuterColumnExpressionKeepsIdentityUntilReset() {
        final ObjectPool<OuterColumnExpression> references = new ObjectPool<>(OuterColumnExpression.FACTORY, 1);
        final OuterColumnExpression reference = references.next().of(12, ColumnType.SYMBOL, 9);
        Assert.assertEquals(12, reference.getColumnId());
        Assert.assertEquals(ColumnType.SYMBOL, reference.getDataType());
        Assert.assertEquals(9, reference.getPosition());
        Assert.assertThrows(IllegalArgumentException.class, () -> new OuterColumnExpression().of(-1, ColumnType.INT, 0));
        references.clear();
        Assert.assertSame(reference, references.next());
        Assert.assertEquals(-1, reference.getColumnId());
        Assert.assertEquals(ColumnType.UNDEFINED, reference.getDataType());
    }

    @Test
    public void testPoolReuseClearsOwnedContainersAndPayloads() {
        final ObjectPool<ScanPlan> scans = new ObjectPool<>(ScanPlan.FACTORY, 1);
        final ObjectPool<ProjectPlan> projects = new ObjectPool<>(ProjectPlan.FACTORY, 1);
        final ObjectPool<SortPlan> sorts = new ObjectPool<>(SortPlan.FACTORY, 1);
        final ObjectPool<LimitPlan> limits = new ObjectPool<>(LimitPlan.FACTORY, 1);
        final ObjectPool<ColumnExpression> columns = new ObjectPool<>(ColumnExpression.FACTORY, 1);
        final ObjectPool<ConstantExpression> constants = new ObjectPool<>(ConstantExpression.FACTORY, 1);
        final ScanPlan scan = scans.next().of(tableToken(), 17, 3, true);
        scan.getOutput().add(42, "x", ColumnType.LONG, true);
        scan.getSourceColumnIndexes().add(5);
        final ColumnExpression column = columns.next().of(42, ColumnType.LONG, 6);
        final ProjectPlan project = projects.next().of(scan, 9);
        project.getExpressions().add(column);
        final SortPlan sort = sorts.next().of(project, 11);
        sort.getColumnIds().add(42);
        sort.getDirections().add(SortDirection.DESCENDING);
        final ConstantExpression lo = constants.next().ofLong(-7, 16);
        final LimitPlan limit = limits.next().of(sort, lo, null, 14);
        final OutputSchema output = scan.getOutput();
        final IntList indexes = scan.getSourceColumnIndexes();
        final ObjList<BoundExpression> expressions = project.getExpressions();
        final IntList sortColumns = sort.getColumnIds();
        final ObjList<SortDirection> directions = sort.getDirections();

        scans.clear();
        projects.clear();
        sorts.clear();
        limits.clear();
        columns.clear();
        constants.clear();
        Assert.assertSame(scan, scans.next());
        Assert.assertSame(project, projects.next());
        Assert.assertSame(sort, sorts.next());
        Assert.assertSame(limit, limits.next());
        Assert.assertTrue(scan instanceof ScanPlan);
        Assert.assertTrue(project instanceof ProjectPlan);
        Assert.assertTrue(sort instanceof SortPlan);
        Assert.assertTrue(limit instanceof LimitPlan);
        Assert.assertSame(column, columns.next());
        Assert.assertSame(lo, constants.next());
        Assert.assertSame(output, scan.getOutput());
        Assert.assertEquals(0, output.getColumnCount());
        Assert.assertEquals(-1, output.getTimestampColumnId());
        Assert.assertSame(indexes, scan.getSourceColumnIndexes());
        Assert.assertEquals(0, indexes.size());
        Assert.assertNull(scan.getTableToken());
        Assert.assertFalse(scan.isUpdate());
        Assert.assertEquals(-1, scan.getMetadataVersion());
        Assert.assertEquals(-1, scan.getPosition());
        Assert.assertSame(expressions, project.getExpressions());
        Assert.assertEquals(0, expressions.size());
        Assert.assertNull(project.getInput());
        Assert.assertSame(sortColumns, sort.getColumnIds());
        Assert.assertSame(directions, sort.getDirections());
        Assert.assertEquals(0, sortColumns.size());
        Assert.assertEquals(0, directions.size());
        Assert.assertNull(limit.getLo());
        Assert.assertNull(limit.getHi());
        Assert.assertNull(limit.getInput());
        Assert.assertEquals(-1, column.getColumnId());
        Assert.assertEquals(ColumnType.UNDEFINED, column.getDataType());
        Assert.assertEquals(-1, column.getPosition());
        Assert.assertEquals(ColumnType.UNDEFINED, lo.getDataType());
        Assert.assertEquals(0, lo.getLongValue());
        lo.ofLong(Long.MIN_VALUE, 1);
        Assert.assertEquals(Long.MIN_VALUE, lo.getLongValue());
        Assert.assertEquals(ColumnType.LONG, lo.getDataType());
    }

    @Test
    public void testSchemaCopyRetainsIdentityWithoutSharingMutableContainers() {
        final OutputSchema source = new OutputSchema();
        final OutputSchema nested = new OutputSchema();
        nested.add(7, "nested", ColumnType.LONG, true);
        source.add(3, "hidden", ColumnType.RECORD, nested, false, "source");
        source.add(11, "ts", ColumnType.TIMESTAMP, true);
        source.setTimestampIndex(1);
        final OutputSchema copy = new OutputSchema();
        copy.copyFrom(source);
        copy.copyFrom(copy);
        source.clear();

        Assert.assertEquals(2, copy.getColumnCount());
        Assert.assertEquals(3, copy.getColumnId(0));
        Assert.assertFalse(copy.isVisible(0));
        Assert.assertSame(nested, copy.getMetadata(0));
        TestUtils.assertEquals("source", copy.getColumnQualifier(0));
        Assert.assertNull(copy.getColumnQualifier(1));
        Assert.assertTrue(copy.hasColumnQualifiers());
        Assert.assertTrue(copy.hasColumnQualifier("SOURCE"));
        Assert.assertFalse(source.hasColumnQualifiers());
        Assert.assertNull(copy.getMetadata(1));
        Assert.assertEquals(ColumnType.RECORD, copy.getColumnType(0));
        Assert.assertEquals(11, copy.getColumnId(1));
        Assert.assertEquals(11, copy.getTimestampColumnId());
        Assert.assertEquals(1, copy.getColumnIndexQuiet("ts"));
        Assert.assertEquals(1, copy.getColumnIndexQuiet("t.TS", 2, 4));
        Assert.assertEquals(-1, source.getColumnIndexQuiet("ts"));
        Assert.assertThrows(IndexOutOfBoundsException.class, () -> source.setTimestampIndex(0));
        // Intermediate join outputs may contain equal names from different scopes.
        // Logical identity must still distinguish them without renaming the columns.
        copy.add(15, "ts", ColumnType.LONG, true);
        Assert.assertEquals(1, copy.getColumnIndexById(11));
        Assert.assertEquals(2, copy.getColumnIndexById(15));
        Assert.assertEquals(11, copy.getTimestampColumnId());
        Assert.assertEquals(3, copy.getColumnCount());
    }

    @Test
    public void testSchemaQualifiedLookupDistinguishesAmbiguousReferences() {
        final OutputSchema schema = new OutputSchema()
                .add(1, "id", ColumnType.INT, null, true, "left")
                .add(2, "ID", ColumnType.LONG, null, true, "right")
                .add(3, "unique", ColumnType.INT, true);
        Assert.assertEquals(0, schema.getColumnIndexQuiet("id"));
        Assert.assertEquals(OutputSchema.COLUMN_AMBIGUOUS, schema.getColumnIndexQuiet(null, "id", 0, 2));
        Assert.assertEquals(0, schema.getColumnIndexQuiet("LEFT", "left.ID", 5, 7));
        Assert.assertEquals(1, schema.getColumnIndexQuiet("right", "id", 0, 2));
        Assert.assertEquals(2, schema.getColumnIndexQuiet(null, "unique", 0, 6));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("right", "unique", 0, 6));
        Assert.assertFalse(schema.hasColumnQualifier("unknown"));
        schema.add(4, "id", ColumnType.INT, null, false, "left");
        Assert.assertEquals(0, schema.getColumnIndexQuiet("left", "id", 0, 2));
        Assert.assertEquals(3, schema.getColumnIndexById(4));
        schema.add(6, "", ColumnType.TIMESTAMP, null, false, "left");
        schema.setTimestampIndex(4);
        Assert.assertEquals(-1, schema.getColumnIndexQuiet(""));
        Assert.assertEquals(-1, schema.getColumnIndexQuiet("left", "", 0, 0));
        Assert.assertEquals(6, schema.getTimestampColumnId());
        schema.clear();
        schema.add(5, "id", ColumnType.INT, true);
        Assert.assertFalse(schema.hasColumnQualifiers());
        Assert.assertFalse(schema.hasColumnQualifier("left"));
        Assert.assertNull(schema.getColumnQualifier(0));
    }

    @Test
    public void testWarmedRepresentationBuildRemapAndResetAllocatesNoJavaHeap() throws Exception {
        try (TestUtils.ThreadMetricsScope<com.sun.management.ThreadMXBean> scope = TestUtils.threadAllocationScope()) {
            final com.sun.management.ThreadMXBean threadMXBean = scope.getBean();
            final ObjectPool<ScanPlan> scans = new ObjectPool<>(ScanPlan.FACTORY, 1);
            final ObjectPool<ProjectPlan> projects = new ObjectPool<>(ProjectPlan.FACTORY, 1);
            final ObjectPool<ColumnExpression> columns = new ObjectPool<>(ColumnExpression.FACTORY, 2);
            final CharacterStore characters = new CharacterStore(64, 2);
            final OutputSchema fixture = new OutputSchema();
            fixture.add(11, "first", ColumnType.LONG, true);
            fixture.add(12, "unused", ColumnType.BOOLEAN, false);
            fixture.add(13, "last", ColumnType.LONG, true);
            final TableToken token = tableToken();
            for (int i = 0; i < 20_000; i++) {
                buildAndRemap(scans, projects, columns, characters, fixture, token);
            }

            long minAllocatedBytes = Long.MAX_VALUE;
            long checksum = 0;
            for (int round = 0; round < 5; round++) {
                final long allocatedBefore = threadMXBean.getCurrentThreadAllocatedBytes();
                for (int i = 0; i < 10_000; i++) {
                    checksum += buildAndRemap(scans, projects, columns, characters, fixture, token);
                }
                minAllocatedBytes = Math.min(minAllocatedBytes, threadMXBean.getCurrentThreadAllocatedBytes() - allocatedBefore);
            }
            Assert.assertEquals(50_000, checksum);
            Assert.assertEquals(0, minAllocatedBytes);
            Assert.assertEquals(13, scans.peekQuick(0).getOutput().getColumnId(0));
            Assert.assertEquals(11, scans.peekQuick(0).getOutput().getColumnId(1));
            TestUtils.assertEquals("last", projects.peekQuick(0).getOutput().getColumnName(0));
        }
    }

    private static int buildAndRemap(
            ObjectPool<ScanPlan> scans,
            ObjectPool<ProjectPlan> projects,
            ObjectPool<ColumnExpression> columns,
            CharacterStore characters,
            OutputSchema fixture,
            TableToken token
    ) {
        scans.clear();
        projects.clear();
        columns.clear();
        characters.clear();
        final ScanPlan scan = scans.next().of(token, 2, 0);
        scan.getOutput().copyFrom(fixture);
        final ProjectPlan project = projects.next().of(scan, 4);
        for (int i = 0; i < 2; i++) {
            final int index = i == 0 ? 2 : 0;
            project.getExpressions().add(columns.next().of(fixture.getColumnId(index), fixture.getColumnType(index), 7 + i));
            final CharacterStoreEntry name = characters.newEntry();
            name.put(fixture.getColumnName(index));
            project.getOutput().add(20 + i, name.toImmutable(), fixture.getColumnType(index), true);
        }
        // Remove an unused scan column and change physical positions while expressions
        // retain the identities assigned before the remap.
        scan.getOutput().clear();
        for (int i = 0; i < 2; i++) {
            final int index = i == 0 ? 2 : 0;
            scan.getOutput().add(fixture.getColumnId(index), fixture.getColumnName(index), fixture.getColumnType(index), true);
            scan.getSourceColumnIndexes().add(index);
        }
        return scan.getOutput().getColumnIndexById(((ColumnExpression) project.getExpressions().getQuick(0)).getColumnId())
                + scan.getOutput().getColumnIndexById(((ColumnExpression) project.getExpressions().getQuick(1)).getColumnId());
    }

    private static ScanPlan scan() {
        return new ScanPlan().of(tableToken(), 2, 0);
    }

    private static TableToken tableToken() {
        return new TableToken("trades", "trades~1", null, 1, false, false, false);
    }
}
