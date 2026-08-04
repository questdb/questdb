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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowContextImpl;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.engine.window.WindowMapSpec;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.WindowExpression;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import org.junit.Assert;
import org.junit.Test;

/**
 * The identity two window functions have to share before they may share one partition map.
 * <p>
 * These cases drive {@link WindowMapSpec#of} through a real {@link WindowContextImpl}, the
 * same class the compiler configures per window column, rather than through a compiled
 * query. That is what lets them vary one part of the identity at a time - including the
 * parts a streaming query cannot reach at all, since the fast path admits only ZERO_PASS
 * functions and so never puts two different pass structures in front of the group compiler.
 * The end-to-end half of the contract, where the specs come off real SQL, is
 * {@code WindowAccumulatorPlanTest}'s.
 */
public class WindowMapSpecTest {

    private static final int COLUMN_K = 1;
    private static final int COLUMN_TS = 0;
    private static final int COLUMN_X = 2;
    private static final int COLUMN_Y = 3;

    @Test
    public void testAnExpressionOrderTermDeclines() throws SqlException {
        // The term resolves to no base column, and two windows ordered by two expressions
        // nothing compared must not be read as ordered alike.
        Assert.assertNull(spec(builder().orderBy(literal("x + 1"), IQueryModel.ORDER_DIRECTION_ASCENDING)));
        // ... while the same window ordered by a plain column is a spec.
        Assert.assertNotNull(spec(builder()));
    }

    @Test
    public void testAnExpressionPartitionTermDeclines() throws SqlException {
        // A constant stands in for any compiled expression: what the spec requires is a
        // direct column reference, because no canonical fingerprint proves two compiled
        // expressions equivalent.
        Assert.assertNull(spec(builder().partitionBy(IntConstant.newInstance(1), ColumnType.INT)));
    }

    @Test
    public void testAnUnpartitionedWindowDeclines() throws SqlException {
        // No key domain to co-locate: such a function keeps its state in scalar fields and
        // owns no map at all, so a group would only add a probe.
        Assert.assertNull(spec(builder().noPartitionBy()));
    }

    @Test
    public void testEveryPartOfTheIdentityDiscriminates() throws SqlException {
        final WindowMapSpec reference = spec(builder());
        Assert.assertNotNull(reference);

        // The key domain: which columns, and how they are written. The two are separate
        // because a live-view compile resolves a SYMBOL key through its string, so one
        // column list can produce two key layouts and those cannot share a map.
        assertDistinct(reference, builder().partitionBy(new VarcharColumn(COLUMN_Y), ColumnType.VARCHAR));
        assertDistinct(reference, builder().keyColumnType(ColumnType.STRING));

        // The row order the cumulative frame is accumulated in.
        assertDistinct(reference, builder().orderBy(literal("y"), IQueryModel.ORDER_DIRECTION_ASCENDING));
        assertDistinct(reference, builder().orderBy(literal("ts"), IQueryModel.ORDER_DIRECTION_DESCENDING));
        assertDistinct(reference, builder().orderDismissed(false));
        assertDistinct(reference, builder().scanDirection(RecordCursorFactory.SCAN_DIRECTION_BACKWARD));

        // The frame.
        assertDistinct(reference, builder().framingMode(WindowExpression.FRAMING_RANGE));
        assertDistinct(reference, builder().rowsLo(-10));
        assertDistinct(reference, builder().rowsHi(-1));
        // The exclusion beside the bound it was folded into. A high bound of -1 is already
        // below the current row, so the fold does not fire and the kind is the only
        // difference left - which is exactly the case that would be invisible if the spec
        // carried the folded bound alone.
        assertDistinct(
                builder().rowsHi(-1).build(),
                builder().rowsHi(-1).exclusionKind(WindowExpression.EXCLUDE_CURRENT_ROW)
        );

        // The traversal itself. Neither of these is reachable from the streaming path,
        // which admits ZERO_PASS functions alone; both are in the identity for the cached
        // executor, whose sort groups may hold several pass structures at once.
        assertDistinct(reference, builder().passCount(WindowFunction.TWO_PASS));
        assertDistinct(reference, builder().pass1ScanDirection(WindowFunction.Pass1ScanDirection.BACKWARD));

        // The designated timestamp a RANGE bound is expressed in.
        assertDistinct(reference, builder().timestampIndex(-1));
        assertDistinct(reference, builder().timestampType(ColumnType.TIMESTAMP_NANO));
    }

    @Test
    public void testTheBoundsAreTheOnesTheRuntimeEvaluates() throws SqlException {
        // EXCLUDE CURRENT ROW over a frame ending at the current row is a frame ending one
        // unit below it, and the spec records the frame the factories dispatch on rather
        // than the one the model holds.
        final WindowMapSpec excluded = spec(builder().exclusionKind(WindowExpression.EXCLUDE_CURRENT_ROW));
        Assert.assertNotNull(excluded);
        Assert.assertEquals(-1, excluded.getRowsHi());

        // A RANGE bound written in seconds is stored in the designated timestamp's own
        // units, so two windows whose bounds are one width written two ways are one spec.
        final WindowMapSpec seconds = spec(builder()
                .framingMode(WindowExpression.FRAMING_RANGE)
                .rowsLo(-5, 's')
                .rowsHi(0));
        final WindowMapSpec micros = spec(builder()
                .framingMode(WindowExpression.FRAMING_RANGE)
                .rowsLo(-5_000_000L)
                .rowsHi(0));
        Assert.assertNotNull(seconds);
        Assert.assertNotNull(micros);
        Assert.assertEquals(-5_000_000L, seconds.getRowsLo());
        Assert.assertTrue(seconds.isSameSpec(micros));
    }

    @Test
    public void testTwoSpellingsOfOneWindowAreOneSpec() throws SqlException {
        final WindowMapSpec left = spec(builder());
        final WindowMapSpec right = spec(builder());
        Assert.assertNotNull(left);
        Assert.assertNotNull(right);
        Assert.assertNotSame(left, right);
        Assert.assertTrue(left.isSameSpec(right));
        Assert.assertTrue(right.isSameSpec(left));
        Assert.assertTrue(left.isSameSpec(left));
        // What the spec resolved the window to, read back once so the fixtures below are
        // varying something the reference actually holds.
        Assert.assertEquals(1, left.getPartitionColumnCount());
        Assert.assertEquals(COLUMN_K, left.getPartitionColumnIndex(0));
        Assert.assertEquals(1, left.getKeyColumnCount());
        Assert.assertEquals(ColumnType.VARCHAR, left.getKeyColumnType(0));
        Assert.assertEquals(1, left.getOrderColumnCount());
        Assert.assertEquals(COLUMN_TS, left.getOrderColumnIndex(0));
    }

    private static void assertDistinct(WindowMapSpec reference, Fixture fixture) throws SqlException {
        final WindowMapSpec other = spec(fixture);
        Assert.assertNotNull(String.valueOf(fixture), other);
        Assert.assertFalse(String.valueOf(other), reference.isSameSpec(other));
        Assert.assertFalse(String.valueOf(other), other.isSameSpec(reference));
    }

    /**
     * The reference window: {@code PARTITION BY k ORDER BY ts ROWS BETWEEN UNBOUNDED
     * PRECEDING AND CURRENT ROW}, order dismissed against a forward base scan. Every case
     * above varies one part of it.
     */
    private static Fixture builder() {
        return new Fixture();
    }

    private static ExpressionNode literal(CharSequence token) {
        return new ObjectPool<>(ExpressionNode.FACTORY, 1).next().of(ExpressionNode.LITERAL, token, 0, 0);
    }

    private static RecordMetadata metadata() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("k", ColumnType.VARCHAR));
        metadata.add(new TableColumnMetadata("x", ColumnType.DOUBLE));
        metadata.add(new TableColumnMetadata("y", ColumnType.VARCHAR));
        return metadata;
    }

    private static WindowMapSpec spec(Fixture fixture) throws SqlException {
        return fixture.build();
    }

    /**
     * One window, configured onto a real {@link WindowContextImpl} exactly as the compiler
     * configures it, with a setter per part of the identity.
     */
    private static final class Fixture {
        private final RecordMetadata metadata = metadata();
        private int exclusionKind = WindowExpression.EXCLUDE_NO_OTHERS;
        private int framingMode = WindowExpression.FRAMING_ROWS;
        private int keyColumnType = ColumnType.VARCHAR;
        private IntList orderByDirections = directions(IQueryModel.ORDER_DIRECTION_ASCENDING);
        private ObjList<ExpressionNode> orderBy = nodes(literal("ts"));
        private boolean orderDismissed = true;
        private WindowFunction.Pass1ScanDirection pass1ScanDirection = WindowFunction.Pass1ScanDirection.FORWARD;
        private int passCount = WindowFunction.ZERO_PASS;
        private Function partitionByFunction = new VarcharColumn(COLUMN_K);
        private long rowsHi = 0;
        private char rowsHiUnit = 0;
        private long rowsLo = Long.MIN_VALUE;
        private char rowsLoUnit = 0;
        private int scanDirection = RecordCursorFactory.SCAN_DIRECTION_FORWARD;
        private int timestampIndex = COLUMN_TS;
        private int timestampType = ColumnType.TIMESTAMP;

        WindowMapSpec build() throws SqlException {
            final VirtualRecord partitionByRecord;
            final ColumnTypes keyTypes;
            if (partitionByFunction == null) {
                partitionByRecord = null;
                keyTypes = null;
            } else {
                final ObjList<Function> partitionByFunctions = new ObjList<>();
                partitionByFunctions.add(partitionByFunction);
                partitionByRecord = new VirtualRecord(partitionByFunctions);
                final ArrayColumnTypes types = new ArrayColumnTypes();
                types.add(keyColumnType);
                keyTypes = types;
            }
            final WindowContextImpl context = new WindowContextImpl();
            context.clear();
            context.of(
                    partitionByRecord,
                    null,
                    keyTypes,
                    orderBy.size() > 0,
                    orderDismissed ? scanDirection : RecordCursorFactory.SCAN_DIRECTION_OTHER,
                    0,
                    framingMode,
                    rowsLo,
                    rowsLoUnit,
                    0,
                    0,
                    rowsHi,
                    rowsHiUnit,
                    0,
                    0,
                    exclusionKind,
                    0,
                    timestampIndex,
                    timestampType,
                    false,
                    0
            );
            return WindowMapSpec.of(
                    context,
                    orderBy,
                    orderByDirections,
                    orderDismissed,
                    new PassStub(passCount, pass1ScanDirection),
                    metadata,
                    // These functions read the record the metadata describes, which is the
                    // streaming case: the two arguments differ only for a cached compile,
                    // whose record chain leaves a hole at every window output's index.
                    metadata
            );
        }

        Fixture exclusionKind(int exclusionKind) {
            this.exclusionKind = exclusionKind;
            return this;
        }

        Fixture framingMode(int framingMode) {
            this.framingMode = framingMode;
            return this;
        }

        Fixture keyColumnType(int keyColumnType) {
            this.keyColumnType = keyColumnType;
            return this;
        }

        Fixture noPartitionBy() {
            this.partitionByFunction = null;
            return this;
        }

        Fixture orderBy(ExpressionNode node, int direction) {
            this.orderBy = nodes(node);
            this.orderByDirections = directions(direction);
            return this;
        }

        Fixture orderDismissed(boolean orderDismissed) {
            this.orderDismissed = orderDismissed;
            return this;
        }

        Fixture partitionBy(Function function, int keyColumnType) {
            this.partitionByFunction = function;
            this.keyColumnType = keyColumnType;
            return this;
        }

        Fixture pass1ScanDirection(WindowFunction.Pass1ScanDirection pass1ScanDirection) {
            this.pass1ScanDirection = pass1ScanDirection;
            return this;
        }

        Fixture passCount(int passCount) {
            this.passCount = passCount;
            return this;
        }

        Fixture rowsHi(long rowsHi) {
            this.rowsHi = rowsHi;
            return this;
        }

        Fixture rowsLo(long rowsLo) {
            this.rowsLo = rowsLo;
            return this;
        }

        Fixture rowsLo(long rowsLo, char unit) {
            this.rowsLo = rowsLo;
            this.rowsLoUnit = unit;
            return this;
        }

        Fixture scanDirection(int scanDirection) {
            this.scanDirection = scanDirection;
            return this;
        }

        Fixture timestampIndex(int timestampIndex) {
            this.timestampIndex = timestampIndex;
            return this;
        }

        Fixture timestampType(int timestampType) {
            this.timestampType = timestampType;
            return this;
        }

        private static IntList directions(int direction) {
            final IntList directions = new IntList();
            directions.add(direction);
            return directions;
        }

        private static ObjList<ExpressionNode> nodes(ExpressionNode node) {
            final ObjList<ExpressionNode> nodes = new ObjList<>();
            nodes.add(node);
            return nodes;
        }
    }

    /**
     * A window function that is nothing but the pass structure the spec reads off it. The
     * argument is a column of the fixture's base so the stub is a plausible SELECT-list
     * function rather than a degenerate one; nothing here evaluates it.
     */
    private static final class PassStub extends BaseWindowFunction {
        private final WindowFunction.Pass1ScanDirection pass1ScanDirection;
        private final int passCount;

        private PassStub(int passCount, WindowFunction.Pass1ScanDirection pass1ScanDirection) {
            super(DoubleColumn.newInstance(COLUMN_X));
            this.passCount = passCount;
            this.pass1ScanDirection = pass1ScanDirection;
        }

        @Override
        public String getName() {
            return "pass_stub";
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return pass1ScanDirection;
        }

        @Override
        public int getPassCount() {
            return passCount;
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }
    }
}
