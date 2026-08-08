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
import io.questdb.griffin.engine.functions.IntFunction;
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
    /**
     * The nodes every fixture's terms are built from. Never released, so each {@code next()}
     * is a fresh one - these trees stand in for what the parser hands the compiler, and the
     * spec is required to keep nothing of them.
     */
    private static final ObjectPool<ExpressionNode> NODES = new ObjectPool<>(ExpressionNode.FACTORY, 16);

    @Test
    public void testAnExpressionOrderTermDeclines() throws SqlException {
        // The term resolves to no base column, and two windows ordered by two expressions
        // nothing compared must not be read as ordered alike.
        Assert.assertNull(spec(builder().orderBy(literal("x + 1"), IQueryModel.ORDER_DIRECTION_ASCENDING)));
        // ... while the same window ordered by a plain column is a spec.
        Assert.assertNotNull(spec(builder()));
    }

    @Test
    public void testAnExpressionPartitionTermJoinsOnItsRenderedIdentity() throws SqlException {
        // Two windows keyed by x + 1, written and compiled separately. What makes them one
        // group is that their parsed trees render the same identity against one metadata -
        // not that anything compared two compiled functions, which nothing can do.
        final WindowMapSpec left = spec(builder().partitionBy(sum(literal("x"), constant("1"))));
        final WindowMapSpec right = spec(builder().partitionBy(sum(literal("x"), constant("1"))));
        Assert.assertNotNull(left);
        Assert.assertNotNull(right);
        Assert.assertTrue(left.isSameSpec(right));
        // An expression term is a term with no column of its own, and the runtime has to
        // evaluate it: the spec says both, and the compiled terms it carries for that are the
        // ones the window function it was snapshotted for owns.
        Assert.assertTrue(left.hasExpressionPartitionKey());
        Assert.assertEquals(-1, left.getPartitionColumnIndex(0));
        Assert.assertNotNull(left.getPartitionByFunctions());
        // The rendering itself, pinned once: the operation's own token, its arity, and the
        // resolved column beside the literal constant.
        Assert.assertEquals("!+/2(#2:" + ColumnType.DOUBLE + ",=1)", left.getPartitionKeyIdentity());

        // The operation is part of the identity rather than only its operands, which is the
        // one thing Function.isEquivalentTo cannot say - two BinaryFunctions over one pair of
        // columns answer true to it whether they add or subtract.
        assertDistinct(left, builder().partitionBy(difference(literal("x"), constant("1"))));
        // ... and so are the operands, their order, and every constant in them.
        assertDistinct(left, builder().partitionBy(sum(literal("x"), constant("2"))));
        assertDistinct(left, builder().partitionBy(sum(constant("1"), literal("x"))));
        assertDistinct(left, builder().partitionBy(sum(literal("y"), constant("1"))));
        // A direct column renders through the same identity, so a spec is one string however
        // its terms were written - and a column term is never an expression one.
        final WindowMapSpec column = spec(builder());
        Assert.assertNotNull(column);
        Assert.assertFalse(column.hasExpressionPartitionKey());
        Assert.assertNull(column.getPartitionByFunctions());
        Assert.assertEquals("#" + COLUMN_K + ":" + ColumnType.VARCHAR, column.getPartitionKeyIdentity());
        Assert.assertFalse(column.isSameSpec(left));
    }

    @Test
    public void testAPartitionTermThisBuildCannotNameDeclines() throws SqlException {
        // A node kind the identity does not name. Each of these could be given one later, and
        // none of them can be given one by omission - so the window forms no group and its
        // functions keep the maps they own outside one.
        Assert.assertNull(spec(builder().partitionBy(node(ExpressionNode.BIND_VARIABLE, "$1"))));
        Assert.assertNull(spec(builder().partitionBy(node(ExpressionNode.QUERY, "select"))));
        // A name that resolves to no column of the metadata the term was compiled against.
        Assert.assertNull(spec(builder().partitionBy(literal("absent"))));
        // A tree this build renders perfectly well, whose compiled function answers a
        // different partition on every evaluation. Sharing one evaluation between two calls
        // would be a different query, so the term declines on the function rather than on the
        // tree.
        Assert.assertNull(spec(builder().partitionBy(
                node(ExpressionNode.FUNCTION, "rnd_int"),
                new RandomStub(),
                ColumnType.INT
        )));
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
        assertDistinct(reference, builder().partitionBy(literal("y"), new VarcharColumn(COLUMN_Y), ColumnType.VARCHAR));
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

    private static ExpressionNode constant(CharSequence token) {
        return node(ExpressionNode.CONSTANT, token);
    }

    /**
     * {@code left - right}, which is {@link #sum}'s tree with one token changed - the whole of
     * the difference two calls fused on their operands alone would miss.
     */
    private static ExpressionNode difference(ExpressionNode left, ExpressionNode right) {
        return operation("-", left, right);
    }

    private static ExpressionNode literal(CharSequence token) {
        return node(ExpressionNode.LITERAL, token);
    }

    private static ExpressionNode node(int type, CharSequence token) {
        return NODES.next().of(type, token, 0, 0);
    }

    private static ExpressionNode operation(CharSequence token, ExpressionNode left, ExpressionNode right) {
        final ExpressionNode node = node(ExpressionNode.OPERATION, token);
        node.lhs = left;
        node.rhs = right;
        node.paramCount = 2;
        return node;
    }

    private static ExpressionNode sum(ExpressionNode left, ExpressionNode right) {
        return operation("+", left, right);
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
        private boolean isOrderDismissed = true;
        private int keyColumnType = ColumnType.VARCHAR;
        private IntList orderByDirections = directions(IQueryModel.ORDER_DIRECTION_ASCENDING);
        private ObjList<ExpressionNode> orderBy = nodes(literal("ts"));
        private WindowFunction.Pass1ScanDirection pass1ScanDirection = WindowFunction.Pass1ScanDirection.FORWARD;
        private int passCount = WindowFunction.ZERO_PASS;
        private Function partitionByFunction = new VarcharColumn(COLUMN_K);
        private ExpressionNode partitionByNode = literal("k");
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
            final ObjList<ExpressionNode> partitionBy = new ObjList<>();
            if (partitionByFunction == null) {
                partitionByRecord = null;
                keyTypes = null;
            } else {
                partitionBy.add(partitionByNode);
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
                    isOrderDismissed ? scanDirection : RecordCursorFactory.SCAN_DIRECTION_OTHER,
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
                    partitionBy,
                    orderBy,
                    orderByDirections,
                    isOrderDismissed,
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
            this.partitionByNode = null;
            return this;
        }

        Fixture orderBy(ExpressionNode node, int direction) {
            this.orderBy = nodes(node);
            this.orderByDirections = directions(direction);
            return this;
        }

        Fixture orderDismissed(boolean isOrderDismissed) {
            this.isOrderDismissed = isOrderDismissed;
            return this;
        }

        /**
         * An expression term, standing on its parsed tree alone: the compiled function beside
         * it is a constant, which is what any expression looks like to the one question the
         * spec asks a compiled term - whether it is a direct column, which it is not.
         */
        Fixture partitionBy(ExpressionNode node) {
            return partitionBy(node, IntConstant.newInstance(1), ColumnType.DOUBLE);
        }

        Fixture partitionBy(ExpressionNode node, Function function, int keyColumnType) {
            this.partitionByNode = node;
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
     * A compiled term whose tree is canonical and whose value is not: the identity renders it
     * and the spec still declines, because two calls sharing one evaluation of it would be a
     * different query from two calls evaluating it each.
     */
    private static final class RandomStub extends IntFunction {
        @Override
        public int getInt(Record rec) {
            return 0;
        }

        @Override
        public boolean isRandom() {
            return true;
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
