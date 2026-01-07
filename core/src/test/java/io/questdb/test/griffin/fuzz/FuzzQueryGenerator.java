package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.TextPlanSink;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.griffin.fuzz.joins.AsofJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.FullOuterJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.InnerJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.JoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.LeftOuterJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.LtJoinOperatorFactory;
import io.questdb.test.griffin.fuzz.joins.RightOuterJoinOperatorFactory;

public final class FuzzQueryGenerator {
    private final ObjList<JoinOperatorFactory> joinFactories;
    private final int maxJoins;

    /**
     * Creates a generator that composes join operators up to the supplied limits.
     */
    public FuzzQueryGenerator(
            ObjList<JoinOperatorFactory> joinFactories,
            int maxJoins
    ) {
        this.joinFactories = joinFactories;
        this.maxJoins = maxJoins;
    }

    /**
     * Builds a fuzz case by composing join operators (0..maxJoins) and optional filter/order/limit steps.
     * The output includes an under-test factory and an oracle factory that should be equivalent
     * for comparison, plus ordering metadata for result normalization so row-wise equality is stable.
     */
    public FuzzCase generate(
            FuzzBuildContext context,
            Rnd rnd,
            CursorFactoryProvider leftProvider,
            CursorFactoryProvider rightProvider
    ) throws Exception {
        OperatorState state = new OperatorState(leftProvider.get(), leftProvider.get(), false);
        // Mix base and virtual projections to cover both page-frame and row-by-row join inputs.
        if (rnd.nextBoolean()) {
            state.underTest = VirtualRecordCursorFactorySupport.wrap(state.underTest);
            state.oracle = VirtualRecordCursorFactorySupport.wrap(state.oracle);
        }
        int joinTarget = rnd.nextInt(maxJoins + 1);

        for (int i = 0; i < joinTarget; i++) {
            RecordCursorFactory rightUnderTest = rightProvider.get();
            RecordCursorFactory rightOracle = rightProvider.get();
            JoinOperatorFactory joinFactory = pickJoinFactory(rnd, state.underTest, rightUnderTest);
            if (joinFactory == null) {
                break;
            }
            // Mix virtual inputs per-join to exercise non-page-frame factory paths, but keep
            // some hash-join cases on random-access cursors to hit light hash joins.
            boolean isHashJoin = joinFactory instanceof InnerJoinOperatorFactory
                    || joinFactory instanceof LeftOuterJoinOperatorFactory
                    || joinFactory instanceof RightOuterJoinOperatorFactory
                    || joinFactory instanceof FullOuterJoinOperatorFactory;
            boolean wrapRight = rnd.nextBoolean() && (!isHashJoin || rnd.nextInt(100) < 30);
            if (wrapRight) {
                rightUnderTest = VirtualRecordCursorFactorySupport.wrap(rightUnderTest);
                rightOracle = VirtualRecordCursorFactorySupport.wrap(rightOracle);
            }
            JoinScanRequirement requirement = joinFactory.scanRequirement();
            if (requirement.left().requiresOrdering()) {
                state.underTest = OrderBySupport.orderByTimestamp(context, state.underTest, requirement.left());
                state.oracle = OrderBySupport.orderByTimestamp(context, state.oracle, requirement.left());
            }
            if (requirement.right().requiresOrdering()) {
                rightUnderTest = OrderBySupport.orderByTimestamp(context, rightUnderTest, requirement.right());
                rightOracle = OrderBySupport.orderByTimestamp(context, rightOracle, requirement.right());
            }
            String slaveAlias = "slave" + i;
            JoinInputs inputs = new JoinInputs(state.underTest, rightUnderTest, state.oracle, rightOracle);
            FuzzFactories factories = joinFactory.build(context, inputs, rnd, slaveAlias);
            state.underTest = factories.underTest;
            state.oracle = factories.oracle;
            state.ordered = requirement.left().requiresOrdering();
            if (!joinFactory.canChain()) {
                break;
            }
        }

        FilterOperatorFactory filterFactory = new FilterOperatorFactory();
        if (filterFactory.supports(state.underTest) && rnd.nextBoolean()) {
            state = filterFactory.apply(context, state, rnd);
        }
        OrderByOperatorFactory orderByFactory = new OrderByOperatorFactory();
        if (orderByFactory.supports(state.underTest) && rnd.nextBoolean()) {
            state = orderByFactory.apply(context, state, rnd);
        }
        LimitOperatorFactory limitFactory = new LimitOperatorFactory();
        if (limitFactory.supports(state.underTest) && rnd.nextBoolean()) {
            if (!state.ordered) {
                if (orderByFactory.supports(state.underTest)) {
                    state = orderByFactory.apply(context, state, rnd);
                } else {
                    // Can't safely apply LIMIT without deterministic ordering for comparison.
                    limitFactory = null;
                }
            }
            if (limitFactory != null) {
                state = limitFactory.apply(context, state, rnd);
            }
        }

        String description = buildDescription(state.underTest, state.oracle);
        return new FuzzCase(
                new SizeCheckingRecordCursorFactory(state.underTest),
                new SizeCheckingRecordCursorFactory(state.oracle),
                state.ordered,
                description
        );
    }

    private JoinOperatorFactory pickJoinFactory(Rnd rnd, RecordCursorFactory left, RecordCursorFactory right) {
        ObjList<JoinOperatorFactory> candidates = new ObjList<>();
        boolean leftHasTs = left.getMetadata().getTimestampIndex() != -1;
        boolean rightHasTs = right.getMetadata().getTimestampIndex() != -1;
        for (int i = 0, n = joinFactories.size(); i < n; i++) {
            JoinOperatorFactory factory = joinFactories.getQuick(i);
            // ASOF/LT require all slave value columns to be supported by the value sink.
            if ((factory instanceof AsofJoinOperatorFactory || factory instanceof LtJoinOperatorFactory)
                    && !supportsAsofOrLt(right.getMetadata())) {
                continue;
            }
            JoinScanRequirement requirement = factory.scanRequirement();
            if (requirement.left().requiresTimestamp() && !leftHasTs) {
                continue;
            }
            if (requirement.right().requiresTimestamp() && !rightHasTs) {
                continue;
            }
            candidates.add(factory);
        }
        if (candidates.size() == 0) {
            return null;
        }
        return candidates.getQuick(rnd.nextInt(candidates.size()));
    }

    private static boolean supportsAsofOrLt(io.questdb.cairo.sql.RecordMetadata slaveMetadata) {
        // RecordValueSinkFactory limits supported types; reject schemas that would throw.
        for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
            int type = io.questdb.cairo.ColumnType.tagOf(slaveMetadata.getColumnType(i));
            if (!isRecordValueSinkSupported(type)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isRecordValueSinkSupported(int type) {
        // Keep in sync with RecordValueSinkFactory tag support to avoid UnsupportedOperationException.
        return switch (type) {
            case io.questdb.cairo.ColumnType.INT,
                    io.questdb.cairo.ColumnType.SYMBOL,
                    io.questdb.cairo.ColumnType.IPv4,
                    io.questdb.cairo.ColumnType.GEOINT,
                    io.questdb.cairo.ColumnType.LONG,
                    io.questdb.cairo.ColumnType.GEOLONG,
                    io.questdb.cairo.ColumnType.DATE,
                    io.questdb.cairo.ColumnType.TIMESTAMP,
                    io.questdb.cairo.ColumnType.BYTE,
                    io.questdb.cairo.ColumnType.GEOBYTE,
                    io.questdb.cairo.ColumnType.SHORT,
                    io.questdb.cairo.ColumnType.GEOSHORT,
                    io.questdb.cairo.ColumnType.CHAR,
                    io.questdb.cairo.ColumnType.BOOLEAN,
                    io.questdb.cairo.ColumnType.FLOAT,
                    io.questdb.cairo.ColumnType.DOUBLE,
                    io.questdb.cairo.ColumnType.DECIMAL8,
                    io.questdb.cairo.ColumnType.DECIMAL16,
                    io.questdb.cairo.ColumnType.DECIMAL32,
                    io.questdb.cairo.ColumnType.DECIMAL64,
                    io.questdb.cairo.ColumnType.DECIMAL128,
                    io.questdb.cairo.ColumnType.DECIMAL256 -> true;
            default -> false;
        };
    }

    private static String buildDescription(RecordCursorFactory underTest, RecordCursorFactory oracle) {
        StringBuilder sb = new StringBuilder();
        sb.append("plan=\n");
        sb.append(explain(underTest));
        sb.append("\noraclePlan=\n");
        sb.append(explain(oracle));
        return sb.toString();
    }

    private static String explain(RecordCursorFactory factory) {
        TextPlanSink sink = new TextPlanSink();
        sink.val(factory, factory);
        return sink.getSink().toString();
    }
}
