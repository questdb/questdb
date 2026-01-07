package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.NestedLoopFullJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopLeftJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NestedLoopRightJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.NullRecordFactory;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Rnd;

abstract class AbstractNestedLoopJoinOperatorFactory implements JoinOperatorFactory {
    private final int joinKind;

    AbstractNestedLoopJoinOperatorFactory(int joinKind) {
        this.joinKind = joinKind;
    }

    @Override
    public boolean canChain() {
        return true;
    }

    @Override
    public JoinScanRequirement scanRequirement() {
        return new JoinScanRequirement(ScanDirectionRequirement.NONE, ScanDirectionRequirement.NONE);
    }

    @Override
    public FuzzFactories build(FuzzBuildContext context, JoinInputs inputs, Rnd rnd, String slaveAlias) {
        JoinSpecUtils.JoinKeySelection keys = JoinSpecUtils.selectJoinKeys(
                inputs.leftUnderTest.getMetadata(),
                inputs.rightUnderTest.getMetadata(),
                rnd
        );
        if (keys.leftKeys.size() == 0) {
            throw new IllegalArgumentException("nested loop join requires join keys");
        }
        var joinFilter = JoinSpecUtils.createJoinFilter(inputs.leftUnderTest.getMetadata(), keys);
        if (joinFilter == null) {
            throw new IllegalArgumentException("nested loop join requires a join filter");
        }
        RecordMetadata masterMetadata = inputs.leftUnderTest.getMetadata();
        RecordMetadata slaveMetadata = inputs.rightUnderTest.getMetadata();
        JoinRecordMetadata joinMetadata = context.createJoinMetadata(
                masterMetadata,
                slaveMetadata,
                masterMetadata.getTimestampIndex(),
                slaveAlias
        );
        RecordCursorFactory underTest = switch (joinKind) {
            case QueryModel.JOIN_CROSS_LEFT -> new NestedLoopLeftJoinRecordCursorFactory(
                    joinMetadata,
                    inputs.leftUnderTest,
                    inputs.rightUnderTest,
                    masterMetadata.getColumnCount(),
                    joinFilter,
                    NullRecordFactory.getInstance(slaveMetadata)
            );
            case QueryModel.JOIN_CROSS_RIGHT -> new NestedLoopRightJoinRecordCursorFactory(
                    joinMetadata,
                    inputs.leftUnderTest,
                    inputs.rightUnderTest,
                    masterMetadata.getColumnCount(),
                    joinFilter,
                    NullRecordFactory.getInstance(masterMetadata)
            );
            case QueryModel.JOIN_CROSS_FULL -> new NestedLoopFullJoinRecordCursorFactory(
                    context.getConfiguration(),
                    joinMetadata,
                    inputs.leftUnderTest,
                    inputs.rightUnderTest,
                    masterMetadata.getColumnCount(),
                    joinFilter,
                    NullRecordFactory.getInstance(masterMetadata),
                    NullRecordFactory.getInstance(slaveMetadata)
            );
            default -> throw new IllegalArgumentException("unsupported nested loop join kind: " + joinKind);
        };
        RecordCursorFactory oracle = new ReferenceJoinRecordCursorFactory(
                inputs.leftOracle,
                inputs.rightOracle,
                joinMetadata,
                joinKind,
                null,
                joinFilter
        );
        return new FuzzFactories(underTest, oracle);
    }
}
