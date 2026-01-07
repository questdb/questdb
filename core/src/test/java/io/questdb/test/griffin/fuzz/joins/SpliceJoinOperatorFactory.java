package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.SpliceJoinLightRecordCursorFactory;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Rnd;

public final class SpliceJoinOperatorFactory implements JoinOperatorFactory {
    @Override
    public boolean canChain() {
        return false;
    }

    @Override
    public JoinScanRequirement scanRequirement() {
        return new JoinScanRequirement(ScanDirectionRequirement.FORWARD, ScanDirectionRequirement.FORWARD);
    }

    @Override
    public FuzzFactories build(FuzzBuildContext context, JoinInputs inputs, Rnd rnd, String slaveAlias) {
        JoinSpecUtils.JoinKeySelection keys = JoinSpecUtils.selectJoinKeys(inputs.leftUnderTest.getMetadata(), inputs.rightUnderTest.getMetadata(), rnd);
        RecordMetadata masterMetadata = inputs.leftUnderTest.getMetadata();
        RecordMetadata slaveMetadata = inputs.rightUnderTest.getMetadata();
        boolean isSelfJoin = masterMetadata == slaveMetadata;
        JoinKeyPlan keyPlan = context.planJoinKeys(masterMetadata, slaveMetadata, keys.leftKeys, keys.rightKeys, isSelfJoin);
        JoinContext joinContext = new JoinContext();
        JoinRecordMetadata metadata = context.createJoinMetadata(
                masterMetadata,
                slaveMetadata,
                -1,
                slaveAlias
        );
        RecordCursorFactory underTest = new SpliceJoinLightRecordCursorFactory(
                context.getConfiguration(),
                metadata,
                inputs.leftUnderTest,
                inputs.rightUnderTest,
                keyPlan.keyTypes,
                new ArrayColumnTypes()
                        .add(ColumnType.LONG)
                        .add(ColumnType.LONG)
                        .add(ColumnType.LONG)
                        .add(ColumnType.LONG),
                keyPlan.masterKeySink,
                keyPlan.slaveKeySink,
                masterMetadata.getColumnCount(),
                joinContext
        );
        RecordCursorFactory oracle = new ReferenceSpliceJoinRecordCursorFactory(
                inputs.leftOracle,
                inputs.rightOracle,
                metadata,
                keyPlan
        );
        return new FuzzFactories(underTest, oracle);
    }
}
