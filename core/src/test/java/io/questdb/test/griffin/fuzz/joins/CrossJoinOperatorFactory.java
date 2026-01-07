package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.join.CrossJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Rnd;

public final class CrossJoinOperatorFactory implements JoinOperatorFactory {
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
        RecordMetadata masterMetadata = inputs.leftUnderTest.getMetadata();
        RecordMetadata slaveMetadata = inputs.rightUnderTest.getMetadata();
        JoinRecordMetadata joinMetadata = context.createJoinMetadata(
                masterMetadata,
                slaveMetadata,
                masterMetadata.getTimestampIndex(),
                slaveAlias
        );
        RecordCursorFactory underTest = new CrossJoinRecordCursorFactory(
                joinMetadata,
                inputs.leftUnderTest,
                inputs.rightUnderTest,
                masterMetadata.getColumnCount()
        );
        RecordCursorFactory oracle = new ReferenceJoinRecordCursorFactory(
                inputs.leftOracle,
                inputs.rightOracle,
                joinMetadata,
                QueryModel.JOIN_CROSS,
                null,
                null
        );
        return new FuzzFactories(underTest, oracle);
    }
}
