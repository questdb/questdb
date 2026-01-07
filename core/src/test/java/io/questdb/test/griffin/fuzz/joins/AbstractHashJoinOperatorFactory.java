package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.join.HashJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinLightRecordCursorFactory;
import io.questdb.griffin.engine.join.HashOuterJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.Rnd;

abstract class AbstractHashJoinOperatorFactory implements JoinOperatorFactory {
    private final int joinKind;

    AbstractHashJoinOperatorFactory(int joinKind) {
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
        boolean fullFat = rnd.nextInt(100) < 20;
        RecordMetadata masterMetadata = inputs.leftUnderTest.getMetadata();
        RecordMetadata slaveMetadata = inputs.rightUnderTest.getMetadata();
        boolean isSelfJoin = masterMetadata == slaveMetadata;
        JoinKeyPlan keyPlan = context.planJoinKeys(masterMetadata, slaveMetadata, keys.leftKeys, keys.rightKeys, isSelfJoin);
        JoinContext joinContext = new JoinContext();
        JoinRecordMetadata joinMetadata = context.createJoinMetadata(
                masterMetadata,
                slaveMetadata,
                joinKind == QueryModel.JOIN_RIGHT_OUTER || joinKind == QueryModel.JOIN_FULL_OUTER
                        ? -1
                        : masterMetadata.getTimestampIndex(),
                slaveAlias
        );

        RecordCursorFactory underTest;
        if (inputs.rightUnderTest.recordCursorSupportsRandomAccess() && !fullFat) {
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.INT);
            if (joinKind == QueryModel.JOIN_INNER) {
                valueTypes.add(ColumnType.INT);
                underTest = new HashJoinLightRecordCursorFactory(
                        context.getConfiguration(),
                        joinMetadata,
                        inputs.leftUnderTest,
                        inputs.rightUnderTest,
                        keyPlan.keyTypes,
                        valueTypes,
                        keyPlan.masterKeySink,
                        keyPlan.slaveKeySink,
                        masterMetadata.getColumnCount(),
                        joinContext
                );
            } else {
                if (joinKind == QueryModel.JOIN_RIGHT_OUTER || joinKind == QueryModel.JOIN_FULL_OUTER) {
                    valueTypes.add(ColumnType.BOOLEAN);
                }
                underTest = new HashOuterJoinLightRecordCursorFactory(
                        context.getConfiguration(),
                        joinMetadata,
                        inputs.leftUnderTest,
                        inputs.rightUnderTest,
                        keyPlan.keyTypes,
                        valueTypes,
                        keyPlan.masterKeySink,
                        keyPlan.slaveKeySink,
                        masterMetadata.getColumnCount(),
                        joinContext,
                        joinKind
                );
            }
        } else {
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG);
            valueTypes.add(ColumnType.LONG);
            valueTypes.add(ColumnType.LONG);
            if (joinKind == QueryModel.JOIN_RIGHT_OUTER || joinKind == QueryModel.JOIN_FULL_OUTER) {
                valueTypes.add(ColumnType.BOOLEAN);
            }
            EntityColumnFilter columnFilter = new EntityColumnFilter();
            columnFilter.of(slaveMetadata.getColumnCount());
            RecordSink slaveSink = RecordSinkFactory.getInstance(
                    context.getAsm(),
                    slaveMetadata,
                    columnFilter,
                    context.getConfiguration()
            );
            if (joinKind == QueryModel.JOIN_INNER) {
                underTest = new HashJoinRecordCursorFactory(
                        context.getConfiguration(),
                        joinMetadata,
                        inputs.leftUnderTest,
                        inputs.rightUnderTest,
                        keyPlan.keyTypes,
                        valueTypes,
                        keyPlan.masterKeySink,
                        keyPlan.slaveKeySink,
                        slaveSink,
                        masterMetadata.getColumnCount(),
                        joinContext
                );
            } else {
                underTest = new HashOuterJoinRecordCursorFactory(
                        context.getConfiguration(),
                        joinMetadata,
                        inputs.leftUnderTest,
                        inputs.rightUnderTest,
                        keyPlan.keyTypes,
                        valueTypes,
                        keyPlan.masterKeySink,
                        keyPlan.slaveKeySink,
                        slaveSink,
                        masterMetadata.getColumnCount(),
                        joinContext,
                        joinKind
                );
            }
        }

        RecordCursorFactory oracle = new ReferenceJoinRecordCursorFactory(
                inputs.leftOracle,
                inputs.rightOracle,
                joinMetadata,
                joinKind,
                keyPlan,
                null
        );
        return new FuzzFactories(underTest, oracle);
    }
}
