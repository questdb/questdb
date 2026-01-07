package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.join.AsOfJoinRecordCursorFactory;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.model.JoinContext;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.Rnd;

public final class AsofJoinOperatorFactory implements JoinOperatorFactory {
    @Override
    public boolean canChain() {
        return true;
    }

    @Override
    public JoinScanRequirement scanRequirement() {
        return new JoinScanRequirement(ScanDirectionRequirement.FORWARD, ScanDirectionRequirement.FORWARD);
    }

    @Override
    public FuzzFactories build(FuzzBuildContext context, JoinInputs inputs, Rnd rnd, String slaveAlias) {
        JoinSpecUtils.JoinKeySelection keys = JoinSpecUtils.selectJoinKeys(inputs.leftUnderTest.getMetadata(), inputs.rightUnderTest.getMetadata(), rnd);
        long tolerance = rnd.nextBoolean() ? Numbers.LONG_NULL : rnd.nextPositiveLong() % 1_000_000L;
        RecordMetadata masterMetadata = inputs.leftUnderTest.getMetadata();
        RecordMetadata slaveMetadata = inputs.rightUnderTest.getMetadata();
        boolean isSelfJoin = masterMetadata == slaveMetadata;
        JoinKeyPlan keyPlan = context.planJoinKeys(masterMetadata, slaveMetadata, keys.leftKeys, keys.rightKeys, isSelfJoin);
        JoinContext joinContext = new JoinContext();
        JoinRecordMetadata metadata = new JoinRecordMetadata(
                context.getConfiguration(),
                masterMetadata.getColumnCount() + slaveMetadata.getColumnCount()
        );
        metadata.copyColumnMetadataFrom(null, masterMetadata);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        ArrayColumnTypes slaveTypes = new ArrayColumnTypes();
        IntList columnIndex = new IntList(slaveMetadata.getColumnCount());
        ListColumnFilter slaveValueColumns = new ListColumnFilter();

        int slaveTimestampIndex = slaveMetadata.getTimestampIndex();
        int slaveValueTimestampIndex = -1;
        for (int i = 0, n = slaveMetadata.getColumnCount(); i < n; i++) {
            if (isKeyColumn(keyPlan, i)) {
                continue;
            }
            metadata.add(slaveAlias, slaveMetadata.getColumnMetadata(i));
            slaveValueColumns.add(i + 1);
            columnIndex.add(i);
            valueTypes.add(slaveMetadata.getColumnType(i));
            slaveTypes.add(slaveMetadata.getColumnType(i));
            if (i == slaveTimestampIndex) {
                slaveValueTimestampIndex = valueTypes.getColumnCount() - 1;
            }
        }

        for (int i = 0, n = keyPlan.slaveKeyColumns.getColumnCount(); i < n; i++) {
            int index = keyPlan.slaveKeyColumns.getColumnIndexFactored(i);
            metadata.add(slaveAlias, slaveMetadata.getColumnMetadata(index));
            slaveTypes.add(slaveMetadata.getColumnType(index));
            columnIndex.add(index);
        }

        if (masterMetadata.getTimestampIndex() != -1) {
            metadata.setTimestampIndex(masterMetadata.getTimestampIndex());
        }

        RecordValueSink slaveValueSink = RecordValueSinkFactory.getInstance(
                context.getAsm(),
                slaveMetadata,
                slaveValueColumns
        );

        RecordCursorFactory underTest = new AsOfJoinRecordCursorFactory(
                context.getConfiguration(),
                metadata,
                inputs.leftUnderTest,
                inputs.rightUnderTest,
                keyPlan.keyTypes,
                valueTypes,
                slaveTypes,
                keyPlan.masterKeySink,
                keyPlan.slaveKeySink,
                masterMetadata.getColumnCount(),
                slaveValueSink,
                columnIndex,
                joinContext,
                keyPlan.masterKeyColumns,
                tolerance,
                slaveValueTimestampIndex
        );

        RecordCursorFactory oracle = new ReferenceAsofJoinRecordCursorFactory(
                inputs.leftOracle,
                inputs.rightOracle,
                metadata,
                keyPlan,
                tolerance,
                false
        );
        return new FuzzFactories(underTest, oracle);
    }

    private static boolean isKeyColumn(JoinKeyPlan keyPlan, int slaveIndex) {
        for (int i = 0, n = keyPlan.slaveKeyColumns.getColumnCount(); i < n; i++) {
            if (keyPlan.slaveKeyColumns.getColumnIndexFactored(i) == slaveIndex) {
                return true;
            }
        }
        return false;
    }
}
