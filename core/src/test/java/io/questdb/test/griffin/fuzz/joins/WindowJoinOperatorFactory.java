package io.questdb.test.griffin.fuzz.joins;

import io.questdb.test.griffin.fuzz.*;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.engine.join.WindowJoinRecordCursorFactory;
import io.questdb.griffin.model.QueryModel;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.IntList;

public final class WindowJoinOperatorFactory implements JoinOperatorFactory {
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
        JoinSpecUtils.JoinKeySelection keys = new JoinSpecUtils.JoinKeySelection(new IntList(), new IntList());
        ObjList<io.questdb.griffin.engine.functions.GroupByFunction> groupByFunctions = new ObjList<>();
        groupByFunctions.add(new CountLongConstGroupByFunction());
        long lo = rnd.nextBoolean() ? 0 : rnd.nextPositiveLong() % 1_000L;
        long hi = rnd.nextBoolean() ? 0 : rnd.nextPositiveLong() % 1_000L;
        WindowSpec window = new WindowSpec(lo, hi, false, groupByFunctions);
        ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        for (int i = 0, n = window.groupByFunctions.size(); i < n; i++) {
            window.groupByFunctions.getQuick(i).initValueTypes(valueTypes);
        }

        RecordMetadata masterMetadata = inputs.leftUnderTest.getMetadata();
        RecordMetadata slaveMetadata = inputs.rightUnderTest.getMetadata();
        JoinRecordMetadata joinMetadata = context.createJoinMetadata(
                masterMetadata,
                slaveMetadata,
                masterMetadata.getTimestampIndex(),
                slaveAlias
        );

        GenericRecordMetadata outputMetadata = GenericRecordMetadata.copyOf(masterMetadata);
        for (int i = 0, n = window.groupByFunctions.size(); i < n; i++) {
            String name = "agg" + i;
            outputMetadata.add(new TableColumnMetadata(name, window.groupByFunctions.getQuick(i).getType()));
        }

        RecordCursorFactory underTest = new WindowJoinRecordCursorFactory(
                context.getAsm(),
                context.getConfiguration(),
                outputMetadata,
                joinMetadata,
                inputs.leftUnderTest,
                inputs.rightUnderTest,
                window.includePrevailing,
                null,
                window.lo,
                window.hi,
                window.groupByFunctions,
                valueTypes,
                null
        );

        RecordCursorFactory oracle = new ReferenceWindowJoinRecordCursorFactory(
                inputs.leftOracle,
                inputs.rightOracle,
                outputMetadata,
                window,
                keys.leftKeys,
                keys.rightKeys
        );
        return new FuzzFactories(underTest, oracle);
    }
}
