package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;

public final class JoinKeyPlan {
    public final ListColumnFilter masterKeyColumns;
    public final ListColumnFilter slaveKeyColumns;
    public final ArrayColumnTypes keyTypes;
    public final RecordSink masterKeySink;
    public final RecordSink slaveKeySink;

    public JoinKeyPlan(
            ListColumnFilter masterKeyColumns,
            ListColumnFilter slaveKeyColumns,
            ArrayColumnTypes keyTypes,
            RecordSink masterKeySink,
            RecordSink slaveKeySink
    ) {
        this.masterKeyColumns = masterKeyColumns;
        this.slaveKeyColumns = slaveKeyColumns;
        this.keyTypes = keyTypes;
        this.masterKeySink = masterKeySink;
        this.slaveKeySink = slaveKeySink;
    }
}
