package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;

/** An empty key list disables dedup; otherwise it includes the designated timestamp. */
public class FuzzDedupOperation implements FuzzTransactionOperation {
    private final IntList keys = new IntList();

    public FuzzDedupOperation(IntList keys) {
        this.keys.addAll(keys);
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI writer, int virtualTimestampIndex, LongList excludedTsIntervals) {
        AlterOperationBuilder builder = new AlterOperationBuilder();
        if (keys.size() == 0) {
            builder.ofDedupDisable(0, writer.getTableToken());
        } else {
            builder.ofDedupEnable(0, writer.getTableToken());
            for (int i = 0; i < keys.size(); i++) {
                builder.setDedupKeyFlag(keys.getQuick(i));
            }
        }
        try (SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE);
             var operation = builder.build()) {
            operation.withContext(context);
            writer.apply(operation, true);
        }
        return true;
    }

    public boolean isEnabled() {
        return keys.size() > 0;
    }
}
