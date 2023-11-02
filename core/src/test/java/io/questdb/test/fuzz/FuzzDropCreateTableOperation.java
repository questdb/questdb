package io.questdb.test.fuzz;

import io.questdb.cairo.*;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;

public class FuzzDropCreateTableOperation implements FuzzTransactionOperation {
    static final Log LOG = LogFactory.getLog(FuzzDropCreateTableOperation.class);

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI tableWriter, int virtualTimestampIndex) {
        TableToken tableToken = tableWriter.getTableToken();
        String tableName = tableToken.getTableName();
        boolean isWal = engine.isWalTable(tableToken);
        int timestampIndex = tableWriter.getMetadata().getTimestampIndex();
        RecordMetadata copyDenseMeta = denseMetaCopy(tableWriter.getMetadata(), timestampIndex);

        try (MemoryMARW vm = Vm.getMARWInstance(); Path path = new Path()) {
            engine.releaseInactive();
            while (true) {
                try {
                    LOG.info().$("dropping table ").$(tableToken.getDirName()).$();
                    engine.drop(path, tableToken);
                    break;
                } catch (CairoException ignore) {
                    if (!isWal) {
                        engine.releaseInactive();
                        Os.sleep(50);
                    }
                }
            }

            engine.createTable(
                    AllowAllSecurityContext.INSTANCE,
                    vm,
                    path,
                    false,
                    new TableStructMetadataAdapter(
                            tableName,
                            isWal,
                            copyDenseMeta,
                            engine.getConfiguration(),
                            PartitionBy.DAY,
                            copyDenseMeta.getTimestampIndex()
                    ),
                    false
            );
        }
        return true;
    }

    private RecordMetadata denseMetaCopy(TableRecordMetadata metadata, int timestampIndex) {
        GenericRecordMetadata newMeta = new GenericRecordMetadata();
        int tsIndex = -1;
        for (int i = 0; i < metadata.getColumnCount(); i++) {
            int type = metadata.getColumnType(i);
            if (type > 0) {
                CharSequence name = metadata.getColumnName(i);
                if (!ColumnType.isSymbol(type)) {
                    newMeta.add(new TableColumnMetadata(Chars.toString(name), type));
                } else {
                    newMeta.add(new TableColumnMetadata(Chars.toString(name), type, true, 4096, true, null));
                }
                if (i == timestampIndex) {
                    tsIndex = newMeta.getColumnCount() - 1;
                }
            }
        }
        newMeta.setTimestampIndex(tsIndex);
        return newMeta;
    }
}
