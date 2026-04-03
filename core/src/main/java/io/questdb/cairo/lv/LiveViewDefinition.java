package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.std.Chars;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

public class LiveViewDefinition {
    public static final String LIVE_VIEW_DEFINITION_FILE_NAME = "_lv";
    public static final int LIVE_VIEW_DEFINITION_FORMAT_MSG_TYPE = 0;

    private final String baseTableName;
    private final TableToken baseTableToken;
    private final long lagMicros;
    private final GenericRecordMetadata metadata;
    private final long retentionMicros;
    private final String viewName;
    private final String viewSql;

    public LiveViewDefinition(
            String viewName,
            String viewSql,
            String baseTableName,
            TableToken baseTableToken,
            long lagMicros,
            long retentionMicros,
            GenericRecordMetadata metadata
    ) {
        this.viewName = viewName;
        this.viewSql = viewSql;
        this.baseTableName = baseTableName;
        this.baseTableToken = baseTableToken;
        this.lagMicros = lagMicros;
        this.retentionMicros = retentionMicros;
        this.metadata = metadata;
    }

    public static void append(@NotNull LiveViewDefinition definition, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
        block.putStr(definition.viewSql);
        block.putStr(definition.baseTableName);
        block.putLong(definition.lagMicros);
        block.putLong(definition.retentionMicros);
        block.commit(LIVE_VIEW_DEFINITION_FORMAT_MSG_TYPE);
        writer.commit();
    }

    public static LiveViewDefinition readFrom(
            @NotNull BlockFileReader reader,
            @NotNull Path path,
            int rootLen,
            @NotNull TableToken liveViewToken,
            @NotNull TableToken baseTableToken,
            @NotNull GenericRecordMetadata metadata
    ) {
        path.trimTo(rootLen).concat(liveViewToken.getDirName()).concat(LIVE_VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == LIVE_VIEW_DEFINITION_FORMAT_MSG_TYPE) {
                long offset = 0;
                CharSequence viewSqlCs = block.getStr(offset);
                offset += Vm.getStorageLength(viewSqlCs);
                String viewSql = Chars.toString(viewSqlCs);

                CharSequence baseTableNameCs = block.getStr(offset);
                offset += Vm.getStorageLength(baseTableNameCs);
                String baseTableName = Chars.toString(baseTableNameCs);

                long lagMicros = block.getLong(offset);
                offset += Long.BYTES;
                long retentionMicros = block.getLong(offset);

                return new LiveViewDefinition(
                        liveViewToken.getTableName(),
                        viewSql,
                        baseTableName,
                        baseTableToken,
                        lagMicros,
                        retentionMicros,
                        metadata
                );
            }
        }
        throw CairoException.critical(0)
                .put("cannot read live view definition, block not found [path=").put(path).put(']');
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public TableToken getBaseTableToken() {
        return baseTableToken;
    }

    public long getLagMicros() {
        return lagMicros;
    }

    public GenericRecordMetadata getMetadata() {
        return metadata;
    }

    public long getRetentionMicros() {
        return retentionMicros;
    }

    public String getViewName() {
        return viewName;
    }

    public String getViewSql() {
        return viewSql;
    }
}
