package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.OperationFuture;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.mp.SCSequence;
import io.questdb.std.CharSequenceObjHashMap;
import org.jetbrains.annotations.Nullable;

public class DropOperation implements Operation {
    public static final int DROP_ALL_TABLES = 2;
    public static final int DROP_ENTITY_UNDEFINED = -1;
    public static final String DROP_FLAG_IF_EXISTS = "if_exists";
    public static final int DROP_SINGLE_TABLE = 1;
    public static final CharSequence IF_EXISTS_VALUE_STUB = "";
    private final CairoEngine cairoEngine;
    private final int cmd;
    private final String entityName;
    private final int entityNamePosition;
    private final CharSequenceObjHashMap<CharSequence> flags = new CharSequenceObjHashMap<>();
    private final DropOperationHandler dropOperationHandler;
    private final String sqlText;
    private final long beginNanos;
    private final DoneOperationFuture operationFuture = new DoneOperationFuture();

    public DropOperation(
            CairoEngine engine,
            int cmd,
            String entityName,
            int entityNamePosition,
            CharSequenceObjHashMap<CharSequence> flags,
            @Nullable DropOperationHandler dropOperationHandler,
            String sqlText,
            long beginNanos
    ) {
        this.cairoEngine = engine;
        this.cmd = cmd;
        this.entityName = entityName;
        this.entityNamePosition = entityNamePosition;
        this.flags.putAll(flags);
        this.dropOperationHandler = dropOperationHandler;
        this.sqlText = sqlText;
        this.beginNanos = beginNanos;
    }

    @Override
    public void close() {
        // nothing to fee
    }

    public OperationFuture execute(SqlExecutionContext sqlExecutionContext, @Nullable SCSequence eventSubSeq) throws SqlException {
        try {
            switch (cmd) {
                case DROP_ALL_TABLES:
                    try (SqlCompiler compiler = cairoEngine.getSqlCompiler()) {
                        compiler.dropAllTables(sqlExecutionContext);
                    }
                    break;
                case DROP_SINGLE_TABLE:
                    try (SqlCompiler compiler = cairoEngine.getSqlCompiler()) {
                        compiler.dropTable(sqlExecutionContext, entityName, entityNamePosition, flags);
                    }
                    break;
                default:
                    assert dropOperationHandler != null;
                    dropOperationHandler.execute(sqlExecutionContext, entityName, entityNamePosition, flags);
                    break;
            }
            QueryProgress.logEnd(0, sqlText, sqlExecutionContext, beginNanos, false);
            return operationFuture;
        } catch (Throwable e) {
            QueryProgress.logError(e, 0, sqlText, sqlExecutionContext, beginNanos, false);
            throw e;
        }
    }

    @FunctionalInterface
    public interface DropOperationHandler {
        void execute(
                SqlExecutionContext sqlExecutionContext,
                String entityName,
                int entityPosition,
                CharSequenceObjHashMap<CharSequence> tags
        ) throws SqlException;
    }
}
