package io.questdb.griffin.engine.ops;

import io.questdb.cairo.CairoEngine;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;

public class DropOperationBuilder implements Mutable {
    private final CairoEngine cairoEngine;
    private final CharSequenceObjHashMap<CharSequence> flags = new CharSequenceObjHashMap<>();
    private long beginNanos;
    private int cmd = DropOperation.DROP_ENTITY_UNDEFINED;
    private DropOperation.DropOperationHandler dropOperationHandler;
    private String entityName;
    private int entityNamePosition;
    private CharSequence sqlText;

    public DropOperationBuilder(CairoEngine cairoEngine) {
        this.cairoEngine = cairoEngine;
    }

    public void addFlag(String flagName, CharSequence flagValue) {
        flags.put(flagName, flagValue);
    }

    public DropOperation build() {
        return new DropOperation(
                cairoEngine,
                cmd,
                entityName,
                entityNamePosition,
                flags,
                dropOperationHandler,
                Chars.toString(sqlText),
                beginNanos
        );
    }

    @Override
    public void clear() {
        this.entityName = null;
        this.entityNamePosition = -1;
        this.cmd = DropOperation.DROP_ENTITY_UNDEFINED;
        this.flags.clear();
        this.dropOperationHandler = null;
    }

    public int getCmd() {
        return cmd;
    }

    public String getEntityName() {
        return entityName;
    }

    public void ofDropAllTables() {
        this.cmd = DropOperation.DROP_ALL_TABLES;
    }

    public void ofDropTable(String tableName, int tableNamePosition, boolean ifExists) {
        this.cmd = DropOperation.DROP_SINGLE_TABLE;
        this.entityName = tableName;
        this.entityNamePosition = tableNamePosition;
        if (ifExists) {
            // value doesn't matter, this is a boolean flag
            flags.put(DropOperation.DROP_FLAG_IF_EXISTS, DropOperation.IF_EXISTS_VALUE_STUB);
        }
    }

    public void setBeginNanos(long beginNanos) {
        this.beginNanos = beginNanos;
    }

    public void setCmd(int cmd) {
        this.cmd = cmd;
    }

    public void setDropOperationHandler(DropOperation.DropOperationHandler dropOperationHandler) {
        this.dropOperationHandler = dropOperationHandler;
    }

    public void setEntity(String entityName, int entityNamePosition) {
        this.entityName = entityName;
        this.entityNamePosition = entityNamePosition;
    }

    public void setSqlText(CharSequence sqlText) {
        this.sqlText = sqlText;
    }
}
