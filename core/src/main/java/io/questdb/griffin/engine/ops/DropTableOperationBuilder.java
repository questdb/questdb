package io.questdb.griffin.engine.ops;

import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Mutable;

public class DropTableOperationBuilder implements Mutable {
    private final CharSequenceObjHashMap<CharSequence> flags = new CharSequenceObjHashMap<>();
    private int entityNamePosition;
    private CharSequence sqlText;
    private String tableName;

    public DropTableOperation build() {
        return new DropTableOperation(Chars.toString(sqlText), tableName, entityNamePosition, flags);
    }

    @Override
    public void clear() {
        this.tableName = null;
        this.entityNamePosition = -1;
        this.flags.clear();
    }

    public String getTableName() {
        return tableName;
    }

    public void of(String tableName, int tableNamePosition, boolean ifExists) {
        this.tableName = tableName;
        this.entityNamePosition = tableNamePosition;
        if (ifExists) {
            // value doesn't matter, this is a boolean flag
            flags.put(DropTableOperation.DROP_FLAG_IF_EXISTS, DropTableOperation.IF_EXISTS_VALUE_STUB);
        }
    }

    public void setSqlText(CharSequence sqlText) {
        this.sqlText = sqlText;
    }
}
