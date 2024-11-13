package io.questdb.griffin.engine.ops;

import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;

public class DropTableOperationBuilder implements Mutable {
    private int entityNamePosition;
    private boolean ifExists;
    private CharSequence sqlText;
    private String tableName;

    public DropTableOperation build() {
        return new DropTableOperation(Chars.toString(sqlText), tableName, entityNamePosition, ifExists);
    }

    @Override
    public void clear() {
        this.tableName = null;
        this.entityNamePosition = -1;
        this.ifExists = false;
    }

    public String getTableName() {
        return tableName;
    }

    public void of(@NotNull String tableName, int tableNamePosition, boolean ifExists) {
        this.tableName = tableName;
        this.entityNamePosition = tableNamePosition;
        this.ifExists = ifExists;
    }

    public void setSqlText(CharSequence sqlText) {
        this.sqlText = sqlText;
    }
}
