package io.questdb.griffin.engine.ops;

import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;

public class DropTableOperationBuilder implements Mutable {
    private boolean ifExists;
    private CharSequence sqlText;
    private String tableName;
    private int tableNamePosition;

    public DropTableOperation build() {
        return new DropTableOperation(Chars.toString(sqlText), tableName, tableNamePosition, ifExists);
    }

    @Override
    public void clear() {
        this.tableName = null;
        this.tableNamePosition = -1;
        this.ifExists = false;
    }

    public String getTableName() {
        return tableName;
    }

    public void of(@NotNull String tableName, int tableNamePosition, boolean ifExists) {
        this.tableName = tableName;
        this.tableNamePosition = tableNamePosition;
        this.ifExists = ifExists;
    }

    public void setSqlText(CharSequence sqlText) {
        this.sqlText = sqlText;
    }
}
