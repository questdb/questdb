package io.questdb.griffin.engine.ops;

import io.questdb.cairo.TableToken;

public class CreateTableOperationFuture extends DoneOperationFuture {
    private TableToken tableToken;

    public TableToken getTableToken() {
        return tableToken;
    }

    public void setTableToken(TableToken tableToken) {
        this.tableToken = tableToken;
    }
}
