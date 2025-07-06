package io.questdb.griffin.engine.ops;

import io.questdb.cairo.TableToken;

public class CreateTableOperationFuture extends DoneOperationFuture {
    TableToken tableToken;

    public TableToken getTableToken() {
        return tableToken;
    }
}
