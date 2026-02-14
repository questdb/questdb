package io.questdb.griffin.engine.ops;

import io.questdb.std.Chars;
import io.questdb.std.Mutable;

public class GenericDropOperationBuilder implements Mutable {
    protected CharSequence entityName;
    protected int entityNamePosition;
    protected boolean ifExists;
    protected int operationCode;
    protected CharSequence sqlText;

    public GenericDropOperation build() {
        return new GenericDropOperation(
                operationCode,
                Chars.toString(sqlText),
                Chars.toString(entityName),
                entityNamePosition,
                ifExists
        );
    }

    @Override
    public void clear() {
        this.operationCode = 0;
        this.sqlText = null;
        this.entityName = null;
        this.entityNamePosition = 0;
        this.ifExists = false;
    }

    public void setEntityName(CharSequence entityName) {
        this.entityName = entityName;
    }

    public void setEntityNamePosition(int entityNamePosition) {
        this.entityNamePosition = entityNamePosition;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public void setOperationCode(int operationCode) {
        this.operationCode = operationCode;
    }

    public void setSqlText(CharSequence sqlText) {
        this.sqlText = sqlText;
    }
}
