package io.questdb.griffin.engine.ops;

import io.questdb.std.Chars;
import io.questdb.std.Mutable;

public class GenericDropOperationBuilder implements Mutable {
    private CharSequence entityName;
    private int entityNamePosition;
    private boolean ifExists;
    private int operationCode;

    public GenericDropOperation build() {
        return new GenericDropOperation(operationCode, Chars.toString(entityName), entityNamePosition, ifExists);
    }

    @Override
    public void clear() {
        this.operationCode = 0;
        this.entityName = null;
        this.entityNamePosition = 0;
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
}
