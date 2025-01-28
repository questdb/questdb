package io.questdb.griffin.engine.ops;

import io.questdb.std.Chars;
import io.questdb.std.Mutable;
import org.jetbrains.annotations.NotNull;

public class DropMatViewOperationBuilder implements Mutable {
    private boolean ifExists;
    private String matViewName;
    private int matViewNamePosition;
    private CharSequence sqlText;

    public DropMatViewOperation build() {
        return new DropMatViewOperation(Chars.toString(sqlText), matViewName, matViewNamePosition, ifExists);
    }

    @Override
    public void clear() {
        this.matViewName = null;
        this.matViewNamePosition = -1;
        this.ifExists = false;
    }

    public String getMatViewName() {
        return matViewName;
    }

    public void of(@NotNull String matViewName, int matViewNamePosition, boolean ifExists) {
        this.matViewName = matViewName;
        this.matViewNamePosition = matViewNamePosition;
        this.ifExists = ifExists;
    }

    public void setSqlText(CharSequence sqlText) {
        this.sqlText = sqlText;
    }
}
