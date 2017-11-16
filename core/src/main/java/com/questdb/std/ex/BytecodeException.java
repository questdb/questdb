package com.questdb.std.ex;

import com.questdb.common.JournalRuntimeException;

public class BytecodeException extends JournalRuntimeException {
    public static final BytecodeException INSTANCE = new BytecodeException("Error in bytecode");

    private BytecodeException(String message, Object... args) {
        super(message, args);
    }
}
