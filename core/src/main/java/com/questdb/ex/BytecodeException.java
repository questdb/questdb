package com.questdb.ex;

public class BytecodeException extends JournalRuntimeException {
    public static final BytecodeException INSTANCE = new BytecodeException("Error in bytecode");

    private BytecodeException(String message, Object... args) {
        super(message, args);
    }
}
