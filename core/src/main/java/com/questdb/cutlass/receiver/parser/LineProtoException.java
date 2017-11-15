package com.questdb.cutlass.receiver.parser;

public class LineProtoException extends RuntimeException {
    public static final LineProtoException INSTANCE = new LineProtoException();

    private LineProtoException() {
    }
}
