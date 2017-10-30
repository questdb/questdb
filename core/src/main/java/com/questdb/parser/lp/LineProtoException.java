package com.questdb.parser.lp;

public class LineProtoException extends RuntimeException {
    public static final LineProtoException INSTANCE = new LineProtoException();

    private LineProtoException() {
    }
}
