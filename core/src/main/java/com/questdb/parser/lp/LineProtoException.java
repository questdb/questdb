package com.questdb.parser.lp;

public class LineProtoException extends Exception {
    public static final LineProtoException INSTANCE = new LineProtoException();

    private LineProtoException() {
    }
}
