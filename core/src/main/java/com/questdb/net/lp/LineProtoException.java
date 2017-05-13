package com.questdb.net.lp;

public class LineProtoException extends Exception {
    public static final LineProtoException INSTANCE = new LineProtoException();

    private LineProtoException() {
    }
}
