package io.questdb.cutlass.http;

public class RetryHolder {
    public static final Retry MARKER = () -> true;

    public Retry retry;
}
