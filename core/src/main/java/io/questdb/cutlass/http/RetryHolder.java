package io.questdb.cutlass.http;

public class RetryHolder {
    public static final Retry MARKER = (selector) -> true;

    public Retry retry;
}
