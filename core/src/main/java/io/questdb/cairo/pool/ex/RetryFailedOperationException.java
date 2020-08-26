package io.questdb.cairo.pool.ex;

import io.questdb.cairo.CairoException;

public class RetryFailedOperationException extends CairoException {
    public static final RetryFailedOperationException INSTANCE;

    static {
        INSTANCE = new RetryFailedOperationException();
        INSTANCE.put("resource is busy and retry queue is full");
    }
}
