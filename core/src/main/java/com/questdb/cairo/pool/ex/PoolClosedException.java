package com.questdb.cairo.pool.ex;

import com.questdb.cairo.CairoException;

public class PoolClosedException extends CairoException {
    public static final PoolClosedException INSTANCE = new PoolClosedException();
}
