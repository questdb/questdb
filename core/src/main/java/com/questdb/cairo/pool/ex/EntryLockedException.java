package com.questdb.cairo.pool.ex;

import com.questdb.cairo.CairoException;

public class EntryLockedException extends CairoException {
    public static final EntryLockedException INSTANCE = new EntryLockedException();
}
