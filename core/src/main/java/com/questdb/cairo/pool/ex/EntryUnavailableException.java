package com.questdb.cairo.pool.ex;

import com.questdb.cairo.CairoException;

public class EntryUnavailableException extends CairoException {
    public static final EntryUnavailableException INSTANCE = new EntryUnavailableException();
}
