package io.questdb.cairo.security;

import io.questdb.cairo.CairoSecurityContext;

public class ReadOnlyCairoSecurityContext implements CairoSecurityContext {
    public static final ReadOnlyCairoSecurityContext INSTANCE = new ReadOnlyCairoSecurityContext();

    @Override
    public boolean canWrite() {
        return false;
    }
}
