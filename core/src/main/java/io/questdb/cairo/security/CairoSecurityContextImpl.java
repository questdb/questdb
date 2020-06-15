package io.questdb.cairo.security;

import io.questdb.cairo.CairoSecurityContext;

public class CairoSecurityContextImpl implements CairoSecurityContext {
    private final boolean canWrite;

    public CairoSecurityContextImpl(boolean canWrite) {
        super();
        this.canWrite = canWrite;
    }

    @Override
    public boolean canWrite() {
        return canWrite;
    }
}
