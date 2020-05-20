package io.questdb.cairo.security;

import io.questdb.cairo.CairoSecurityContext;

public class CairoSecurityContextImpl implements CairoSecurityContext {
    private final boolean canWrite;
    private final long maxInMemoryRows;

    public CairoSecurityContextImpl(boolean canWrite, long maxInMemoryRows) {
        super();
        this.canWrite = canWrite;
        this.maxInMemoryRows = maxInMemoryRows;
    }

    @Override
    public boolean canWrite() {
        return canWrite;
    }

    @Override
    public long getMaxInMemoryRows() {
        return maxInMemoryRows;
    }
}
