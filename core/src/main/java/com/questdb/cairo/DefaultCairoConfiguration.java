package com.questdb.cairo;

import com.questdb.misc.FilesFacade;
import com.questdb.misc.FilesFacadeImpl;
import com.questdb.std.clock.Clock;
import com.questdb.std.clock.MilliClock;
import com.questdb.std.str.ImmutableCharSequence;

public class DefaultCairoConfiguration implements CairoConfiguration {

    private final CharSequence root;

    public DefaultCairoConfiguration(CharSequence root) {
        this.root = ImmutableCharSequence.of(root);
    }

    @Override
    public int getFileOperationRetryCount() {
        return 30;
    }

    @Override
    public FilesFacade getFilesFacade() {
        return FilesFacadeImpl.INSTANCE;
    }

    @Override
    public long getIdleCheckInterval() {
        return 100;
    }

    @Override
    public long getInactiveReaderTTL() {
        return -10000;
    }

    @Override
    public long getInactiveWriterTTL() {
        return -10000;
    }

    @Override
    public int getMkDirMode() {
        return 509;
    }

    @Override
    public int getReaderPoolSegments() {
        return 2;
    }

    @Override
    public CharSequence getRoot() {
        return root;
    }

    @Override
    public Clock getClock() {
        return MilliClock.INSTANCE;
    }
}
