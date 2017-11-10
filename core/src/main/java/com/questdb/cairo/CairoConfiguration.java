package com.questdb.cairo;

import com.questdb.misc.FilesFacade;
import com.questdb.std.clock.Clock;

public interface CairoConfiguration {
    int getFileOperationRetryCount();

    FilesFacade getFilesFacade();

    long getIdleCheckInterval();

    long getInactiveReaderTTL();

    long getInactiveWriterTTL();

    int getMkDirMode();

    int getReaderPoolSegments();

    CharSequence getRoot();

    Clock getClock();
}
