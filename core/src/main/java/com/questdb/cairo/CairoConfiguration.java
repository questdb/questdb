package com.questdb.cairo;

import com.questdb.std.FilesFacade;
import com.questdb.std.microtime.MicrosecondClock;

public interface CairoConfiguration {
    int getFileOperationRetryCount();

    FilesFacade getFilesFacade();

    long getIdleCheckInterval();

    long getInactiveReaderTTL();

    long getInactiveWriterTTL();

    int getMkDirMode();

    int getReaderPoolSegments();

    CharSequence getRoot();

    MicrosecondClock getClock();
}
