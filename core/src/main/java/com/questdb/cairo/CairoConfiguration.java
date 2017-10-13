package com.questdb.cairo;

public interface CairoConfiguration {
    int getFileOperationRetryCount();

    FilesFacade getFilesFacade();

    long getIdleCheckInterval();

    long getInactiveReaderTTL();

    long getInactiveWriterTTL();

    int getMkDirMode();

    int getReaderPoolSegments();

    CharSequence getRoot();
}
