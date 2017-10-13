package com.questdb.cairo;

public interface CairoConfiguration {
    FilesFacade getFilesFacade();

    long getIdleCheckInterval();

    long getInactiveReaderTTL();

    long getInactiveWriterTTL();

    int getReaderPoolSegments();

    CharSequence getRoot();
}
