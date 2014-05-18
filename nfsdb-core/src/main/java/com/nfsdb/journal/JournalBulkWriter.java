package com.nfsdb.journal;

import com.nfsdb.journal.concurrent.TimerCache;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalMetadata;

public class JournalBulkWriter<T> extends JournalWriter<T> {
    public JournalBulkWriter(JournalMetadata<T> metadata, JournalKey<T> key, TimerCache timerCache) throws JournalException {
        super(metadata, key, timerCache);
    }

    @Override
    public JournalMode getMode() {
        return JournalMode.APPEND_ONLY;
    }
}
