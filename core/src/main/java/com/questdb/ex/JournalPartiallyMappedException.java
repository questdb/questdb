package com.questdb.ex;

public class JournalPartiallyMappedException extends JournalException {
    public static final JournalPartiallyMappedException INSTANCE = new JournalPartiallyMappedException();

    public JournalPartiallyMappedException() {
        super("Metadata is unusable for writer. Partially mapped?");
    }
}
