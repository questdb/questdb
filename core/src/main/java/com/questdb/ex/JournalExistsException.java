package com.questdb.ex;

public class JournalExistsException extends JournalException {
    public static final JournalExistsException INSTANCE = new JournalExistsException();
}
