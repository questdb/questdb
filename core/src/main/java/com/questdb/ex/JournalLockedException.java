package com.questdb.ex;

public class JournalLockedException extends JournalException {
    public static final JournalLockedException INSTANCE = new JournalLockedException();
}
