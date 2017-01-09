package com.questdb.ex;

public class RetryLockException extends JournalException {
    public static final RetryLockException INSTANCE = new RetryLockException();
}
