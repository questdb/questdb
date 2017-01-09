package com.questdb.ex;

public class SystemException extends JournalException {
    public static final SystemException INSTANCE = new SystemException();
}
