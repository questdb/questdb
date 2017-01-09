package com.questdb.ex;

public class WriterBusyException extends JournalException {
    public static final WriterBusyException INSTANCE = new WriterBusyException();
}
