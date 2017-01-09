package com.questdb.ex;

public class FactoryClosedException extends JournalException {
    public static final FactoryClosedException INSTANCE = new FactoryClosedException();
}
