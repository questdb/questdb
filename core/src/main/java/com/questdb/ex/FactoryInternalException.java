package com.questdb.ex;

public class FactoryInternalException extends JournalException {
    public static final FactoryInternalException INSTANCE = new FactoryInternalException();
}
