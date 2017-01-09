package com.questdb.ex;

public class FactoryFullException extends JournalException {
    public static final FactoryFullException INSTANCE = new FactoryFullException();
}
