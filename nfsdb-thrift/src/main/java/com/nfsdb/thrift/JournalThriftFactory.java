package com.nfsdb.thrift;

import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.factory.JournalConfiguration;
import com.nfsdb.journal.factory.JournalFactory;

import java.io.File;

public class JournalThriftFactory extends JournalFactory {

    public JournalThriftFactory(String journalBase) throws JournalConfigurationException {
        super(new JournalConfiguration(new File(journalBase)).setNullsAdaptorFactory(new ThriftNullsAdaptorFactory()).build());
    }
}
