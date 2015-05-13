/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.test.tools;

import com.nfsdb.*;
import com.nfsdb.exceptions.JournalConfigurationException;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalClosingListener;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.MetadataBuilder;
import com.nfsdb.utils.Files;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.List;

public class JournalTestFactory extends JournalFactory implements TestRule, JournalClosingListener {

    private final List<Journal> journals = new ArrayList<>();

    public JournalTestFactory(JournalConfiguration configuration) throws JournalConfigurationException {
        super(configuration);
    }

    @Override
    public Statement apply(final Statement base, final Description desc) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Throwable throwable = null;
                try {
                    Files.deleteOrException(getConfiguration().getJournalBase());
                    Files.mkDirsOrException(getConfiguration().getJournalBase());
                    base.evaluate();
                } catch (Throwable e) {
                    throwable = e;
                } finally {
                    for (Journal journal : journals) {
                        if (journal != null && journal.isOpen()) {
                            journal.setCloseListener(null);
                            journal.close();
                        }
                    }
                    journals.clear();
                    Files.deleteOrException(getConfiguration().getJournalBase());
                }

                if (throwable != null) {
                    throw throwable;
                }
            }
        };
    }

    @Override
    public <T> JournalBulkReader<T> bulkReader(JournalKey<T> key) throws JournalException {
        JournalBulkReader<T> reader = super.bulkReader(key);
        journals.add(reader);
        reader.setCloseListener(this);
        return reader;
    }

    @Override
    public <T> JournalBulkWriter<T> bulkWriter(JournalKey<T> key) throws JournalException {
        JournalBulkWriter<T> writer = super.bulkWriter(key);
        journals.add(writer);
        writer.setCloseListener(this);
        return writer;
    }

    @Override
    public boolean closing(Journal journal) {
        journals.remove(journal);
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Journal reader(JournalMetadata metadata) throws JournalException {
        Journal reader = new Journal(metadata, metadata.getKey());
        journals.add(reader);
        reader.setCloseListener(this);
        return reader;
    }

    @Override
    public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        Journal<T> result = super.reader(key);
        journals.add(result);
        result.setCloseListener(this);
        return result;
    }

    @Override
    public <T> JournalWriter<T> writer(JournalKey<T> key) throws JournalException {
        JournalWriter<T> writer = super.writer(key);
        journals.add(writer);
        writer.setCloseListener(this);
        return writer;
    }

    public <T> JournalWriter<T> writer(MetadataBuilder<T> b) throws JournalException {
        JournalWriter<T> writer = super.writer(b);
        journals.add(writer);
        writer.setCloseListener(this);
        return writer;
    }
}