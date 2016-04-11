/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.test.tools;

import com.nfsdb.*;
import com.nfsdb.ex.JournalConfigurationException;
import com.nfsdb.ex.JournalException;
import com.nfsdb.factory.JournalClosingListener;
import com.nfsdb.factory.JournalFactory;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.MetadataBuilder;
import com.nfsdb.misc.Files;
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
    public <T> Journal<T> reader(JournalKey<T> key) throws JournalException {
        Journal<T> result = super.reader(key);
        journals.add(result);
        result.setCloseListener(this);
        return result;
    }

    @Override
    public <T> JournalBulkWriter<T> bulkWriter(JournalKey<T> key) throws JournalException {
        JournalBulkWriter<T> writer = super.bulkWriter(key);
        journals.add(writer);
        writer.setCloseListener(this);
        return writer;
    }

    @Override
    public <T> JournalWriter<T> bulkWriter(JournalMetadata<T> metadata) throws JournalException {
        JournalWriter<T> writer = super.bulkWriter(metadata);
        journals.add(writer);
        writer.setCloseListener(this);
        return writer;
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
}