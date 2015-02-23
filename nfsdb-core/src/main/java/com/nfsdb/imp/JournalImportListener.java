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

package com.nfsdb.imp;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.logging.Logger;
import com.nfsdb.utils.Numbers;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class JournalImportListener implements InputAnalysisListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(JournalImportListener.class);
    private final JournalWriterFactory factory;
    private final String location;
    private JournalWriter writer;
    private ColumnMetadata metadata[];

    public JournalImportListener(JournalWriterFactory factory, String location) {
        this.factory = factory;
        this.location = location;
        switch (factory.exists(location)) {
            case EXISTS_FOREIGN:
                throw new JournalRuntimeException("A foreign file/directory already exists: " + (new File(factory.getConfiguration().getJournalBase(), location)));
            case EXISTS:
                try {
                    writer = factory.writer(location);
                } catch (JournalException e) {
                    throw new JournalRuntimeException(e);
                }
        }
    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void onError(int line) {

    }

    @Override
    public void onField(int line, CharSequence[] values, int hi) {
        try {
            JournalEntryWriter w = writer.entryWriter();
            for (int i = 0; i < hi; i++) {
                if (values[i].length() == 0) {
                    continue;
                }
                try {
                    switch (metadata[i].type) {
                        case STRING:
                            w.putStr(i, values[i]);
                            break;
                        case DOUBLE:
                            w.putDouble(i, Numbers.parseDouble(values[i]));
                            break;
                        case INT:
                            w.putInt(i, Numbers.parseInt(values[i]));
                            break;
                        case FLOAT:
                            w.putFloat(i, Numbers.parseFloat(values[i]));
                            break;
                    }
                } catch (Exception e) {
                    LOGGER.info("Error at (%d,%d) as %s: %s", line, i, metadata[i].type, e.getMessage());
                    break;
                }
            }
            w.append();
        } catch (JournalException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onFieldCount(int count) {

    }

    @Override
    public void onHeader(CharSequence[] values, int hi) {

    }

    @Override
    public void onLineCount(int count) {

    }

    @Override
    public void onMetadata(ColumnMetadata metadata[]) {
        if (writer == null) {
            try {
                writer = factory.writer(new JournalStructure(location, this.metadata = metadata));
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }
}
