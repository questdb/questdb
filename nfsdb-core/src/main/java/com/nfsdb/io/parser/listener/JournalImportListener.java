/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.io.parser.listener;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.ImportedColumnMetadata;
import com.nfsdb.logging.Logger;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Dates;
import com.nfsdb.utils.Numbers;

import java.io.Closeable;

public class JournalImportListener implements InputAnalysisListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(JournalImportListener.class);
    private final JournalWriterFactory factory;
    private final String location;
    private JournalWriter writer;
    private ImportedColumnMetadata metadata[];

    public JournalImportListener(JournalWriterFactory factory, String location) {
        this.factory = factory;
        this.location = location;
    }

    @Override
    public void close() {
        if (writer != null) {
            writer.close();
        }
    }

    @Override
    public void onError(int line) {

    }

    @Override
    public void onFieldCount(int count) {
    }

    @Override
    public void onFields(int line, CharSequence[] values, int hi) {
        try {
            JournalEntryWriter w = writer.entryWriter();
            for (int i = 0; i < hi; i++) {
                if (values[i].length() == 0) {
                    continue;
                }
                try {
                    switch (metadata[i].importedType) {
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
                        case DATE_ISO:
                            w.putDate(i, Dates.parseDateTime(values[i]));
                            break;
                        case DATE_1:
                            w.putDate(i, Dates.parseDateTimeFmt1(values[i]));
                            break;
                        case DATE_2:
                            w.putDate(i, Dates.parseDateTimeFmt2(values[i]));
                            break;
                        case SYMBOL:
                            w.putSym(i, values[i]);
                            break;
                        case LONG:
                            w.putLong(i, Numbers.parseLong(values[i]));
                            break;
                        case BOOLEAN:
                            w.putBool(i, Chars.equalsIgnoreCase(values[i], "true"));
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
    public void onHeader(CharSequence[] values, int hi) {
    }

    @Override
    public void onLineCount(int count) {

    }

    @Override
    public void onMetadata(ImportedColumnMetadata metadata[]) {
        if (writer == null) {
            try {
                writer = factory.writer(new JournalStructure(location, this.metadata = metadata));
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }
}
