/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.io.parser.listener;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.collections.LongList;
import com.nfsdb.collections.ObjList;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.ImportedColumnMetadata;
import com.nfsdb.logging.Logger;
import com.nfsdb.misc.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class JournalImportListener implements InputAnalysisListener, Closeable {
    private static final Logger LOGGER = Logger.getLogger(JournalImportListener.class);
    private final JournalWriterFactory factory;
    private final String location;
    private final LongList errors = new LongList();
    private JournalWriter writer;
    private ObjList<ImportedColumnMetadata> metadata;

    public JournalImportListener(JournalWriterFactory factory, String location) {
        this.factory = factory;
        this.location = location;
    }

    @Override
    public void close() {
        Misc.free(writer);
    }

    public LongList getErrors() {
        return errors;
    }

    @Override
    public void onError(int line) {

    }

    @Override
    public void onFieldCount(int count) {
    }

    @SuppressFBWarnings({"SF_SWITCH_NO_DEFAULT"})
    @Override
    public void onFields(int line, CharSequence[] values, int hi) {
        try {
            JournalEntryWriter w = writer.entryWriter();
            for (int i = 0; i < hi; i++) {
                if (Unsafe.arrayGet(values, i).length() == 0) {
                    continue;
                }
                try {
                    switch (metadata.getQuick(i).importedType) {
                        case STRING:
                            w.putStr(i, Unsafe.arrayGet(values, i));
                            break;
                        case DOUBLE:
                            w.putDouble(i, Numbers.parseDouble(Unsafe.arrayGet(values, i)));
                            break;
                        case INT:
                            w.putInt(i, Numbers.parseInt(Unsafe.arrayGet(values, i)));
                            break;
                        case FLOAT:
                            w.putFloat(i, Numbers.parseFloat(Unsafe.arrayGet(values, i)));
                            break;
                        case DATE_ISO:
                            w.putDate(i, Dates.parseDateTime(Unsafe.arrayGet(values, i)));
                            break;
                        case DATE_1:
                            w.putDate(i, Dates.parseDateTimeFmt1(Unsafe.arrayGet(values, i)));
                            break;
                        case DATE_2:
                            w.putDate(i, Dates.parseDateTimeFmt2(Unsafe.arrayGet(values, i)));
                            break;
                        case DATE_3:
                            w.putDate(i, Dates.parseDateTimeFmt3(Unsafe.arrayGet(values, i)));
                            break;
                        case SYMBOL:
                            w.putSym(i, Unsafe.arrayGet(values, i));
                            break;
                        case LONG:
                            w.putLong(i, Numbers.parseLong(Unsafe.arrayGet(values, i)));
                            break;
                        case BOOLEAN:
                            w.putBool(i, Chars.equalsIgnoreCase(Unsafe.arrayGet(values, i), "true"));
                    }
                } catch (Exception e) {
                    errors.increment(i);
                    LOGGER.debug("Error at (%d,%d) as %s: %s", line, i, metadata.getQuick(i).type, e.getMessage());
                    break;
                }
            }
            w.append();
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    @Override
    public void onHeader(CharSequence[] values, int hi) {
    }

    @Override
    public void onLineCount(int count) {

    }

    @Override
    public void onMetadata(ObjList<ImportedColumnMetadata> metadata) {
        if (writer == null) {
            try {
                writer = factory.bulkWriter(new JournalStructure(location, this.metadata = metadata));
                errors.seed(metadata.size(), 0);
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }
}
