/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
import com.nfsdb.JournalKey;
import com.nfsdb.JournalWriter;
import com.nfsdb.ex.*;
import com.nfsdb.factory.JournalWriterFactory;
import com.nfsdb.factory.configuration.ColumnMetadata;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.io.ImportedColumnMetadata;
import com.nfsdb.io.ImportedColumnType;
import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.*;
import com.nfsdb.std.LongList;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class JournalImportListener implements InputAnalysisListener, Closeable {
    private static final Log LOG = LogFactory.getLog(JournalImportListener.class);
    private final JournalWriterFactory factory;
    private final String location;
    private final LongList errors = new LongList();
    private JournalWriter writer;
    private ObjList<ImportedColumnMetadata> metadata;
    private long _size;

    public JournalImportListener(JournalWriterFactory factory, String location) {
        this.factory = factory;
        this.location = location;
    }

    @Override
    public void close() {
        Misc.free(writer);
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    public void commit() {
        if (writer != null) {
            try {
                writer.commit();
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }

    public LongList getErrors() {
        return errors;
    }

    @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
    public long getImportedRowCount() {
        try {
            return writer.size() - _size;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    public JournalMetadata getMetadata() {
        return writer.getMetadata();
    }

    @Override
    public void onError(int line) {

    }

    @Override
    public void onFieldCount(int count) {
    }

    @Override
    public void onFields(int line, CharSequence[] values, int hi) {
        boolean append = true;
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
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    errors.increment(i);
                    LOG.debug().$("Error at (").$(line).$(',').$(i).$(") as ").$(metadata.getQuick(i).type).$(": ").$(e.getMessage()).$();
                    append = false;
                    break;
                }
            }
            if (append) {
                w.append();
            }
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
                switch (factory.getConfiguration().exists(location)) {
                    case DOES_NOT_EXIST:
                        writer = factory.bulkWriter(new JournalStructure(location, this.metadata = metadata));
                        break;
                    case EXISTS:
                        writer = mapColumnsAndOpenWriter(location, this.metadata = metadata);
                        break;
                    default:
                        throw ImportNameException.INSTANCE;
                }
                _size = writer.size();
                errors.seed(writer.getMetadata().getColumnCount(), 0);
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private JournalWriter mapColumnsAndOpenWriter(String location, ObjList<ImportedColumnMetadata> metadata) throws JournalException {
        JournalMetadata<Object> jm = factory.getConfiguration().createMetadata(new JournalKey<>(location));

        // now, compare column count.
        // Cannot continue if different

        if (jm.getColumnCount() != metadata.size()) {
            throw ImportColumnCountException.INSTANCE;
        }


        // Go over "discovered" metadata and really adjust it
        // to what journal can actually take
        // one useful thing discovered type can bring is information
        // about date format. The rest of it we will pretty much overwrite

        for (int i = 0, n = metadata.size(); i < n; i++) {
            ImportedColumnMetadata im = metadata.getQuick(i);
            ColumnMetadata cm = jm.getColumnQuick(i);

            im.type = cm.type;
            im.importedType = toImportedType(cm.type, im.importedType);
        }

        return factory.bulkWriter(jm);
    }

    private ImportedColumnType toImportedType(ColumnType type, ImportedColumnType importedType) {
        switch (type) {
            case STRING:
                return ImportedColumnType.STRING;
            case BINARY:
                throw ImportBinaryException.INSTANCE;
            case BOOLEAN:
                return ImportedColumnType.BOOLEAN;
            case BYTE:
                return ImportedColumnType.BYTE;
            case DATE:
                switch (importedType) {
                    case DATE_1:
                    case DATE_2:
                    case DATE_3:
                    case DATE_ISO:
                        return importedType;
                    default:
                        return ImportedColumnType.DATE_ISO;
                }
            case DOUBLE:
                return ImportedColumnType.DOUBLE;
            case FLOAT:
                return ImportedColumnType.FLOAT;
            case INT:
                return ImportedColumnType.INT;
            case LONG:
                return ImportedColumnType.LONG;
            case SHORT:
                return ImportedColumnType.SHORT;
            case SYMBOL:
                return ImportedColumnType.SYMBOL;
            default:
                return importedType;
        }
    }
}
