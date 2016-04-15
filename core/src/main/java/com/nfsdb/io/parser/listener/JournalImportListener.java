/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Numbers;
import com.nfsdb.std.DirectByteCharSequence;
import com.nfsdb.std.LongList;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjList;
import com.nfsdb.store.ColumnType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;

@SuppressFBWarnings({"EXS_EXCEPTION_SOFTENING_NO_CHECKED"})
public class JournalImportListener implements InputAnalysisListener, Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(JournalImportListener.class);
    private final JournalWriterFactory factory;
    private final LongList errors = new LongList();
    private String location;
    private ObjList<ImportedColumnMetadata> metadata;
    private JournalWriter writer;
    private long _size;
    private boolean overwrite;

    public JournalImportListener(JournalWriterFactory factory) {
        this.factory = factory;
    }

    @Override
    public void clear() {
        writer = Misc.free(writer);
        errors.clear();
        _size = 0;
    }

    @Override
    public void close() {
        clear();
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

    public JournalImportListener of(String location, boolean overwrite) {
        this.location = location;
        this.overwrite = overwrite;
        return this;
    }

    @Override
    public void onError(int line) {

    }

    @Override
    public void onFieldCount(int count) {
    }

    @Override
    public void onFields(int line, ObjList<DirectByteCharSequence> values, int hi) {
        boolean append = true;
        try {
            JournalEntryWriter w = writer.entryWriter();
            for (int i = 0; i < hi; i++) {
                if (values.getQuick(i).length() == 0) {
                    continue;
                }
                try {
                    switch (metadata.getQuick(i).type) {
                        case STRING:
                            w.putStr(i, values.getQuick(i));
                            break;
                        case DOUBLE:
                            w.putDouble(i, Numbers.parseDouble(values.getQuick(i)));
                            break;
                        case INT:
                            w.putInt(i, Numbers.parseInt(values.getQuick(i)));
                            break;
                        case FLOAT:
                            w.putFloat(i, Numbers.parseFloat(values.getQuick(i)));
                            break;
                        case DATE_ISO:
                            w.putDate(i, Dates.parseDateTime(values.getQuick(i)));
                            break;
                        case DATE_1:
                            w.putDate(i, Dates.parseDateTimeFmt1(values.getQuick(i)));
                            break;
                        case DATE_2:
                            w.putDate(i, Dates.parseDateTimeFmt2(values.getQuick(i)));
                            break;
                        case DATE_3:
                            w.putDate(i, Dates.parseDateTimeFmt3(values.getQuick(i)));
                            break;
                        case SYMBOL:
                            w.putSym(i, values.getQuick(i));
                            break;
                        case LONG:
                            w.putLong(i, Numbers.parseLong(values.getQuick(i)));
                            break;
                        case BOOLEAN:
                            w.putBool(i, Chars.equalsIgnoreCase(values.getQuick(i), "true"));
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
    public void onHeader(ObjList<DirectByteCharSequence> values, int hi) {
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
                        this.metadata = metadata;
                        writer = factory.bulkWriter(createStructure());
                        break;
                    case EXISTS:
                        this.metadata = metadata;
                        if (overwrite) {
                            factory.getConfiguration().delete(location);
                            writer = factory.bulkWriter(createStructure());
                        } else {
                            writer = mapColumnsAndOpenWriter();
                        }
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

    private JournalStructure createStructure() {
        ObjList<ColumnMetadata> m = new ObjList<>(metadata.size());
        for (int i = 0, n = metadata.size(); i < n; i++) {
            ColumnMetadata cm = new ColumnMetadata();
            ImportedColumnMetadata im = metadata.getQuick(i);
            cm.name = im.name.toString();
            cm.type = im.type.getColumnType();

            switch (cm.type) {
                case STRING:
                    cm.size = cm.avgSize + 4;
                    break;
                default:
                    cm.size = cm.type.size();
                    break;
            }
            m.add(cm);
        }
        return new JournalStructure(location, m);
    }

    @SuppressWarnings("unchecked")
    private JournalWriter mapColumnsAndOpenWriter() throws JournalException {

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
            im.type = toImportedType(cm.type, im.type);
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
