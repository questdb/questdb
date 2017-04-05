/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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
 ******************************************************************************/

package com.questdb.txt.parser.listener;

import com.questdb.JournalEntryWriter;
import com.questdb.JournalWriter;
import com.questdb.ex.*;
import com.questdb.factory.Factory;
import com.questdb.factory.configuration.ColumnMetadata;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Chars;
import com.questdb.std.time.Dates;
import com.questdb.misc.Misc;
import com.questdb.misc.Numbers;
import com.questdb.std.LongList;
import com.questdb.std.Mutable;
import com.questdb.std.ObjList;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.store.ColumnType;
import com.questdb.txt.ImportedColumnMetadata;
import com.questdb.txt.ImportedColumnType;

import java.io.Closeable;

import static com.questdb.factory.configuration.JournalConfiguration.DOES_NOT_EXIST;
import static com.questdb.factory.configuration.JournalConfiguration.EXISTS;

public class JournalImportListener implements InputAnalysisListener, Closeable, Mutable {
    public static final int ATOMICITY_STRICT = 0;
    public static final int ATOMICITY_RELAXED = 1;

    private static final Log LOG = LogFactory.getLog(JournalImportListener.class);
    private final Factory factory;
    private final LongList errors = new LongList();
    private String name;
    private ObjList<ImportedColumnMetadata> metadata;
    private boolean header;
    private JournalWriter writer;
    private long _size;
    private boolean overwrite;
    private boolean durable;
    private int atomicity;

    public JournalImportListener(Factory factory) {
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

    public void commit() {
        if (writer != null) {
            try {
                if (durable) {
                    writer.commitDurable();
                } else {
                    writer.commit();
                }
            } catch (JournalException e) {
                throw new JournalRuntimeException(e);
            }
        }
    }

    public LongList getErrors() {
        return errors;
    }

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

    public boolean isHeader() {
        return header;
    }

    public JournalImportListener of(String name, boolean overwrite, boolean durable, int atomicity) {
        this.name = name;
        this.overwrite = overwrite;
        this.durable = durable;
        this.atomicity = atomicity;
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
                    switch (metadata.getQuick(i).importedColumnType) {
                        case ImportedColumnType.STRING:
                            w.putStr(i, values.getQuick(i));
                            break;
                        case ImportedColumnType.DOUBLE:
                            w.putDouble(i, Numbers.parseDouble(values.getQuick(i)));
                            break;
                        case ImportedColumnType.INT:
                            w.putInt(i, Numbers.parseInt(values.getQuick(i)));
                            break;
                        case ImportedColumnType.FLOAT:
                            w.putFloat(i, Numbers.parseFloat(values.getQuick(i)));
                            break;
                        case ImportedColumnType.DATE_ISO:
                            w.putDate(i, Dates.parseDateTime(values.getQuick(i)));
                            break;
                        case ImportedColumnType.DATE_1:
                            w.putDate(i, Dates.parseDateTimeFmt1(values.getQuick(i)));
                            break;
                        case ImportedColumnType.DATE_2:
                            w.putDate(i, Dates.parseDateTimeFmt2(values.getQuick(i)));
                            break;
                        case ImportedColumnType.DATE_3:
                            w.putDate(i, Dates.parseDateTimeFmt3(values.getQuick(i)));
                            break;
                        case ImportedColumnType.SYMBOL:
                            w.putSym(i, values.getQuick(i));
                            break;
                        case ImportedColumnType.LONG:
                            w.putLong(i, Numbers.parseLong(values.getQuick(i)));
                            break;
                        case ImportedColumnType.BOOLEAN:
                            w.putBool(i, Chars.equalsIgnoreCase(values.getQuick(i), "true"));
                            break;
                        default:
                            break;
                    }
                } catch (Exception e) {
                    switch (atomicity) {
                        case ATOMICITY_STRICT:
                            LOG.info().$("Error at (").$(line).$(',').$(i).$(')').$();
                            throw new JournalRuntimeException("Error on line: " + line + ", col: " + i);
                        default:
                            errors.increment(i);
                            LOG.debug().$("Error at (").$(line).$(',').$(i).$(") as ").$(metadata.getQuick(i).importedColumnType).$(": ").$(e.getMessage()).$();
                            append = false;
                    }
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
    public void onMetadata(ObjList<ImportedColumnMetadata> metadata, boolean header) {
        if (writer == null) {
            this.metadata = metadata;
            this.header = header;
            try {
                switch (factory.getConfiguration().exists(name)) {
                    case DOES_NOT_EXIST:
                        writer = factory.writer(createStructure());
                        break;
                    case EXISTS:
                        if (overwrite) {
                            factory.delete(name);
                            writer = factory.writer(createStructure());
                        } else {
                            writer = mapColumnsAndOpenWriter();
                        }
                        break;
                    default:
                        throw ImportNameException.INSTANCE;
                }
                writer.setSequentialAccess(true);
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
            cm.type = ImportedColumnType.columnTypeOf(im.importedColumnType);

            switch (cm.type) {
                case ColumnType.STRING:
                    cm.size = cm.avgSize + 4;
                    break;
                default:
                    cm.size = ColumnType.sizeOf(cm.type);
                    break;
            }
            m.add(cm);
        }
        return new JournalStructure(name, m);
    }

    @SuppressWarnings("unchecked")
    private JournalWriter mapColumnsAndOpenWriter() throws JournalException {

        JournalMetadata<Object> jm = factory.getMetadata(name);

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
            im.importedColumnType = toImportedType(cm.type, im.importedColumnType);
        }

        return factory.writer(jm);
    }

    private int toImportedType(int columnType, int importedColumnType) {

        switch (columnType) {
            case ColumnType.BINARY:
                throw ImportBinaryException.INSTANCE;
            case ColumnType.DATE:
                switch (importedColumnType) {
                    case ImportedColumnType.DATE_1:
                    case ImportedColumnType.DATE_2:
                    case ImportedColumnType.DATE_3:
                    case ImportedColumnType.DATE_ISO:
                        return importedColumnType;
                    default:
                        return ImportedColumnType.DATE_ISO;
                }
            case ColumnType.STRING:
                return ImportedColumnType.STRING;
            case ColumnType.BOOLEAN:
                return ImportedColumnType.BOOLEAN;
            case ColumnType.BYTE:
                return ImportedColumnType.BYTE;
            case ColumnType.DOUBLE:
                return ImportedColumnType.DOUBLE;
            case ColumnType.FLOAT:
                return ImportedColumnType.FLOAT;
            case ColumnType.INT:
                return ImportedColumnType.INT;
            case ColumnType.LONG:
                return ImportedColumnType.LONG;
            case ColumnType.SHORT:
                return ImportedColumnType.SHORT;
            case ColumnType.SYMBOL:
                return ImportedColumnType.SYMBOL;
            default:
                return importedColumnType;

        }
    }
}
