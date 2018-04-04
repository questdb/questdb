/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.parser.plaintext;

import com.questdb.BootstrapEnv;
import com.questdb.common.ColumnType;
import com.questdb.common.JournalRuntimeException;
import com.questdb.ex.ImportColumnCountException;
import com.questdb.ex.ImportNameException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.parser.ImportedColumnMetadata;
import com.questdb.std.*;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.DirectBytes;
import com.questdb.std.str.DirectCharSink;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.Factory;
import com.questdb.store.factory.configuration.ColumnMetadata;
import com.questdb.store.factory.configuration.JournalMetadata;
import com.questdb.store.factory.configuration.JournalStructure;

import java.io.Closeable;

import static com.questdb.store.factory.configuration.JournalConfiguration.DOES_NOT_EXIST;
import static com.questdb.store.factory.configuration.JournalConfiguration.EXISTS;

public class PlainTextStoringParser implements MetadataAwareTextParser, Closeable, Mutable {
    public static final int ATOMICITY_STRICT = 0;
    public static final int ATOMICITY_RELAXED = 1;

    private static final Log LOG = LogFactory.getLog(PlainTextStoringParser.class);
    private final Factory factory;
    private final LongList errors = new LongList();
    private final DirectCharSink utf8Sink = new DirectCharSink(4096);
    private String name;
    private ObjList<ImportedColumnMetadata> metadata;
    private boolean header;
    private JournalWriter writer;
    private long _size;
    private boolean overwrite;
    private boolean durable;
    private int atomicity;

    public PlainTextStoringParser(BootstrapEnv env) {
        this.factory = env.factory;
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
        utf8Sink.close();
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

    public ObjList<ImportedColumnMetadata> getImportedMetadata() {
        return metadata;
    }

    public long getImportedRowCount() {
        try {
            return writer.size() - _size;
        } catch (JournalException e) {
            throw new JournalRuntimeException(e);
        }
    }

    public JournalMetadata getJournalMetadata() {
        return writer.getMetadata();
    }

    public boolean isHeader() {
        return header;
    }

    public PlainTextStoringParser of(String name, boolean overwrite, boolean durable, int atomicity) {
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
                    DirectByteCharSequence charField;
                    ImportedColumnMetadata m = metadata.getQuick(i);
                    switch (m.importedColumnType) {
                        case ColumnType.STRING:
                            utf8Sink.clear();
                            charField = values.getQuick(i);
                            Chars.utf8Decode(charField.getLo(), charField.getHi(), utf8Sink);
                            w.putStr(i, (DirectBytes) utf8Sink);
                            break;
                        case ColumnType.DOUBLE:
                            w.putDouble(i, Numbers.parseDouble(values.getQuick(i)));
                            break;
                        case ColumnType.INT:
                            w.putInt(i, Numbers.parseInt(values.getQuick(i)));
                            break;
                        case ColumnType.FLOAT:
                            w.putFloat(i, Numbers.parseFloat(values.getQuick(i)));
                            break;
                        case ColumnType.DATE:
                            if (m.dateFormat != null && m.dateLocale != null) {
                                w.putDate(i, m.dateFormat.parse(values.getQuick(i), m.dateLocale));
                            } else {
                                throw NumericException.INSTANCE;
                            }
                            break;
                        case ColumnType.SYMBOL:
                            utf8Sink.clear();
                            charField = values.getQuick(i);
                            Chars.utf8Decode(charField.getLo(), charField.getHi(), utf8Sink);
                            w.putSym(i, utf8Sink);
                            break;
                        case ColumnType.LONG:
                            w.putLong(i, Numbers.parseLong(values.getQuick(i)));
                            break;
                        case ColumnType.BOOLEAN:
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
            cm.type = im.importedColumnType;

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
            im.importedColumnType = cm.type;
        }

        return factory.writer(jm);
    }
}
