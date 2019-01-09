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

package com.questdb.cutlass.text;

import com.questdb.cairo.*;
import com.questdb.cairo.sql.CairoEngine;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.std.*;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectByteCharSequence;
import com.questdb.std.str.DirectCharSink;
import com.questdb.std.str.Path;

import java.io.Closeable;

import static com.questdb.std.Chars.utf8DecodeMultiByte;

public class CairoTextWriter implements TextLexer.Listener, Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(CairoTextWriter.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final LongList columnErrorCounts = new LongList();
    private final DirectCharSink utf8Sink;
    private final AppendMemory appendMemory = new AppendMemory();
    private final Path path;
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private CharSequence tableName;
    private ObjList<TextMetadata> textMetadata;
    private TableWriter writer;
    private long _size;
    private boolean overwrite;
    private boolean durable;
    private int atomicity;

    public CairoTextWriter(CairoConfiguration configuration, CairoEngine engine, Path path, TextConfiguration textConfiguration) {
        this.configuration = configuration;
        this.engine = engine;
        this.path = path;
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkCapacity());
    }

    public static boolean utf8Decode(long lo, long hi, CharSink sink) {
        long p = lo;
        int quoteCount = 0;

        while (p < hi) {
            byte b = Unsafe.getUnsafe().getByte(p);
            if (b < 0) {
                int n = utf8DecodeMultiByte(p, hi, b, sink);
                if (n == -1) {
                    // UTF8 error
                    return false;
                }
                p += n;
            } else {
                if (b == '"') {
                    if (quoteCount++ % 2 == 0) {
                        sink.put('"');
                    }
                } else {
                    sink.put((char) b);
                }
                ++p;
            }
        }
        return true;
    }

    @Override
    public void clear() {
        writer = Misc.free(writer);
        columnErrorCounts.clear();
        _size = 0;
    }

    @Override
    public void close() {
        clear();
        utf8Sink.close();
        appendMemory.close();
    }

    public void commit() {
        if (writer != null) {
            if (durable) {
                assert false;
            } else {
                writer.commit();
            }
        }
    }

    public LongList getColumnErrorCounts() {
        return columnErrorCounts;
    }

    public RecordMetadata getMetadata() {
        return writer.getMetadata();
    }

    public long getWrittenLineCount() {
        return writer.size() - _size;
    }

    public CairoTextWriter of(CharSequence name, boolean overwrite, boolean durable, int atomicity) {
        this.tableName = name;
        this.overwrite = overwrite;
        this.durable = durable;
        this.atomicity = atomicity;
        return this;
    }

    @Override
    public void onFields(long line, ObjList<DirectByteCharSequence> values, int hi) {
        final TableWriter.Row w = writer.newRow(0);
        for (int i = 0; i < hi; i++) {
            if (values.getQuick(i).length() == 0) {
                continue;
            }
            try {
                final TextMetadata m = textMetadata.getQuick(i);
                final DirectByteCharSequence dbcs = values.getQuick(i);
                switch (m.type) {
                    case ColumnType.BOOLEAN:
                        w.putBool(i, Chars.equalsIgnoreCase(values.getQuick(i), "true"));
                        break;
                    case ColumnType.STRING:
                        w.putStr(i, decode(dbcs));
                        break;
                    case ColumnType.DOUBLE:
                        w.putDouble(i, Numbers.parseDouble(dbcs));
                        break;
                    case ColumnType.BYTE:
                        w.putByte(i, (byte) Numbers.parseInt(dbcs));
                        break;
                    case ColumnType.SHORT:
                        w.putShort(i, (short) Numbers.parseInt(dbcs));
                        break;
                    case ColumnType.INT:
                        w.putInt(i, Numbers.parseInt(dbcs));
                        break;
                    case ColumnType.FLOAT:
                        w.putFloat(i, Numbers.parseFloat(dbcs));
                        break;
                    case ColumnType.DATE:
                        w.putDate(i, m.dateFormat.parse(decode(dbcs), m.dateLocale));
                        break;
                    case ColumnType.SYMBOL:
                        w.putSym(i, decode(dbcs));
                        break;
                    case ColumnType.LONG:
                        w.putLong(i, Numbers.parseLong(dbcs));
                        break;
                    default:
                        break;
                }
            } catch (NumericException | Utf8Exception ignore) {
                LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(textMetadata.getQuick(i).type)).$("]\n\t");
                logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(values.getQuick(i)).$();
                columnErrorCounts.increment(i);
                switch (atomicity) {
                    case Atomicity.SKIP_ALL:
                        writer.rollback();
                        throw CairoException.instance(0).put("bad syntax [line=").put(line).put(", col=").put(i).put(']');
                    case Atomicity.SKIP_ROW:
                        w.cancel();
                        return;
                    default:
                        // SKIP column
                        break;
                }
            }
        }
        w.append();
    }

    private void createTable(ObjList<TextMetadata> importMetadata) {
        TableUtils.createTable(
                configuration.getFilesFacade(),
                appendMemory,
                path,
                configuration.getRoot(),
                tableStructureAdapter.of(importMetadata),
                configuration.getMkDirMode()
        );
    }

    private CharSequence decode(DirectByteCharSequence value) throws Utf8Exception {
        utf8Sink.clear();
        if (utf8Decode(value.getLo(), value.getHi(), utf8Sink)) {
            return utf8Sink;
        }
        throw Utf8Exception.INSTANCE;
    }

    private TableWriter openWriterAndOverrideImportTypes() {

        TableWriter writer = engine.getWriter(tableName);
        RecordMetadata metadata = writer.getMetadata();

        // now, compare column count.
        // Cannot continue if different

        if (metadata.getColumnCount() < this.textMetadata.size()) {
            writer.close();
            throw CairoException.instance(0)
                    .put("column count mismatch [textColumnCount=").put(textMetadata.size())
                    .put(", tableColumnCount=").put(metadata.getColumnCount())
                    .put(", table=").put(tableName)
                    .put(']');
        }


        // Go over "discovered" textMetadata and really adjust it
        // to what journal can actually take
        // one useful thing discovered type can bring is information
        // about date format. The rest of it we will pretty much overwrite

        for (int i = 0, n = this.textMetadata.size(); i < n; i++) {
            this.textMetadata.getQuick(i).type = metadata.getColumnType(i);
        }

        return writer;
    }

    void prepareTable(ObjList<TextMetadata> metadata) {
        assert writer == null;

        if (metadata.size() == 0) {
            throw CairoException.instance(0).put("cannot determine text structure");
        }

        this.textMetadata = metadata;
        switch (engine.getStatus(path, tableName)) {
            case TableUtils.TABLE_DOES_NOT_EXIST:
                createTable(metadata);
                writer = engine.getWriter(tableName);
                break;
            case TableUtils.TABLE_EXISTS:
                if (overwrite) {
                    engine.remove(path, tableName);
                    createTable(metadata);
                    writer = engine.getWriter(tableName);
                } else {
                    writer = openWriterAndOverrideImportTypes();
                }
                break;
            default:
                throw CairoException.instance(0).put("name is reserved [table=").put(tableName).put(']');
        }
        _size = writer.size();
        columnErrorCounts.seed(writer.getMetadata().getColumnCount(), 0);
    }

    private class TableStructureAdapter implements TableStructure {
        private ObjList<TextMetadata> importMetadata;

        @Override
        public int getColumnCount() {
            return importMetadata.size();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return importMetadata.getQuick(columnIndex).name;
        }

        @Override
        public int getColumnType(int columnIndex) {
            return importMetadata.getQuick(columnIndex).type;
        }

        @Override
        public int getIndexBlockCapacity(int columnIndex) {
            return 0;
        }

        @Override
        public boolean getIndexedFlag(int columnIndex) {
            return false;
        }

        @Override
        public int getPartitionBy() {
            // not yet on protocol
            return PartitionBy.NONE;
        }

        @Override
        public boolean getSymbolCacheFlag(int columnIndex) {
            return configuration.getDefaultSymbolCacheFlag();
        }

        @Override
        public int getSymbolCapacity(int columnIndex) {
            return configuration.getDefaultSymbolCapacity();
        }

        @Override
        public CharSequence getTableName() {
            return tableName;
        }

        @Override
        public int getTimestampIndex() {
            // not yet on protocol
            return -1;
        }

        TableStructureAdapter of(ObjList<TextMetadata> importMetadata) {
            this.importMetadata = importMetadata;
            return this;
        }
    }
}
