/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package io.questdb.cutlass.text;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cutlass.text.types.BadDateAdapter;
import io.questdb.cutlass.text.types.BadTimestampAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.DirectCharSink;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class CairoTextWriter implements TextLexer.Listener, Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(CairoTextWriter.class);
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final LongList columnErrorCounts = new LongList();
    private final DirectCharSink utf8Sink;
    private final AppendMemory appendMemory = new AppendMemory();
    private final Path path;
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final TypeManager typeManager;
    private CharSequence tableName;
    private TableWriter writer;
    private long _size;
    private boolean overwrite;
    private boolean durable;
    private int atomicity;
    private ObjList<TypeAdapter> types;

    public CairoTextWriter(
            CairoEngine engine,
            Path path,
            TextConfiguration textConfiguration,
            TypeManager typeManager
    ) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.path = path;
        this.utf8Sink = new DirectCharSink(textConfiguration.getUtf8SinkSize());
        this.typeManager = typeManager;
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
        return writer == null ? null : writer.getMetadata();
    }

    public int getPartitionBy() {
        return writer == null ? PartitionBy.NONE : writer.getPartitionBy();
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public long getWrittenLineCount() {
        return writer == null ? 0 : writer.size() - _size;
    }

    public void of(CharSequence name, boolean overwrite, boolean durable, int atomicity) {
        this.tableName = name;
        this.overwrite = overwrite;
        this.durable = durable;
        this.atomicity = atomicity;
    }

    @Override
    public void onFields(long line, ObjList<DirectByteCharSequence> values, int valuesLength) {
        final TableWriter.Row w = writer.newRow();
        for (int i = 0; i < valuesLength; i++) {
            final DirectByteCharSequence dbcs = values.getQuick(i);
            if (dbcs.length() == 0) {
                continue;
            }
            try {
                types.getQuick(i).write(w, i, dbcs);
            } catch (Exception ignore) {
                logError(line, i, dbcs);
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

    private void logError(long line, int i, DirectByteCharSequence dbcs) {
        LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(types.getQuick(i).getType())).$("]\n\t");
        logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(dbcs).$();
        columnErrorCounts.increment(i);
    }

    private void createTable(
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> detectedTypes,
            CairoSecurityContext cairoSecurityContext
    ) {
        engine.creatTable(
                cairoSecurityContext,
                appendMemory,
                path,
                tableStructureAdapter.of(names, detectedTypes)
        );
        this.types = detectedTypes;
    }

    private void logTypeError(int i) {
        LOG.info()
                .$("mis-detected [table=").$(tableName)
                .$(", column=").$(i)
                .$(", type=").$(ColumnType.nameOf(this.types.getQuick(i).getType()))
                .$(']').$();
    }

    private TableWriter openWriterAndOverrideImportTypes(
            CairoSecurityContext cairoSecurityContext,
            ObjList<TypeAdapter> detectedTypes
    ) {

        TableWriter writer = engine.getWriter(cairoSecurityContext, tableName);
        RecordMetadata metadata = writer.getMetadata();

        // now, compare column count.
        // Cannot continue if different

        if (metadata.getColumnCount() < detectedTypes.size()) {
            writer.close();
            throw CairoException.instance(0)
                    .put("column count mismatch [textColumnCount=").put(detectedTypes.size())
                    .put(", tableColumnCount=").put(metadata.getColumnCount())
                    .put(", table=").put(tableName)
                    .put(']');
        }

        this.types = detectedTypes;

        // now overwrite detected types with actual table column types
        for (int i = 0, n = this.types.size(); i < n; i++) {
            final int columnType = metadata.getColumnType(i);
            if (this.types.getQuick(i).getType() != columnType) {
                // when DATE type is mis-detected as STRING we
                // wouldn't have neither date format nor locale to
                // use when populating this field
                switch (columnType) {
                    case ColumnType.DATE:
                        logTypeError(i);
                        this.types.setQuick(i, BadDateAdapter.INSTANCE);
                        break;
                    case ColumnType.TIMESTAMP:
                        logTypeError(i);
                        this.types.setQuick(i, BadTimestampAdapter.INSTANCE);
                        break;
                    case ColumnType.BINARY:
                        writer.close();
                        throw CairoException.instance(0).put("cannot import text into BINARY column [index=").put(i).put(']');
                    default:
                        this.types.setQuick(i, typeManager.getTypeAdapter(columnType));
                        break;
                }
            }
        }

        return writer;
    }

    void prepareTable(
            CairoSecurityContext cairoSecurityContext,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> detectedTypes
    ) {
        assert writer == null;

        if (detectedTypes.size() == 0) {
            throw CairoException.instance(0).put("cannot determine text structure");
        }

        switch (engine.getStatus(cairoSecurityContext, path, tableName)) {
            case TableUtils.TABLE_DOES_NOT_EXIST:
                createTable(names, detectedTypes, cairoSecurityContext);
                writer = engine.getWriter(cairoSecurityContext, tableName);
                break;
            case TableUtils.TABLE_EXISTS:
                if (overwrite) {
                    engine.remove(cairoSecurityContext, path, tableName);
                    createTable(names, detectedTypes, cairoSecurityContext);
                    writer = engine.getWriter(cairoSecurityContext, tableName);
                } else {
                    writer = openWriterAndOverrideImportTypes(cairoSecurityContext, detectedTypes);
                }
                break;
            default:
                throw CairoException.instance(0).put("name is reserved [table=").put(tableName).put(']');
        }
        _size = writer.size();
        columnErrorCounts.seed(writer.getMetadata().getColumnCount(), 0);
    }

    private class TableStructureAdapter implements TableStructure {
        private ObjList<CharSequence> names;
        private ObjList<TypeAdapter> types;

        @Override
        public int getColumnCount() {
            return types.size();
        }

        @Override
        public CharSequence getColumnName(int columnIndex) {
            return names.getQuick(columnIndex);
        }

        @Override
        public int getColumnType(int columnIndex) {
            return types.getQuick(columnIndex).getType();
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

        TableStructureAdapter of(ObjList<CharSequence> names, ObjList<TypeAdapter> types) {
            this.names = names;
            this.types = types;
            return this;
        }
    }
}
