/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.cutlass.text;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.AppendOnlyVirtualMemory;
import io.questdb.cutlass.text.types.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class CairoTextWriter implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(CairoTextWriter.class);
    private static final String WRITER_LOCK_REASON = "textWriter";
    private final CairoConfiguration configuration;
    private final CairoEngine engine;
    private final LongList columnErrorCounts = new LongList();
    private final AppendOnlyVirtualMemory appendMemory = new AppendOnlyVirtualMemory();
    private final Path path;
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private final TypeManager typeManager;
    private final ObjectPool<OtherToTimestampAdapter> otherToTimestampAdapterPool = new ObjectPool<>(OtherToTimestampAdapter::new, 4);
    private CharSequence tableName;
    private TableWriter writer;
    private long _size;
    private boolean overwrite;
    private boolean durable;
    private int atomicity;
    private int partitionBy;
    private int timestampIndex = -1;
    private CharSequence importedTimestampColumnName;
    private CharSequence designatedTimestampColumnName;
    private int designatedTimestampIndex;
    private ObjList<TypeAdapter> types;
    private final TextLexer.Listener nonPartitionedListener = this::onFieldsNonPartitioned;
    private TimestampAdapter timestampAdapter;
    private final TextLexer.Listener partitionedListener = this::onFieldsPartitioned;
    private int warnings;

    public CairoTextWriter(
            CairoEngine engine,
            Path path,
            TypeManager typeManager
    ) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
        this.path = path;
        this.typeManager = typeManager;
    }

    @Override
    public void clear() {
        otherToTimestampAdapterPool.clear();
        writer = Misc.free(writer);
        columnErrorCounts.clear();
        timestampAdapter = null;
        _size = 0;
        warnings = TextLoadWarning.NONE;
        designatedTimestampColumnName = null;
        designatedTimestampIndex = -1;
        importedTimestampColumnName = null;
    }

    @Override
    public void close() {
        clear();
        appendMemory.close();
    }

    public void closeWriter() {
        writer = Misc.free(writer);
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
        return partitionBy;
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public TextLexer.Listener getTextListener() {
        return timestampAdapter != null ? partitionedListener : nonPartitionedListener;
    }

    public CharSequence getTimestampCol() {
        return designatedTimestampColumnName;
    }

    public int getWarnings() {
        return warnings;
    }

    public long getWrittenLineCount() {
        return writer == null ? 0 : writer.size() - _size;
    }

    public void of(CharSequence name, boolean overwrite, boolean durable, int atomicity, int partitionBy, CharSequence timestampIndexCol) {
        this.tableName = name;
        this.overwrite = overwrite;
        this.durable = durable;
        this.atomicity = atomicity;
        this.partitionBy = partitionBy;
        this.importedTimestampColumnName = timestampIndexCol;
    }

    public void onFieldsNonPartitioned(long line, ObjList<DirectByteCharSequence> values, int valuesLength) {
        final TableWriter.Row w = writer.newRow();
        for (int i = 0; i < valuesLength; i++) {
            final DirectByteCharSequence dbcs = values.getQuick(i);
            if (dbcs.length() == 0) {
                continue;
            }
            if (onField(line, dbcs, w, i)) return;
        }
        w.append();
    }

    public void onFieldsPartitioned(long line, ObjList<DirectByteCharSequence> values, int valuesLength) {
        final int timestampIndex = this.timestampIndex;
        DirectByteCharSequence dbcs = values.getQuick(timestampIndex);
        try {
            final TableWriter.Row w = writer.newRow(timestampAdapter.getTimestamp(dbcs));
            for (int i = 0; i < valuesLength; i++) {
                dbcs = values.getQuick(i);
                if (i == timestampIndex || dbcs.length() == 0) {
                    continue;
                }
                if (onField(line, dbcs, w, i)) return;
            }
            w.append();
        } catch (Exception e) {
            logError(line, timestampIndex, dbcs);
        }
    }

    private void createTable(
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> detectedTypes,
            CairoSecurityContext cairoSecurityContext
    ) throws TextException {
        engine.createTable(
                cairoSecurityContext,
                appendMemory,
                path,
                tableStructureAdapter.of(names, detectedTypes)
        );
        this.types = detectedTypes;
    }

    private void logError(long line, int i, DirectByteCharSequence dbcs) {
        LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(types.getQuick(i).getType())).$("]\n\t");
        logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(dbcs).$();
        columnErrorCounts.increment(i);
    }

    private void logTypeError(int i) {
        LOG.info()
                .$("mis-detected [table=").$(tableName)
                .$(", column=").$(i)
                .$(", type=").$(ColumnType.nameOf(this.types.getQuick(i).getType()))
                .$(']').$();
    }

    private boolean onField(long line, DirectByteCharSequence dbcs, TableWriter.Row w, int i) {
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
                    return true;
                default:
                    // SKIP column
                    break;
            }
        }
        return false;
    }

    private TableWriter openWriterAndOverrideImportTypes(
            CairoSecurityContext cairoSecurityContext,
            ObjList<TypeAdapter> detectedTypes
    ) {

        TableWriter writer = engine.getWriter(cairoSecurityContext, tableName, WRITER_LOCK_REASON);
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
            final TypeAdapter detectedAdapter = this.types.getQuick(i);
            final int detectedType = detectedAdapter.getType();
            if (detectedType != columnType) {
                // when DATE type is mis-detected as STRING we
                // wouldn't have neither date format nor locale to
                // use when populating this field
                switch (columnType) {
                    case ColumnType.DATE:
                        logTypeError(i);
                        this.types.setQuick(i, BadDateAdapter.INSTANCE);
                        break;
                    case ColumnType.TIMESTAMP:
                        if (detectedAdapter instanceof TimestampCompatibleAdapter) {
                            this.types.setQuick(i, otherToTimestampAdapterPool.next().of((TimestampCompatibleAdapter) detectedAdapter));
                        } else {
                            logTypeError(i);
                            this.types.setQuick(i, BadTimestampAdapter.INSTANCE);
                        }
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
    ) throws TextException {
        assert writer == null;

        if (detectedTypes.size() == 0) {
            throw CairoException.instance(0).put("cannot determine text structure");
        }

        switch (engine.getStatus(cairoSecurityContext, path, tableName)) {
            case TableUtils.TABLE_DOES_NOT_EXIST:
                createTable(names, detectedTypes, cairoSecurityContext);
                writer = engine.getWriter(cairoSecurityContext, tableName, WRITER_LOCK_REASON);
                designatedTimestampColumnName = writer.getDesignatedTimestampColumnName();
                designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                partitionBy = writer.getPartitionBy();
                break;
            case TableUtils.TABLE_EXISTS:
                if (overwrite) {
                    engine.remove(cairoSecurityContext, path, tableName);
                    createTable(names, detectedTypes, cairoSecurityContext);
                    writer = engine.getWriter(cairoSecurityContext, tableName, WRITER_LOCK_REASON);
                } else {
                    writer = openWriterAndOverrideImportTypes(cairoSecurityContext, detectedTypes);
                    designatedTimestampColumnName = writer.getDesignatedTimestampColumnName();
                    designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                    if (importedTimestampColumnName != null &&
                            !Chars.equalsNc(importedTimestampColumnName, designatedTimestampColumnName)) {
                        warnings |= TextLoadWarning.TIMESTAMP_MISMATCH;
                    }
                    if (partitionBy != PartitionBy.NONE && partitionBy != writer.getPartitionBy()) {
                        warnings |= TextLoadWarning.PARTITION_TYPE_MISMATCH;
                    }
                    partitionBy = writer.getPartitionBy();
                    tableStructureAdapter.of(names, detectedTypes);
                }
                break;
            default:
                throw CairoException.instance(0).put("name is reserved [table=").put(tableName).put(']');
        }
        _size = writer.size();
        columnErrorCounts.seed(writer.getMetadata().getColumnCount(), 0);
        if (timestampIndex != -1 && types.getQuick(timestampIndex).getType() == ColumnType.TIMESTAMP) {
            timestampAdapter = (TimestampAdapter) types.getQuick(timestampIndex);
        } else {
            timestampAdapter = null;
        }
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
        public boolean isIndexed(int columnIndex) {
            return false;
        }

        @Override
        public boolean isSequential(int columnIndex) {
            return false;
        }

        @Override
        public int getPartitionBy() {
            return partitionBy;
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
            return timestampIndex;
        }

        @Override
        public int getMaxUncommittedRows() {
            return configuration.getMaxUncommittedRows();
        }

        @Override
        public long getCommitLag() {
            return configuration.getCommitLag();
        }

        TableStructureAdapter of(ObjList<CharSequence> names, ObjList<TypeAdapter> types) throws TextException {
            this.names = names;
            this.types = types;

            if (importedTimestampColumnName == null && designatedTimestampColumnName == null) {
                timestampIndex = -1;
            } else if (importedTimestampColumnName != null) {
                timestampIndex = names.indexOf(importedTimestampColumnName);
                if (timestampIndex == -1) {
                    throw TextException.$("invalid timestamp column '").put(importedTimestampColumnName).put('\'');
                }
            } else {
                timestampIndex = names.indexOf(designatedTimestampColumnName);
                if (timestampIndex == -1) {
                    // columns in the imported file may not have headers, then use writer timestamp index
                    timestampIndex = designatedTimestampIndex;
                }
            }

            if (timestampIndex > -1) {
                final TypeAdapter timestampAdapter = types.getQuick(timestampIndex);
                if ((timestampAdapter.getType() != ColumnType.LONG && timestampAdapter.getType() != ColumnType.TIMESTAMP) || timestampAdapter == BadTimestampAdapter.INSTANCE) {
                    throw TextException.$("not a timestamp '").put(importedTimestampColumnName).put('\'');
                }
            }
            return this;
        }
    }
}
