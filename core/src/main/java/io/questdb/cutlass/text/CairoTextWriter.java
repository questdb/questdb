/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.text.types.*;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class CairoTextWriter implements Closeable, Mutable {
    public static final int NO_INDEX = -1;
    private static final Log LOG = LogFactory.getLog(CairoTextWriter.class);
    private static final String WRITER_LOCK_REASON = "textWriter";
    private final LongList columnErrorCounts = new LongList();
    private final CairoConfiguration configuration;
    private final MemoryMARW ddlMem = Vm.getMARWInstance();
    private final CairoEngine engine;
    private final ObjectPool<OtherToTimestampAdapter> otherToTimestampAdapterPool = new ObjectPool<>(OtherToTimestampAdapter::new, 4);
    private final IntList remapIndex = new IntList();
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private int atomicity;
    private CharSequence designatedTimestampColumnName;
    private int designatedTimestampIndex;
    private CharSequence importedTimestampColumnName;
    private int maxUncommittedRows = -1;
    private RecordMetadata metadata;
    private long o3MaxLag = -1;
    private boolean overwrite;
    private int partitionBy;
    private long size;
    private CharSequence tableName;
    private TimestampAdapter timestampAdapter;
    private int timestampIndex = NO_INDEX;
    private ObjList<TypeAdapter> types;
    private int warnings;
    private TableWriter writer;
    private final CsvTextLexer.Listener nonPartitionedListener = this::onFieldsNonPartitioned;
    private final CsvTextLexer.Listener partitionedListener = this::onFieldsPartitioned;

    public CairoTextWriter(CairoEngine engine) {
        this.engine = engine;
        this.configuration = engine.getConfiguration();
    }

    @Override
    public void clear() {
        otherToTimestampAdapterPool.clear();
        writer = Misc.free(writer);
        metadata = null;
        columnErrorCounts.clear();
        timestampAdapter = null;
        size = 0;
        warnings = TextLoadWarning.NONE;
        designatedTimestampColumnName = null;
        designatedTimestampIndex = NO_INDEX;
        timestampIndex = NO_INDEX;
        importedTimestampColumnName = null;
        remapIndex.clear();
    }

    @Override
    public void close() {
        clear();
        ddlMem.close();
    }

    public void closeWriter() {
        writer = Misc.free(writer);
        metadata = null;
    }

    public void commit() {
        if (writer != null) {
            writer.commit();
        }
    }

    public LongList getColumnErrorCounts() {
        return columnErrorCounts;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public CharSequence getTableName() {
        return tableName;
    }

    public CsvTextLexer.Listener getTextListener() {
        return timestampAdapter != null ? partitionedListener : nonPartitionedListener;
    }

    public CharSequence getTimestampCol() {
        return designatedTimestampColumnName;
    }

    public int getWarnings() {
        return warnings;
    }

    public long getWrittenLineCount() {
        return writer == null ? 0 : writer.size() - size;
    }

    public void of(
            CharSequence name,
            boolean overwrite,
            int atomicity,
            int partitionBy,
            CharSequence timestampColumn
    ) {
        this.tableName = name;
        this.overwrite = overwrite;
        this.atomicity = atomicity;
        this.partitionBy = partitionBy;
        this.importedTimestampColumnName = timestampColumn;
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
                if (onField(line, dbcs, w, i)) {
                    return;
                }
            }
            w.append();
            checkUncommittedRowCount();
        } catch (Exception e) {
            logError(line, timestampIndex, dbcs);
        }
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    private void checkUncommittedRowCount() {
        if (writer != null && maxUncommittedRows > 0 && writer.getO3RowCount() >= maxUncommittedRows) {
            writer.ic(this.o3MaxLag);
        }
    }

    private TableToken createTable(
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> detectedTypes,
            SecurityContext securityContext,
            Path path
    ) throws TextException {
        TableToken tableToken = engine.createTable(
                securityContext,
                ddlMem,
                path,
                false,
                tableStructureAdapter.of(names, detectedTypes),
                false
        );
        this.types = detectedTypes;
        return tableToken;
    }

    private void initWriterAndOverrideImportTypes(
            TableToken tableToken,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> detectedTypes,
            TypeManager typeManager
    ) {
        final TableWriter writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
        final RecordMetadata metadata = GenericRecordMetadata.copyDense(writer.getMetadata());

        // Now, compare column count. Cannot continue if different.

        if (metadata.getColumnCount() < detectedTypes.size()) {
            writer.close();
            throw CairoException.nonCritical()
                    .put("column count mismatch [textColumnCount=").put(detectedTypes.size())
                    .put(", tableColumnCount=").put(metadata.getColumnCount())
                    .put(", table=").put(tableName)
                    .put(']');
        }

        this.types = detectedTypes;

        // Overwrite detected types with actual table column types.
        remapIndex.ensureCapacity(types.size());
        for (int i = 0, n = types.size(); i < n; i++) {
            final int columnIndex = metadata.getColumnIndexQuiet(names.getQuick(i));
            final int idx = columnIndex > -1 ? columnIndex : i; // check for strict match ?
            remapIndex.set(i, metadata.getWriterIndex(idx));

            final int columnType = metadata.getColumnType(idx);
            final TypeAdapter detectedAdapter = types.getQuick(i);
            final int detectedType = detectedAdapter.getType();
            if (detectedType != columnType) {
                // when DATE type is mis-detected as STRING we
                // would not have either date format nor locale to
                // use when populating this field
                switch (ColumnType.tagOf(columnType)) {
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
                        throw CairoException.nonCritical().put("cannot import text into BINARY column [index=").put(i).put(']');
                    default:
                        this.types.setQuick(i, typeManager.getTypeAdapter(columnType));
                        break;
                }
            }
        }

        this.writer = writer;
        this.metadata = metadata;
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
            final int tableIndex = remapIndex.size() > 0 ? remapIndex.get(i) : i;
            types.getQuick(i).write(w, tableIndex, dbcs);
        } catch (Exception ignore) {
            logError(line, i, dbcs);
            switch (atomicity) {
                case Atomicity.SKIP_ALL:
                    writer.rollback();
                    throw CairoException.nonCritical().put("bad syntax [line=").put(line).put(", col=").put(i).put(']');
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

    void prepareTable(
            SecurityContext securityContext,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> detectedTypes,
            Path path,
            TypeManager typeManager,
            TimestampAdapter timestampAdapter
    ) throws TextException {
        assert writer == null;

        if (detectedTypes.size() == 0) {
            throw CairoException.nonCritical().put("cannot determine text structure");
        }

        boolean canUpdateMetadata = true;
        TableToken tableToken = engine.getTableTokenIfExists(tableName);

        switch (engine.getTableStatus(path, tableToken)) {
            case TableUtils.TABLE_DOES_NOT_EXIST:
                tableToken = createTable(names, detectedTypes, securityContext, path);
                writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
                metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                designatedTimestampColumnName = writer.getDesignatedTimestampColumnName();
                designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                partitionBy = writer.getPartitionBy();
                break;
            case TableUtils.TABLE_EXISTS:
                if (overwrite) {
                    engine.drop(path, tableToken);
                    tableToken = createTable(names, detectedTypes, securityContext, path);
                    writer = engine.getWriter(tableToken, WRITER_LOCK_REASON);
                    metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                } else {
                    canUpdateMetadata = false;
                    initWriterAndOverrideImportTypes(tableToken, names, detectedTypes, typeManager);
                    designatedTimestampColumnName = writer.getDesignatedTimestampColumnName();
                    designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                    if (importedTimestampColumnName != null &&
                            !Chars.equalsNc(importedTimestampColumnName, designatedTimestampColumnName)) {
                        warnings |= TextLoadWarning.TIMESTAMP_MISMATCH;
                    }
                    if (PartitionBy.isPartitioned(partitionBy) && partitionBy != writer.getPartitionBy()) {
                        warnings |= TextLoadWarning.PARTITION_TYPE_MISMATCH;
                    }
                    partitionBy = writer.getPartitionBy();
                    tableStructureAdapter.of(names, detectedTypes);
                }
                break;
            default:
                throw CairoException.nonCritical().put("name is reserved [table=").put(tableName).put(']');
        }
        if (canUpdateMetadata) {
            if (PartitionBy.isPartitioned(partitionBy)) {
                if (o3MaxLag > -1) {
                    writer.setMetaO3MaxLag(o3MaxLag);
                    LOG.info().$("updating metadata attribute o3MaxLag to ").$(o3MaxLag).$(", table=").utf8(tableName).$();
                }
                if (maxUncommittedRows > -1) {
                    writer.setMetaMaxUncommittedRows(maxUncommittedRows);
                    LOG.info().$("updating metadata attribute maxUncommittedRows to ").$(maxUncommittedRows).$(", table=").utf8(tableName).$();
                }
            }
        } else {
            LOG.info().$("cannot update metadata attributes o3MaxLag and maxUncommittedRows when the table exists and parameter overwrite is false").$();
        }
        size = writer.size();
        columnErrorCounts.seed(writer.getMetadata().getColumnCount(), 0);

        if (timestampIndex != NO_INDEX) {
            if (timestampAdapter != null) {
                this.timestampAdapter = timestampAdapter;
            } else if (ColumnType.isTimestamp(types.getQuick(timestampIndex).getType())) {
                this.timestampAdapter = (TimestampAdapter) types.getQuick(timestampIndex);
            }
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
            return configuration.getIndexValueBlockSize();
        }

        @Override
        public int getMaxUncommittedRows() {
            return configuration.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            return configuration.getO3MaxLag();
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
        public boolean isDedupKey(int columnIndex) {
            return false;
        }

        @Override
        public boolean isIndexed(int columnIndex) {
            return types.getQuick(columnIndex).isIndexed();
        }

        @Override
        public boolean isSequential(int columnIndex) {
            return false;
        }

        @Override
        public boolean isWalEnabled() {
            return configuration.getWalEnabledDefault() && PartitionBy.isPartitioned(partitionBy);
        }

        TableStructureAdapter of(ObjList<CharSequence> names, ObjList<TypeAdapter> types) throws TextException {
            this.names = names;
            this.types = types;

            if (importedTimestampColumnName == null && designatedTimestampColumnName == null) {
                timestampIndex = NO_INDEX;
            } else if (importedTimestampColumnName != null) {
                timestampIndex = names.indexOf(importedTimestampColumnName);
                if (timestampIndex == NO_INDEX) {
                    throw TextException.$("invalid timestamp column '").put(importedTimestampColumnName).put('\'');
                }
            } else {
                timestampIndex = names.indexOf(designatedTimestampColumnName);
                if (timestampIndex == NO_INDEX) {
                    // columns in the imported file may not have headers, then use writer timestamp index
                    timestampIndex = designatedTimestampIndex;
                }
            }

            if (timestampIndex != NO_INDEX) {
                final TypeAdapter timestampAdapter = types.getQuick(timestampIndex);
                final int typeTag = ColumnType.tagOf(timestampAdapter.getType());
                if ((typeTag != ColumnType.LONG && typeTag != ColumnType.TIMESTAMP) || timestampAdapter == BadTimestampAdapter.INSTANCE) {
                    throw TextException.$("not a timestamp '").put(importedTimestampColumnName).put('\'');
                }
            }
            return this;
        }
    }
}
