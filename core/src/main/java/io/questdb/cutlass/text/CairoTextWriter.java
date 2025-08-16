/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableStructure;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMARW;
import io.questdb.cutlass.text.types.BadDateAdapter;
import io.questdb.cutlass.text.types.BadTimestampAdapter;
import io.questdb.cutlass.text.types.OtherToTimestampAdapter;
import io.questdb.cutlass.text.types.TimestampAdapter;
import io.questdb.cutlass.text.types.TimestampCompatibleAdapter;
import io.questdb.cutlass.text.types.TypeAdapter;
import io.questdb.cutlass.text.types.TypeManager;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Path;

import java.io.Closeable;

public class CairoTextWriter implements Closeable, Mutable {
    public static final int NO_INDEX = -1;
    private static final Log LOG = LogFactory.getLog(CairoTextWriter.class);
    private static final String WRITER_LOCK_REASON = "textWriter";
    private final LongList columnErrorCounts = new LongList();
    private final CairoConfiguration configuration;
    private final MemoryMARW ddlMem = Vm.getCMARWInstance();
    private final CairoEngine engine;
    private final ObjectPool<OtherToTimestampAdapter> otherToTimestampAdapterPool = new ObjectPool<>(OtherToTimestampAdapter::new, 4);
    private final IntList remapIndex = new IntList();
    private final TableStructureAdapter tableStructureAdapter = new TableStructureAdapter();
    private int atomicity;
    private boolean create = true;
    private CharSequence designatedTimestampColumnName;
    private int designatedTimestampIndex;
    private CharSequence importedTimestampColumnName;
    private int maxUncommittedRows = -1;
    private RecordMetadata metadata;
    private long o3MaxLag = -1;
    private boolean overwrite;
    private int partitionBy;
    private CharSequence tableName;
    private TimestampAdapter timestampAdapter;
    private int timestampIndex = NO_INDEX;
    private ObjList<TypeAdapter> types;
    private int warnings;
    private TableWriterAPI writer;
    private int writtenLineCount;
    private final CsvTextLexer.Listener partitionedListener = this::onFieldsPartitioned;
    private final CsvTextLexer.Listener nonPartitionedListener = this::onFieldsNonPartitioned;

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
        writtenLineCount = 0;
        warnings = TextLoadWarning.NONE;
        designatedTimestampColumnName = null;
        designatedTimestampIndex = NO_INDEX;
        timestampIndex = NO_INDEX;
        importedTimestampColumnName = null;
        maxUncommittedRows = -1;
        o3MaxLag = -1;
        remapIndex.clear();
        create = true;
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

    public boolean getCreate() {
        return create;
    }

    public int getMaxUncommittedRows() {
        return maxUncommittedRows;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public long getO3MaxLag() {
        return o3MaxLag;
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
        return writtenLineCount;
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

    public void onFieldsNonPartitioned(long line, ObjList<DirectUtf8String> values, int valuesLength) {
        final TableWriter.Row w = writer.newRow();
        for (int i = 0; i < valuesLength; i++) {
            final DirectUtf8String dus = values.getQuick(i);
            if (dus.size() == 0) {
                continue;
            }
            if (onField(line, dus, w, i)) {
                return;
            }
        }
        w.append();
        writtenLineCount++;
    }

    public void onFieldsPartitioned(long line, ObjList<DirectUtf8String> values, int valuesLength) {
        final int timestampIndex = this.timestampIndex;
        DirectUtf8String dus = values.getQuick(timestampIndex);
        try {
            final TableWriter.Row w = writer.newRow(timestampAdapter.getTimestamp(dus));
            for (int i = 0; i < valuesLength; i++) {
                dus = values.getQuick(i);
                if (i == timestampIndex || dus.size() == 0) {
                    continue;
                }
                if (onField(line, dus, w, i)) {
                    return;
                }
            }
            w.append();
            writtenLineCount++;
            checkUncommittedRowCount();
        } catch (Exception e) {
            logError(line, timestampIndex, dus);
        }
    }

    public void setCreate(boolean create) {
        this.create = create;
    }

    public void setMaxUncommittedRows(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    public void setO3MaxLag(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    private void checkUncommittedRowCount() {
        if (writer != null && maxUncommittedRows > 0 && writer.getUncommittedRowCount() >= maxUncommittedRows) {
            writer.ic(o3MaxLag);
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

    private CharSequence getDesignatedTimestampColumnName(RecordMetadata metadata) {
        return metadata.getTimestampIndex() > -1 ? metadata.getColumnName(metadata.getTimestampIndex()) : null;
    }

    private void initWriterAndOverrideImportTypes(
            TableToken tableToken,
            ObjList<CharSequence> names,
            ObjList<TypeAdapter> detectedTypes,
            TypeManager typeManager
    ) {
        final TableWriterAPI writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
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
        remapIndex.setPos(types.size());
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
                        // different timestamp type
                        if (detectedAdapter instanceof TimestampAdapter) {
                            ((TimestampAdapter) detectedAdapter).reCompileDateFormat(ColumnType.getTimestampDriver(columnType).getTimestampDateFormatFactory());
                        } else if (detectedAdapter instanceof TimestampCompatibleAdapter) {
                            types.setQuick(i, otherToTimestampAdapterPool.next().of((TimestampCompatibleAdapter) detectedAdapter, columnType));
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

    private void logError(long line, int i, DirectUtf8Sequence dus) {
        LogRecord logRecord = LOG.error().$("type syntax [type=").$(ColumnType.nameOf(types.getQuick(i).getType())).$("]\n\t");
        logRecord.$('[').$(line).$(':').$(i).$("] -> ").$(dus).$();
        columnErrorCounts.increment(i);
    }

    private void logTypeError(int i) {
        LOG.info()
                .$("mis-detected [table=").$safe(tableName)
                .$(", column=").$(i)
                .$(", type=").$(ColumnType.nameOf(types.getQuick(i).getType()))
                .$(']').$();
    }

    private boolean onField(long line, DirectUtf8Sequence dus, TableWriter.Row w, int i) {
        try {
            final int tableIndex = remapIndex.size() > 0 ? remapIndex.get(i) : i;
            types.getQuick(i).write(w, tableIndex, dus);
        } catch (Exception ignore) {
            logError(line, i, dus);
            switch (atomicity) {
                case Atomicity.SKIP_ALL:
                    w.cancel();
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

        TableToken tableToken;
        boolean tableReCreated = false;
        switch (engine.getTableStatus(path, tableName)) {
            case TableUtils.TABLE_DOES_NOT_EXIST:
                if (!create) {
                    throw CairoException.tableDoesNotExist(tableName).put(" and create param was set to false.");
                }
                tableToken = createTable(names, detectedTypes, securityContext, path);
                tableReCreated = true;
                writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
                metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                designatedTimestampColumnName = getDesignatedTimestampColumnName(metadata);
                designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                break;
            case TableUtils.TABLE_EXISTS:
                tableToken = engine.getTableTokenIfExists(tableName);
                if (tableToken != null && tableToken.isMatView()) {
                    throw CairoException.nonCritical()
                            .put("cannot modify materialized view [view=")
                            .put(tableToken.getTableName())
                            .put(']');
                }
                if (overwrite) {
                    securityContext.authorizeTableDrop(tableToken);
                    engine.dropTableOrMatView(path, tableToken);
                    tableToken = createTable(names, detectedTypes, securityContext, path);
                    tableReCreated = true;
                    writer = engine.getTableWriterAPI(tableToken, WRITER_LOCK_REASON);
                    metadata = GenericRecordMetadata.copyDense(writer.getMetadata());
                } else {
                    initWriterAndOverrideImportTypes(tableToken, names, detectedTypes, typeManager);
                    designatedTimestampIndex = writer.getMetadata().getTimestampIndex();
                    designatedTimestampColumnName = getDesignatedTimestampColumnName(writer.getMetadata());
                    if (importedTimestampColumnName != null
                            && !Chars.equalsNc(importedTimestampColumnName, designatedTimestampColumnName)) {
                        warnings |= TextLoadWarning.TIMESTAMP_MISMATCH;
                    }
                    int tablePartitionBy = TableUtils.getPartitionBy(writer.getMetadata(), engine);
                    if (PartitionBy.isPartitioned(partitionBy) && partitionBy != tablePartitionBy) {
                        warnings |= TextLoadWarning.PARTITION_TYPE_MISMATCH;
                    }
                    partitionBy = tablePartitionBy;
                    tableStructureAdapter.of(names, detectedTypes);
                    securityContext.authorizeInsert(tableToken);
                }
                break;
            default:
                throw CairoException.nonCritical().put("name is reserved [table=").put(tableName).put(']');
        }
        if (!tableReCreated && (o3MaxLag > -1 || maxUncommittedRows > -1)) {
            LOG.info().$("cannot update metadata attributes o3MaxLag and maxUncommittedRows when the table exists and parameter overwrite is false").$();
        }
        if (PartitionBy.isPartitioned(partitionBy)) {
            // We want to limit memory consumption during the import, so make sure
            // to use table's maxUncommittedRows and o3MaxLag if they're not set.
            if (o3MaxLag == -1 && !writer.getMetadata().isWalEnabled()) {
                o3MaxLag = TableUtils.getO3MaxLag(writer.getMetadata(), engine);
                LOG.info().$("using table's o3MaxLag ").$(o3MaxLag).$(", table=").$safe(tableName).$();
            }
            if (maxUncommittedRows == -1) {
                maxUncommittedRows = TableUtils.getMaxUncommittedRows(writer.getMetadata(), engine);
                LOG.info().$("using table's maxUncommittedRows ").$(maxUncommittedRows).$(", table=").$safe(tableName).$();
            }
        }
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
            return maxUncommittedRows > -1 && PartitionBy.isPartitioned(partitionBy) ? maxUncommittedRows : configuration.getMaxUncommittedRows();
        }

        @Override
        public long getO3MaxLag() {
            return o3MaxLag > -1 && PartitionBy.isPartitioned(partitionBy) ? o3MaxLag : configuration.getO3MaxLag();
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
