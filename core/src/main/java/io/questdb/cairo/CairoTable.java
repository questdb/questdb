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

package io.questdb.cairo;

import io.questdb.cairo.sql.TableMetadata;
import io.questdb.std.CairoColumn;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.SimpleReadWriteLock;
import org.jetbrains.annotations.NotNull;


// For show tables
// require id, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup


// designated timestamp and partition by are final
// todo: intern column names
public class CairoTable {
    private final CharSequenceObjHashMap<CairoColumn> columns = new CharSequenceObjHashMap<>();
    // consider a versioned lock. consider more granular locking
    private final SimpleReadWriteLock lock;
    private int columnCount;
    private int designatedTimestampIndex;
    // todo: intern, its a column name
    private String designatedTimestampName;
    private String directoryName;
    private boolean isDedup;
    private boolean isSoftLink;
    private long lastMetadataVersion = -1;
    private int maxUncommittedRows;
    private long o3MaxLag;
    private String partitionBy;
    private TableToken token;


    public CairoTable() {
        this.lock = new SimpleReadWriteLock();
    }

    public CairoTable(@NotNull TableToken token) {
        this.token = token;
        this.lock = new SimpleReadWriteLock();
    }

    public CairoTable(@NotNull TableReader tableReader) {
        this.token = tableReader.getTableToken();
        this.lock = new SimpleReadWriteLock();
        updateMetadataIfRequired(tableReader);
    }

    public CairoTable(@NotNull TableMetadata tableMetadata) {
        this.token = tableMetadata.getTableToken();
        this.lock = new SimpleReadWriteLock();
        updateMetadataIfRequired(tableMetadata);
    }

    public static CairoTable newInstanceFromToken(@NotNull TableToken token) {
        return new CairoTable(token);
    }

    // fails if table already exists
    public void addColumn(@NotNull CairoColumn newColumn) {
        lock.writeLock().lock();
        final CharSequence columnName = newColumn.getName();
        final CairoColumn existingColumn = columns.get(columnName);
        if (existingColumn != null) {
            throw CairoException.nonCritical().put("table [name=").put(columnName).put("] already exists in CairoMetadata");
        }
        columns.put(columnName, newColumn);
        lock.writeLock().unlock();
    }

    // fails if table already exists
    public void addColumnUnsafe(@NotNull CairoColumn newColumn) {
        final CharSequence columnName = newColumn.getName();
        final CairoColumn existingColumn = columns.get(columnName);
        if (existingColumn != null) {
            throw CairoException.nonCritical().put("table [name=").put(columnName).put("] already exists in CairoMetadata");
        }
        columns.put(columnName, newColumn);
    }


    public void copyTo(@NotNull CairoTable target) {
        lock.readLock().lock();
        target.lock.writeLock().lock();

        target.token = token;
        target.columnCount = columnCount;
        target.designatedTimestampIndex = designatedTimestampIndex;
        target.designatedTimestampName = designatedTimestampName;
        target.directoryName = directoryName;
        target.isDedup = isDedup;
        target.isSoftLink = isSoftLink;
        target.lastMetadataVersion = lastMetadataVersion;
        target.maxUncommittedRows = maxUncommittedRows;
        target.o3MaxLag = o3MaxLag;
        target.partitionBy = partitionBy;

        target.lock.writeLock().unlock();
        lock.readLock().unlock();
    }

    public long getColumnCount() {
        lock.readLock().lock();
        final long columnCount = this.columnCount;
        lock.readLock().unlock();
        return columnCount;
    }

    public CairoColumn getColumnQuick(@NotNull CharSequence columnName) {
        final CairoColumn col = getColumnQuiet(columnName);
        if (col == null) {
            throw CairoException.tableDoesNotExist(columnName);
        }
        return col;
    }

    public CairoColumn getColumnQuiet(@NotNull CharSequence columnName) {
        lock.readLock().lock();
        final CairoColumn col = columns.get(columnName);
        lock.readLock().unlock();
        return col;
    }

    public int getDesignatedTimestampIndex() {
        lock.readLock().lock();
        final int designatedTimestampIndex = this.designatedTimestampIndex;
        lock.readLock().unlock();
        return designatedTimestampIndex;
    }

    public String getDesignatedTimestampName() {
        lock.readLock().lock();
        final String designatedTimestampName = this.designatedTimestampName;
        lock.readLock().unlock();
        return designatedTimestampName;
    }

    public String getDesignatedTimestampNameUnsafe() {
        return designatedTimestampName;
    }

    public int getDesignatedTimestampUnsafe() {
        return designatedTimestampIndex;
    }

    public String getDirectoryName() {
        lock.readLock().lock();
        final String directoryName = this.directoryName;
        lock.readLock().unlock();
        return directoryName;
    }

    public String getDirectoryNameUnsafe() {
        return token.getDirName();
    }

    public int getId() {
        lock.readLock().lock();
        final int id = this.token.getTableId();
        lock.readLock().unlock();
        return id;
    }

    public int getIdUnsafe() {
        return this.token.getTableId();
    }

    public boolean getIsDedup() {
        lock.readLock().lock();
        final boolean isDedup = this.isDedup;
        lock.readLock().unlock();
        return isDedup;
    }

    public boolean getIsDedupUnsafe() {
        return isDedup;
    }

    public boolean getIsSoftLink() {
        lock.readLock().lock();
        final boolean isSoftLink = this.isSoftLink;
        lock.readLock().unlock();
        return isSoftLink;
    }

    public boolean getIsSoftLinkUnsafe() {
        return isSoftLink;
    }

    public int getMaxUncommittedRows() {
        lock.readLock().lock();
        final int maxUncommittedRows = this.maxUncommittedRows;
        lock.readLock().unlock();
        return maxUncommittedRows;
    }

    public int getMaxUncommittedRowsUnsafe() {
        return maxUncommittedRows;
    }

    public @NotNull String getName() {
        lock.readLock().lock();
        final String name = this.token.getTableName();
        lock.readLock().unlock();
        return name;
    }

    public @NotNull String getNameUnsafe() {
        return this.token.getTableName();
    }

    public long getO3MaxLag() {
        lock.readLock().lock();
        final long o3MaxLag = this.o3MaxLag;
        lock.readLock().unlock();
        return o3MaxLag;
    }

    public long getO3MaxLagUnsafe() {
        return o3MaxLag;
    }

    public String getPartitionBy() {
        lock.readLock().lock();
        final String partitionBy = this.partitionBy;
        lock.readLock().unlock();
        return partitionBy;
    }

    public String getPartitionByUnsafe() {
        return partitionBy;
    }

    public boolean getWalEnabled() {
        lock.readLock().lock();
        final boolean walEnabled = this.token.isWal();
        lock.readLock().unlock();
        return walEnabled;
    }

    public boolean getWalEnabledUnsafe() {
        return token.isWal();
    }

    public boolean isInitialised() {
        return getLastMetadataVersion() != -1;
    }

    public void updateMetadataIfRequired(@NotNull TableReader tableReader) {
        final long lastMetadataVersion = getLastMetadataVersion();
        if (lastMetadataVersion < tableReader.getMetadataVersion()) {
            updateMetadataIfRequired(tableReader, tableReader.getMetadata());
        }
    }

    public void updateMetadataIfRequired(@NotNull TableReader tableReader, @NotNull TableMetadata tableMetadata) {
        final long lastMetadataVersion = getLastMetadataVersion();
        if (lastMetadataVersion < tableReader.getMetadataVersion()) {
            updateMetadataIfRequired(tableReader.getMetadata());
        }
    }

    public void updateMetadataIfRequired(@NotNull TableMetadata tableMetadata) {
        final long lastMetadataVersion = getLastMetadataVersion();
        if (lastMetadataVersion < tableMetadata.getMetadataVersion()) {
            lock.writeLock().lock();

            token = tableMetadata.getTableToken();
            maxUncommittedRows = tableMetadata.getMaxUncommittedRows();
            o3MaxLag = tableMetadata.getO3MaxLag();
            partitionBy = PartitionBy.toString(tableMetadata.getPartitionBy()); // intern this
            isSoftLink = tableMetadata.isSoftLink();
            this.lastMetadataVersion = tableMetadata.getMetadataVersion();
            designatedTimestampIndex = tableMetadata.getTimestampIndex();
            designatedTimestampName = designatedTimestampIndex > -1 ? tableMetadata.getColumnName(designatedTimestampIndex) : null;
            isDedup = designatedTimestampIndex >= 0 && token.isWal() && tableMetadata.isDedupKey(designatedTimestampIndex);
            columnCount = tableMetadata.getColumnCount();

            // now handle columns

            for (int position = 0; position < columnCount; position++) {
                final TableColumnMetadata columnMetadata = tableMetadata.getColumnMetadata(position);
                upsertColumnUnsafe(tableMetadata, columnMetadata);
                // symbols tba
//                cairoColumn.updateMetadata(columnMetadata, isDesignated, position);
//                if (ColumnType.isSymbol(columnMetadata.getType())) {
//                    final SymbolMapReader symbolReader = tableMetadata
//                }
//                if (col == N_SYMBOL_CACHED_COL) {
//                    if (ColumnType.isSymbol(reader.getMetadata().getColumnType(columnIndex))) {
//                        return reader.getSymbolMapReader(columnIndex).isCached();
//                    } else {
//                        return false;
//                    }
//                }
            }

            lock.writeLock().unlock();
        }
    }

    public void upsertColumn(@NotNull TableMetadata tableMetadata, @NotNull TableColumnMetadata columnMetadata) {
        CairoColumn col = getColumnQuiet(columnMetadata.getName());
        final int position = tableMetadata.getColumnIndex(columnMetadata.getName());
        final boolean designated = position == designatedTimestampIndex;
        if (col == null) {
            col = new CairoColumn(columnMetadata, designated, position);
            addColumn(col);
        } else {
            col.updateMetadata(columnMetadata, designated, position);
        }
    }

    public void upsertColumnUnsafe(@NotNull TableMetadata tableMetadata, @NotNull TableColumnMetadata columnMetadata) {
        CairoColumn col = columns.get(columnMetadata.getName());
        final int position = tableMetadata.getColumnIndex(columnMetadata.getName());
        final boolean designated = position == designatedTimestampIndex;
        if (col == null) {
            col = new CairoColumn(columnMetadata, designated, position);
            addColumnUnsafe(col);
        } else {
            col.updateMetadata(columnMetadata, designated, position);
        }
    }

    private long getLastMetadataVersion() {
        lock.readLock().lock();
        final long lastMetadataVersion = this.lastMetadataVersion;
        lock.readLock().unlock();
        return lastMetadataVersion;
    }

    private long getLastMetadataVersionUnsafe() {
        return lastMetadataVersion;
    }
}
