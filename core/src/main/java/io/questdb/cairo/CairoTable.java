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

import io.questdb.std.IntList;
import io.questdb.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.std.ObjList;
import io.questdb.std.SimpleReadWriteLock;
import org.jetbrains.annotations.NotNull;


// designated timestamp and partition by are final
// todo: intern column names
// todo: update this to use column order map, column name index map etc.
public class CairoTable {
    // consider a versioned lock. consider more granular locking
    public final SimpleReadWriteLock lock;
    public LowerCaseCharSequenceIntHashMap columnNameIndexMap = new LowerCaseCharSequenceIntHashMap();
    public IntList columnOrderMap = new IntList();
    public ObjList<CairoColumn> columns = new ObjList<>();
    // todo: intern, its a column name
    private boolean isDedup;
    private boolean isSoftLink;
    private long lastMetadataVersion = -1;
    private int maxUncommittedRows;
    private long o3MaxLag;
    private String partitionBy;
    private int timestampIndex;
    private TableToken token;

    public CairoTable() {
        this.lock = new SimpleReadWriteLock();
    }

    public CairoTable(@NotNull TableToken token) {
        this.token = token;
        this.lock = new SimpleReadWriteLock();
    }

    public void addColumnUnsafe(@NotNull CairoColumn newColumn) throws CairoException {
        final CharSequence columnName = newColumn.getNameUnsafe();
        final CairoColumn existingColumn = getColumnQuietUnsafe(columnName);
        if (existingColumn != null) {
            throw CairoException.nonCritical().put("column already exists in table [table=").put(getNameUnsafe()).put(", column=").put(columnName).put("]");
        }
        columns.add(newColumn);

        final int denseIndex = columns.size() - 1;
        columnNameIndexMap.put(columnName, denseIndex);
    }

    public void copyTo(@NotNull CairoTable target) {
        lock.readLock().lock();
        target.lock.writeLock().lock();

        target.columns = columns;
        target.columnOrderMap = columnOrderMap;
        target.columnNameIndexMap = columnNameIndexMap;
        target.token = token;
        target.timestampIndex = timestampIndex;
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
        final long columnCount = this.columns.size();
        lock.readLock().unlock();
        return columnCount;
    }

    public ObjList<CharSequence> getColumnNames() {
        lock.readLock().lock();
        final ObjList<CharSequence> names = this.columnNameIndexMap.keys();
        lock.readLock().unlock();
        return names;
    }

    public CairoColumn getColumnQuick(@NotNull CharSequence columnName) {
        final CairoColumn col = getColumnQuiet(columnName);
        if (col == null) {
            throw CairoException.tableDoesNotExist(columnName);
        }
        return col;
    }

    public CairoColumn getColumnQuickUnsafe(@NotNull CharSequence columnName) {
        final CairoColumn col = getColumnQuietUnsafe(columnName);
        if (col == null) {
            throw CairoException.tableDoesNotExist(columnName);
        }
        return col;
    }

    public CairoColumn getColumnQuiet(@NotNull CharSequence columnName) {
        lock.readLock().lock();
        final CairoColumn col = getColumnQuietUnsafe(columnName);
        lock.readLock().unlock();
        return col;
    }

    public CairoColumn getColumnQuietUnsafe(@NotNull CharSequence columnName) {
        final int index = columnNameIndexMap.get(columnName);
        if (index != -1) {
            return columns.getQuiet(index);
        } else {
            return null;
        }
    }

    public String getDirectoryName() {
        lock.readLock().lock();
        final String directoryName = this.token.getDirName();
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

    public long getLastMetadataVersion() {
        lock.readLock().lock();
        final long lastMetadataVersion = this.lastMetadataVersion;
        lock.readLock().unlock();
        return lastMetadataVersion;
    }

    public long getLastMetadataVersionUnsafe() {
        return lastMetadataVersion;
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

    public int getTimestampIndex() {
        lock.readLock().lock();
        final int timestampIndex = this.timestampIndex;
        lock.readLock().unlock();
        return timestampIndex;
    }

    public int getTimestampIndexUnsafe() {
        return timestampIndex;
    }

    public CharSequence getTimestampName() {
        lock.readLock().lock();
        final CharSequence timestampName = getTimestampNameUnsafe();
        lock.readLock().unlock();
        return timestampName;
    }

    public CharSequence getTimestampNameUnsafe() {
        final CairoColumn timestampColumn = getColumnQuietUnsafe(this.timestampIndex);
        if (timestampColumn != null) {
            return timestampColumn.getNameUnsafe();
        } else {
            return null;
        }
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

    public void setIsDedupUnsafe(boolean isDedup) {
        this.isDedup = isDedup;
    }

    public void setIsSoftLinkUnsafe(boolean isSoftLink) {
        this.isSoftLink = isSoftLink;
    }

    public void setLastMetadataVersionUnsafe(long lastMetadataVersion) {
        this.lastMetadataVersion = lastMetadataVersion;
    }

    public void setMaxUncommittedRowsUnsafe(int maxUncommittedRows) {
        this.maxUncommittedRows = maxUncommittedRows;
    }

    public void setO3MaxLagUnsafe(long o3MaxLag) {
        this.o3MaxLag = o3MaxLag;
    }

    public void setPartitionByUnsafe(String partitionBy) {
        this.partitionBy = partitionBy;
    }

    public void setTimestampIndexUnsafe(int timestampIndex) {
        this.timestampIndex = timestampIndex;
    }

    private CairoColumn getColumnQuickUnsafe(int position) {
        return columns.getQuick(position);
    }

    private CairoColumn getColumnQuietUnsafe(int position) {
        if (position > -1) {
            return columns.getQuiet(position);
        } else {
            return null;
        }
    }
}
