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
import io.questdb.std.SimpleReadWriteLock;
import org.jetbrains.annotations.NotNull;


// For show tables
// require id, designatedTimestamp, partitionBy, maxUncommittedRows, o3MaxLag, walEnabled, directoryName, dedup


// designated timestamp and partition by are final
// todo: intern column names
public class CairoTable {
    // consider a versioned lock. consider more granular locking
    private final SimpleReadWriteLock tableWideLock;
    private int columnCount;
    private int designatedTimestampIndex;
    // todo: intern, its a column name
    private String designatedTimestampName;
    private String directoryName;
    private boolean isDedup;
    private boolean isSoftLink;
    private long lastMetadataVersion = -1;
    private long maxTimestamp;
    private int maxUncommittedRows;
    private long minTimestamp;
    private long o3MaxLag;
    private String partitionBy;
    private TableToken token;


    public CairoTable() {
        this.tableWideLock = new SimpleReadWriteLock();
    }

    public CairoTable(@NotNull TableToken token) {
        this.token = token;
        this.tableWideLock = new SimpleReadWriteLock();
    }

    public CairoTable(@NotNull TableReader tableReader) {
        this.token = tableReader.getTableToken();
        this.tableWideLock = new SimpleReadWriteLock();
        updateMetadataIfRequired(tableReader);
    }

    public CairoTable(@NotNull TableMetadata tableMetadata) {
        this.token = tableMetadata.getTableToken();
        this.tableWideLock = new SimpleReadWriteLock();
        updateMetadataIfRequired(tableMetadata);
    }

    public static CairoTable newInstanceFromToken(@NotNull TableToken token) {
        return new CairoTable(token);
    }

    public void copyTo(@NotNull CairoTable target) {
        target.tableWideLock.writeLock();
        target.token = token;
        target.columnCount = columnCount;
        target.designatedTimestampIndex = designatedTimestampIndex;
        target.designatedTimestampName = designatedTimestampName;
        target.directoryName = directoryName;
        target.isDedup = isDedup;
        target.isSoftLink = isSoftLink;
        target.lastMetadataVersion = lastMetadataVersion;
        target.maxTimestamp = maxTimestamp;
        target.maxUncommittedRows = maxUncommittedRows;
        target.minTimestamp = minTimestamp;
        target.o3MaxLag = o3MaxLag;
        target.partitionBy = partitionBy;
        target.tableWideLock.writeLock().unlock();
    }

    public long getColumnCount() {
        tableWideLock.readLock().lock();
        final long columnCount = this.columnCount;
        tableWideLock.readLock().unlock();
        return columnCount;
    }

    public int getDesignatedTimestampIndex() {
        tableWideLock.readLock().lock();
        final int designatedTimestampIndex = this.designatedTimestampIndex;
        tableWideLock.readLock().unlock();
        return designatedTimestampIndex;
    }

    public String getDesignatedTimestampName() {
        tableWideLock.readLock().lock();
        final String designatedTimestampName = this.designatedTimestampName;
        tableWideLock.readLock().unlock();
        return designatedTimestampName;
    }

    public String getDesignatedTimestampNameUnsafe() {
        return designatedTimestampName;
    }

    public int getDesignatedTimestampUnsafe() {
        return designatedTimestampIndex;
    }

    public String getDirectoryName() {
        tableWideLock.readLock().lock();
        final String directoryName = this.directoryName;
        tableWideLock.readLock().unlock();
        return directoryName;
    }

    public String getDirectoryNameUnsafe() {
        return token.getDirName();
    }

    public int getId() {
        tableWideLock.readLock().lock();
        final int id = this.token.getTableId();
        tableWideLock.readLock().unlock();
        return id;
    }

    public int getIdUnsafe() {
        return this.token.getTableId();
    }

    public boolean getIsDedup() {
        tableWideLock.readLock().lock();
        final boolean isDedup = this.isDedup;
        tableWideLock.readLock().unlock();
        return isDedup;
    }

    public boolean getIsDedupUnsafe() {
        return isDedup;
    }

    public boolean getIsSoftLink() {
        tableWideLock.readLock().lock();
        final boolean isSoftLink = this.isSoftLink;
        tableWideLock.readLock().unlock();
        return isSoftLink;
    }

    public boolean getIsSoftLinkUnsafe() {
        return isSoftLink;
    }

    public long getMaxTimestamp() {
        tableWideLock.readLock().lock();
        final long maxTimestamp = this.maxTimestamp;
        tableWideLock.readLock().unlock();
        return maxTimestamp;
    }

    public long getMaxTimestampUnsafe() {
        return maxTimestamp;
    }

    public int getMaxUncommittedRows() {
        tableWideLock.readLock().lock();
        final int maxUncommittedRows = this.maxUncommittedRows;
        tableWideLock.readLock().unlock();
        return maxUncommittedRows;
    }

    public int getMaxUncommittedRowsUnsafe() {
        return maxUncommittedRows;
    }

    public long getMinTimestamp() {
        tableWideLock.readLock().lock();
        final long minTimestamp = this.minTimestamp;
        tableWideLock.readLock().unlock();
        return minTimestamp;
    }

    public long getMinTimestampUnsafe() {
        return minTimestamp;
    }

    public @NotNull String getName() {
        tableWideLock.readLock().lock();
        final String name = this.token.getTableName();
        tableWideLock.readLock().unlock();
        return name;
    }

    public @NotNull String getNameUnsafe() {
        return this.token.getTableName();
    }

    public long getO3MaxLag() {
        tableWideLock.readLock().lock();
        final long o3MaxLag = this.o3MaxLag;
        tableWideLock.readLock().unlock();
        return o3MaxLag;
    }

    public long getO3MaxLagUnsafe() {
        return o3MaxLag;
    }

    public String getPartitionBy() {
        tableWideLock.readLock().lock();
        final String partitionBy = this.partitionBy;
        tableWideLock.readLock().unlock();
        return partitionBy;
    }

    public String getPartitionByUnsafe() {
        return partitionBy;
    }

    public boolean getWalEnabled() {
        tableWideLock.readLock().lock();
        final boolean walEnabled = this.token.isWal();
        tableWideLock.readLock().unlock();
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
            tableWideLock.writeLock().lock();
            token = tableReader.getTableToken();
            minTimestamp = tableReader.getMinTimestamp();
            maxTimestamp = tableReader.getMaxTimestamp();
            maxUncommittedRows = tableReader.getMaxUncommittedRows();
            o3MaxLag = tableReader.getO3MaxLag();
            partitionBy = PartitionBy.toString(tableReader.getPartitionedBy()); // intern this
            isSoftLink = tableReader.getMetadata().isSoftLink();
            this.lastMetadataVersion = tableReader.getMetadataVersion();
            designatedTimestampIndex = tableReader.getMetadata().timestampIndex;
            designatedTimestampName = designatedTimestampIndex > -1 ? tableReader.getMetadata().getColumnName(designatedTimestampIndex) : null;
            isDedup = designatedTimestampIndex >= 0 && token.isWal() && tableReader.getMetadata().isDedupKey(designatedTimestampIndex);
            columnCount = tableReader.getColumnCount();
            tableWideLock.writeLock().unlock();
        }
    }

    public void updateMetadataIfRequired(@NotNull TableMetadata tableMetadata) {
        final long lastMetadataVersion = getLastMetadataVersion();
        if (lastMetadataVersion < tableMetadata.getMetadataVersion()) {
            tableWideLock.writeLock().lock();
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
            tableWideLock.writeLock().unlock();
        }
    }

    private long getLastMetadataVersion() {
        tableWideLock.readLock().lock();
        final long lastMetadataVersion = this.lastMetadataVersion;
        tableWideLock.readLock().unlock();
        return lastMetadataVersion;
    }

    private long getLastMetadataVersionUnsafe() {
        return lastMetadataVersion;
    }
    
}
