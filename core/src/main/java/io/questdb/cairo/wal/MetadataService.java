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

package io.questdb.cairo.wal;

import io.questdb.cairo.AttachDetachStatus;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.UpdateOperator;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.std.LongList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface MetadataService {

    /**
     * Adds new column to table, which can be either empty or can have data already. When existing columns
     * already have data this function will create ".top" file in addition to column files. ".top" file contains
     * size of partition at the moment of column creation. It must be used to accurately position inside new
     * column when either appending or reading.
     * <p>
     * <b>Failures</b>
     * Adding new column can fail in many situations. None of the failures affect integrity of data that is already in
     * the table but can leave instance of TableWriter in inconsistent state. When this happens function will throw CairoError.
     * Calling code must close TableWriter instance and open another when problems are rectified. Those problems would be
     * either with disk or memory or both.
     * <p>
     * Whenever function throws CairoException application code can continue using TableWriter instance and may attempt to
     * add columns again.
     * <p>
     * <b>Transactions</b>
     * <p>
     * Pending transaction will be committed before function attempts to add column. Even when function is unsuccessful it may
     * still have committed transaction.
     *
     * @param columnName              of column either ASCII or UTF8 encoded.
     * @param symbolCapacity          when column type is SYMBOL this parameter specifies approximate capacity for symbol map.
     *                                It should be equal to number of unique symbol values stored in the table and getting this
     *                                value badly wrong will cause performance degradation. Must be power of 2
     * @param symbolCacheFlag         when set to true, symbol values will be cached on Java heap.
     * @param columnType              {@link ColumnType}
     * @param isIndexed               configures column to be indexed or not
     * @param indexValueBlockCapacity approximation of number of rows for single index key, must be power of 2
     * @param isSequential            for columns that contain sequential values query optimiser can make assumptions on range searches (future feature)
     * @param isDedupKey              when set to true, column will be used as deduplication key
     */
    void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            boolean isDedupKey,
            SecurityContext securityContext
    );

    default void addColumn(
            CharSequence columnName,
            int columnType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            boolean isDedupKey
    ) {
        addColumn(
                columnName,
                columnType,
                symbolCapacity,
                symbolCacheFlag,
                isIndexed,
                indexValueBlockCapacity,
                isSequential,
                isDedupKey,
                null
        );
    }

    void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize);

    AttachDetachStatus attachPartition(long partitionTimestamp);

    void changeCacheFlag(int columnIndex, boolean isCacheOn);

    void changeColumnType(
            CharSequence columnName,
            int newType,
            int symbolCapacity,
            boolean symbolCacheFlag,
            boolean isIndexed,
            int indexValueBlockCapacity,
            boolean isSequential,
            SecurityContext securityContext
    );

    void changeSymbolCapacity(
            CharSequence columnName,
            int symbolCapacity,
            SecurityContext securityContext
    );

    boolean convertPartitionNativeToParquet(long partitionTimestamp);

    boolean convertPartitionParquetToNative(long partitionTimestamp);

    AttachDetachStatus detachPartition(long partitionTimestamp);

    void disableDeduplication();

    void dropIndex(@NotNull CharSequence columnName);

    /**
     * Enables deduplication with the given upsert keys.
     *
     * @return returns true when dedup was already enabled on the table and the new upsert keys
     * are a subset of the previous upsert keys. Implementations that don't have access
     * to the table metadata always return false.
     */
    boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes);

    void forceRemovePartitions(LongList partitionTimestamps);

    int getMetaMaxUncommittedRows();

    TableRecordMetadata getMetadata();

    int getPartitionBy();

    TableToken getTableToken();

    int getTimestampType();

    UpdateOperator getUpdateOperator();

    void removeColumn(@NotNull CharSequence columnName);

    boolean removePartition(long partitionTimestamp);

    default void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName) {
        renameColumn(columnName, newName, null);
    }

    void renameColumn(@NotNull CharSequence columnName, @NotNull CharSequence newName, SecurityContext securityContext);

    void renameTable(@NotNull CharSequence fromNameTable, @NotNull CharSequence toTableName);

    /**
     * Sets refresh type and settings for materialized view.
     */
    void setMatViewRefresh(
            int refreshType,
            int timerInterval,
            char timerUnit,
            long timerStartUs,
            @Nullable CharSequence timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    );

    /**
     * Sets the incremental refresh limit for materialized view:
     * if positive, it's in hours;
     * if negative, it's in months (and the actual value is positive);
     * zero means "no refresh limit".
     */
    void setMatViewRefreshLimit(int limitHoursOrMonths);

    /**
     * Sets incremental refresh timer values for materialized view.
     */
    void setMatViewRefreshTimer(long startUs, int interval, char unit);

    void setMetaMaxUncommittedRows(int maxUncommittedRows);

    void setMetaO3MaxLag(long o3MaxLagUs);

    /**
     * Sets the time-to-live (TTL) of the data in this table:
     * if positive, it's in hours;
     * if negative, it's in months (and the actual value is positive);
     * zero means "no TTL".
     */
    void setMetaTtl(int ttlHoursOrMonths);

    void squashPartitions();

    void tick();
}
