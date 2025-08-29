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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.AttachDetachStatus;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.UpdateOperator;
import io.questdb.cairo.wal.MetadataService;
import io.questdb.std.LongList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface MetadataServiceStub extends MetadataService {

    @Override
    default void addIndex(@NotNull CharSequence columnName, int indexValueBlockSize) {
        throw CairoException.critical(0).put("add index does not update sequencer metadata");
    }

    @Override
    default AttachDetachStatus attachPartition(long partitionTimestamp) {
        throw CairoException.critical(0).put("attach partition does not update sequencer metadata");
    }

    @Override
    default void changeCacheFlag(int columnIndex, boolean isCacheOn) {
        throw CairoException.critical(0).put("change cache flag does not update sequencer metadata");
    }

    @Override
    default void changeSymbolCapacity(CharSequence columnName, int symbolCapacity, SecurityContext securityContext) {
        throw CairoException.critical(0).put("change symbol capacity does not update sequencer metadata");
    }

    @Override
    default boolean convertPartitionNativeToParquet(long partitionTimestamp) {
        throw CairoException.critical(0).put("convert native partition to parquet does not update sequencer metadata");
    }

    @Override
    default boolean convertPartitionParquetToNative(long partitionTimestamp) {
        throw CairoException.critical(0).put("convert parquet partition to native does not update sequencer metadata");
    }

    @Override
    default AttachDetachStatus detachPartition(long partitionTimestamp) {
        throw CairoException.critical(0).put("detach partition does not update sequencer metadata");
    }

    @Override
    default void disableDeduplication() {
    }

    @Override
    default void dropIndex(@NotNull CharSequence columnName) {
        throw CairoException.critical(0).put("drop index does not update sequencer metadata");
    }

    @Override
    default boolean enableDeduplicationWithUpsertKeys(LongList columnsIndexes) {
        return false;
    }

    default void forceRemovePartitions(LongList partitionTimestamps) {
        throw CairoException.critical(0).put("recover partitions does not update sequencer metadata");
    }

    @Override
    default int getMetaMaxUncommittedRows() {
        throw new UnsupportedOperationException();
    }

    @Override
    default int getPartitionBy() {
        throw new UnsupportedOperationException();
    }

    @Override
    default UpdateOperator getUpdateOperator() {
        throw new UnsupportedOperationException();
    }

    @Override
    default boolean removePartition(long partitionTimestamp) {
        throw CairoException.critical(0).put("remove partition does not update sequencer metadata");
    }

    @Override
    default void setMatViewRefresh(
            int refreshType,
            int timerInterval,
            char timerUnit,
            long timerStartUs,
            @Nullable CharSequence timerTimeZone,
            int periodLength,
            char periodLengthUnit,
            int periodDelay,
            char periodDelayUnit
    ) {
        throw CairoException.critical(0).put("change of materialized view refresh settings does not update sequencer metadata");
    }

    @Override
    default void setMatViewRefreshLimit(int limitHoursOrMonths) {
        throw CairoException.critical(0).put("change of materialized view refresh limit does not update sequencer metadata");
    }

    @Override
    default void setMatViewRefreshTimer(long startUs, int interval, char unit) {
        throw CairoException.critical(0).put("change of materialized view refresh timer does not update sequencer metadata");
    }

    @Override
    default void setMetaMaxUncommittedRows(int maxUncommittedRows) {
        throw CairoException.critical(0).put("change of max uncommitted does not update sequencer metadata");
    }

    @Override
    default void setMetaO3MaxLag(long o3MaxLagUs) {
        throw CairoException.critical(0).put("change of o3MaxLag does not update sequencer metadata");
    }

    @Override
    default void setMetaTtl(int ttlHoursOrMonths) {
        throw CairoException.critical(0).put("change of TTL does not update sequencer metadata");
    }

    @Override
    default void squashPartitions() {
        throw CairoException.critical(0).put("partition squash does not update sequencer metadata");
    }

    @Override
    default void tick() {
        // no-op
    }
}
