/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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
import io.questdb.cairo.UpdateOperator;
import io.questdb.cairo.wal.MetadataChangeSPI;

public interface SequencerMetadataChangeSPI extends MetadataChangeSPI {

    @Override
    default void addIndex(CharSequence columnName, int indexValueBlockSize) {
        throw CairoException.critical().put("add index does not update sequencer metadata");
    }

    @Override
    default AttachDetachStatus attachPartition(long partitionTimestamp) {
        throw CairoException.critical().put("attach partition does not update sequencer metadata");
    }

    @Override
    default void changeCacheFlag(int columnIndex, boolean isCacheOn) {
        throw CairoException.critical().put("change cache flag does not update sequencer metadata");
    }

    @Override
    default AttachDetachStatus detachPartition(long partitionTimestamp) {
        throw CairoException.critical().put("detach partition does not update sequencer metadata");
    }

    @Override
    default void dropIndex(CharSequence columnName) {
        throw CairoException.critical().put("drop index does not update sequencer metadata");
    }

    @Override
    default long getCommitInterval() {
        throw new UnsupportedOperationException();
    }

    @Override
    default long getMetaMaxUncommittedRows() {
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
        throw CairoException.critical().put("remove partition does not update sequencer metadata");
    }

    @Override
    default void setMetaMaxUncommittedRows(int maxUncommittedRows) {
        throw CairoException.critical().put("change max uncommitted does not update sequencer metadata");
    }

    @Override
    default void setMetaO3MaxLag(long o3MaxLagUs) {
        throw CairoException.critical().put("change of o3MaxLag does not update sequencer metadata");
    }

    @Override
    default void tick() {
        // no-op
    }

    @Override
    default void updateCommitInterval(double commitIntervalFraction, long commitIntervalDefault) {
        throw CairoException.critical().put("change commit interval does not update sequencer metadata");
    }
}

