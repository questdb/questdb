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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableWriterBackend;
import io.questdb.griffin.UpdateOperator;

public interface SequencerMetadataWriterBackend extends TableWriterBackend {

    @Override
    default void addIndex(CharSequence columnName, int indexValueBlockSize) {
        throw CairoException.instance(0).put("add index does not update sequencer metadata");
    }

    @Override
    default int attachPartition(long partitionTimestamp) {
        throw CairoException.instance(0).put("attach partition does not update sequencer metadata");
    }

    @Override
    default void changeCacheFlag(int columnIndex, boolean isCacheOn) {
        throw CairoException.instance(0).put("change cache flag does not update sequencer metadata");
    }

    @Override
    default void dropIndex(CharSequence columnName) {
        throw CairoException.instance(0).put("drop index does not update sequencer metadata");
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
        throw CairoException.instance(0).put("remove partition does not update sequencer metadata");
    }

    @Override
    default void setMetaCommitLag(long commitLag) {
        throw CairoException.instance(0).put("change commit lag does not update sequencer metadata");
    }

    @Override
    default void setMetaMaxUncommittedRows(int maxUncommittedRows) {
        throw CairoException.instance(0).put("change max uncommitted does not update sequencer metadata");
    }
}

