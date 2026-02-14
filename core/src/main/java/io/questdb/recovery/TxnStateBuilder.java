/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.recovery;

import io.questdb.std.ObjList;

/**
 * Public builder for {@link TxnState}. Wraps the package-private setters so
 * that tests and the {@code truncate} command can construct modified states.
 */
public final class TxnStateBuilder {
    private final TxnState state = new TxnState();

    public TxnState build() {
        return state;
    }

    public TxnStateBuilder baseVersion(long v) {
        state.setBaseVersion(v);
        return this;
    }

    public TxnStateBuilder columnVersion(long v) {
        state.setColumnVersion(v);
        return this;
    }

    public TxnStateBuilder dataVersion(long v) {
        state.setDataVersion(v);
        return this;
    }

    public TxnStateBuilder fixedRowCount(long v) {
        state.setFixedRowCount(v);
        return this;
    }

    public TxnStateBuilder lagChecksum(int v) {
        state.setLagChecksum(v);
        return this;
    }

    public TxnStateBuilder lagMaxTimestamp(long v) {
        state.setLagMaxTimestamp(v);
        return this;
    }

    public TxnStateBuilder lagMinTimestamp(long v) {
        state.setLagMinTimestamp(v);
        return this;
    }

    public TxnStateBuilder lagRowCount(int v) {
        state.setLagRowCount(v);
        return this;
    }

    public TxnStateBuilder lagTxnCount(int v) {
        state.setLagTxnCount(v);
        return this;
    }

    public TxnStateBuilder mapWriterCount(int v) {
        state.setMapWriterCount(v);
        return this;
    }

    public TxnStateBuilder maxTimestamp(long v) {
        state.setMaxTimestamp(v);
        return this;
    }

    public TxnStateBuilder minTimestamp(long v) {
        state.setMinTimestamp(v);
        return this;
    }

    public ObjList<TxnPartitionState> partitions() {
        return state.getPartitions();
    }

    public TxnStateBuilder partitionTableVersion(long v) {
        state.setPartitionTableVersion(v);
        return this;
    }

    public TxnStateBuilder seqTxn(long v) {
        state.setSeqTxn(v);
        return this;
    }

    public TxnStateBuilder structureVersion(long v) {
        state.setStructureVersion(v);
        return this;
    }

    public ObjList<TxnSymbolState> symbols() {
        return state.getSymbols();
    }

    public TxnStateBuilder transientRowCount(long v) {
        state.setTransientRowCount(v);
        return this;
    }

    public TxnStateBuilder truncateVersion(long v) {
        state.setTruncateVersion(v);
        return this;
    }

    public TxnStateBuilder txn(long v) {
        state.setTxn(v);
        return this;
    }
}
