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

package io.questdb.cutlass.parquet;

import io.questdb.cairo.EmptyTxnScoreboardPool;
import io.questdb.cairo.TxnScoreboardPool;
import io.questdb.cairo.sql.ExecutionCircuitBreaker;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.IntObjHashMap;
import org.jetbrains.annotations.Nullable;

public class CopyExportTask {

    public static final byte NO_PHASE = -1;
    public static final byte PHASE_CLEANUP = 2;
    public static final byte PHASE_MOVE_PARTITIONS = 7;
    public static final byte PHASE_PARTITION_EXPORT = 1;
    public static final byte PHASE_SETUP = 0;
    public static final byte STATUS_CANCELLED = 3;
    public static final byte STATUS_FAILED = 2;
    public static final byte STATUS_FINISHED = 1;
    public static final byte STATUS_STARTED = 0;
    private static final TxnScoreboardPool EMPTY_SCOREBOARD_POOL = new EmptyTxnScoreboardPool();
    private static final Log LOG = LogFactory.getLog(CopyExportTask.class);
    private static final IntObjHashMap<String> PHASE_NAME_MAP = new IntObjHashMap<>();
    private static final IntObjHashMap<String> STATUS_NAME_MAP = new IntObjHashMap<>();
    private int chunkIndex;
    private @Nullable ExecutionCircuitBreaker circuitBreaker;
    private @Nullable CharSequence errorMessage;
    private byte phase;
    private byte status;

    public static String getPhaseName(byte phase) {
        return PHASE_NAME_MAP.get(phase);
    }

    public static String getStatusName(byte status) {
        return STATUS_NAME_MAP.get(status);
    }

    public void clear() {
//        if (phase == PHASE_BOUNDARY_CHECK) {
//            phaseBoundaryCheck.clear();
//        } else if (phase == PHASE_INDEXING) {
//            phaseIndexing.clear();
//        } else if (phase == PHASE_PARTITION_IMPORT) {
//            phasePartitionImport.clear();
//        } else if (phase == PHASE_SYMBOL_TABLE_MERGE) {
//            phaseSymbolTableMerge.clear();
//        } else if (phase == PHASE_UPDATE_SYMBOL_KEYS) {
//            phaseUpdateSymbolKeys.clear();
//        } else if (phase == PHASE_BUILD_SYMBOL_INDEX) {
//            phaseBuildSymbolIndex.clear();
//        } else {
//            throw TextException.$("Unexpected phase ").put(phase);
//        }

    }
    
    public @Nullable CharSequence getErrorMessage() {
        return errorMessage;
    }

    public byte getPhase() {
        return phase;
    }

    public byte getStatus() {
        return status;
    }

    public boolean isCancelled() {
        return this.status == STATUS_CANCELLED;
    }

    public boolean isFailed() {
        return this.status == STATUS_FAILED;
    }

    public void setCircuitBreaker(@Nullable ExecutionCircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    private CopyExportException getCancelException() {
        CopyExportException ex = CopyExportException.instance(this.phase, "Cancelled");
        ex.setCancelled(true);
        return ex;
    }

    private void throwIfCancelled() throws CopyExportException {
        if (circuitBreaker != null && circuitBreaker.checkIfTripped()) {
            throw getCancelException();
        }
    }


    static {
        PHASE_NAME_MAP.put(PHASE_SETUP, "setup");
        PHASE_NAME_MAP.put(PHASE_MOVE_PARTITIONS, "move_partitions");
        PHASE_NAME_MAP.put(PHASE_CLEANUP, "cleanup");

        STATUS_NAME_MAP.put(STATUS_STARTED, "started");
        STATUS_NAME_MAP.put(STATUS_FINISHED, "finished");
        STATUS_NAME_MAP.put(STATUS_FAILED, "failed");
        STATUS_NAME_MAP.put(STATUS_CANCELLED, "cancelled");
    }
}
