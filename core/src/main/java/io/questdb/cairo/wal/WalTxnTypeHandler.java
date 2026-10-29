/*+*****************************************************************************
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

package io.questdb.cairo.wal;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;

public interface WalTxnTypeHandler {
    default void applyDeltaRunWrite(CairoEngine engine, TableWriter writer, long seqTxn) {
        throw CairoException.critical(0).put("Delta transaction recovery is unavailable [table=")
                .put(writer.getTableToken()).put(", txn=").put(seqTxn).put(']');
    }

    default void applyDeltaTableSwitch(TableWriter writer, long seqTxn) {
        throw CairoException.critical(0)
                .put("unsupported sequencer transaction kind for WAL apply [walId=").put(WalUtils.DELTA_TABLE_SWITCH_WAL_ID)
                .put(", table=").put(writer.getTableToken()).put(", seqTxn=").put(seqTxn).put(']');
    }

    int applyUnknownWalTxn(byte walTxnType, TableWriter writer, WalEventCursor cursor, long seqTxn);

    /**
     * Publishes accepted but unpublished Delta transactions. Returns the applied seqTxn, or -1 when skipped.
     */
    default long recoverDeltaTable(CairoEngine engine, TableToken tableToken) {
        throw CairoException.critical(0).put("Delta transaction recovery is unavailable [table=").put(tableToken).put(']');
    }
}
