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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.RecordIdSink;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import org.jetbrains.annotations.Nullable;

/**
 * The NULL extension of a filtered hash outer join by its ON residual, see
 * {@code JoinInput.NullExtension#BY_ON_RESIDUAL}: the join NULL-extends a master row whose keys are all NULL when no
 * slave row passes the ON residual with it, and a slave row whose keys are all NULL when no master row does. The
 * NULL key check of the join, see {@code JoinInput#getNullKeyCheck}, tells a master row whose keys are all NULL, and
 * the join NULL-extends by its keys when it has none. The {@link SlaveChains} of the join walk the chains of slave
 * rows its join key map refers to.
 */
final class HashOuterJoinNullKeys {
    private final SlaveChains chains;
    private final Function filter;
    private final Map joinKeyMap;
    private final RecordSink masterSink;
    private final @Nullable Function nullKeyCheck;
    private boolean hasNullKeyChain;
    private long nullKeyChainHead;
    private long nullKeyRowsToMatch;

    HashOuterJoinNullKeys(
            SlaveChains chains,
            Function filter,
            Map joinKeyMap,
            RecordSink masterSink,
            @Nullable Function nullKeyCheck
    ) {
        this.chains = chains;
        this.filter = filter;
        this.joinKeyMap = joinKeyMap;
        this.masterSink = masterSink;
        this.nullKeyCheck = nullKeyCheck;
    }

    /**
     * Looks up the chain of the slave rows whose keys are all NULL, the key of {@code masterNullRecord}, which a
     * master row matches by the ON residual alone, see {@link #matchNullKeyRows}.
     */
    void findNullKeyChain(Record masterNullRecord) {
        nullKeyRowsToMatch = 0;
        hasNullKeyChain = false;
        if (nullKeyCheck != null) {
            final MapKey key = joinKeyMap.withKey();
            key.put(masterNullRecord, masterSink);
            final MapValue value = key.findValue();
            if (value != null) {
                hasNullKeyChain = true;
                nullKeyChainHead = chains.chainHead(value);
                nullKeyRowsToMatch = chains.chainSize(value);
            }
        }
    }

    /**
     * Whether the join drops {@code masterRecord}, which no slave row on its key chain passes the ON residual with,
     * instead of NULL-extending it: a key of the row is not NULL, or some slave row passes the ON residual with it on
     * {@code joined}, which pairs the master row with the slave record. The ON residual reads the master row, so a
     * master row whose keys are all NULL costs one evaluation per row of the build side.
     */
    boolean isMasterRowDropped(Record masterRecord, Record joined) {
        if (nullKeyCheck == null) {
            return false;
        }
        if (!nullKeyCheck.getBool(masterRecord)) {
            return true;
        }
        final MapRecordCursor keys = joinKeyMap.getCursor();
        try {
            while (keys.hasNext()) {
                chains.toChain(chains.chainHead(keys.getRecord().getValue()));
                while (chains.nextChainRow()) {
                    if (filter.getBool(joined)) {
                        return true;
                    }
                }
            }
            return false;
        } finally {
            keys.toTop();
        }
    }

    /**
     * Whether the join skips the chain of slave rows at {@code chainHead} when it NULL-extends the slave rows no
     * master row matched: it NULL-extends only slave rows whose keys are all NULL, see {@link #findNullKeyChain}.
     */
    boolean isUnmatchedChainSkipped(long chainHead) {
        return nullKeyCheck != null && (!hasNullKeyChain || chainHead != nullKeyChainHead);
    }

    /**
     * Records as matched in {@code matchIdsMap} each slave row of the NULL key chain, see {@link #findNullKeyChain},
     * that passes the ON residual with the current master row on {@code joined}, which pairs the master row with
     * {@code slaveRecord}.
     */
    void matchNullKeyRows(Record joined, Record slaveRecord, Map matchIdsMap) {
        if (nullKeyRowsToMatch > 0) {
            chains.toChain(nullKeyChainHead);
            while (chains.nextChainRow()) {
                if (filter.getBool(joined)) {
                    final MapKey keys = matchIdsMap.withKey();
                    keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
                    if (keys.createValue().isNew()) {
                        nullKeyRowsToMatch--;
                    }
                }
            }
        }
    }

    /**
     * Walks the chains of slave rows that the values of a hash join key map refer to.
     */
    interface SlaveChains {
        /**
         * The head of the chain of slave rows that the key map value refers to.
         */
        long chainHead(MapValue value);

        /**
         * The number of slave rows on the chain that the key map value refers to.
         */
        long chainSize(MapValue value);

        /**
         * Moves the slave record to the next row of the chain {@link #toChain} started, and returns false past its
         * last row.
         */
        boolean nextChainRow();

        /**
         * Starts walking the chain of slave rows at {@code chainHead}.
         */
        void toChain(long chainHead);
    }
}
