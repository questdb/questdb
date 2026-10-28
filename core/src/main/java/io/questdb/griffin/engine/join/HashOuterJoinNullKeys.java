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
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.plan.logical.JoinInput;
import org.jetbrains.annotations.Nullable;

/**
 * The NULL extension of a filtered hash outer join by its ON residual, see
 * {@link JoinInput.NullExtension#BY_ON_RESIDUAL}: the join NULL-extends a master row whose keys are all NULL when no
 * slave row passes the ON residual with it, and a slave row whose keys are all NULL when no master row does; a join
 * that NULL-extends {@link JoinInput.NullExtension#BY_KEYS} does neither. The NULL key check of a LEFT or FULL join,
 * see {@link JoinInput#getNullKeyCheck}, tells a master row whose keys are all NULL, and the NULL key match of the
 * join, see {@link JoinInput#getNullKeyMatch}, tells whether a master row changes which slave rows pass the ON
 * residual. The {@link SlaveChains} of the join walk the chains of slave rows its join key map refers to.
 */
final class HashOuterJoinNullKeys {
    private final SlaveChains chains;
    private final Function filter;
    private final Map joinKeyMap;
    private final RecordSink masterSink;
    private final JoinInput.NullExtension nullExtension;
    private final @Nullable Function nullKeyCheck;
    private final @Nullable JoinInput.NullKeyMatch nullKeyMatch;
    private boolean hasMasterRow;
    private boolean hasNullKeyChain;
    private boolean hasPassingSlaveRow;
    private boolean isPassingSlaveRowSought;
    private long nullKeyChainHead;
    private long nullKeyRowsToMatch;

    HashOuterJoinNullKeys(
            SlaveChains chains,
            Function filter,
            Map joinKeyMap,
            RecordSink masterSink,
            JoinInput.NullExtension nullExtension,
            @Nullable Function nullKeyCheck,
            @Nullable JoinInput.NullKeyMatch nullKeyMatch
    ) {
        this.chains = chains;
        this.filter = filter;
        this.joinKeyMap = joinKeyMap;
        this.masterSink = masterSink;
        this.nullExtension = nullExtension;
        this.nullKeyCheck = nullKeyCheck;
        this.nullKeyMatch = nullKeyMatch;
    }

    /**
     * Starts an execution, whose rows the join reads anew: forgets whether the master has a row and whether a slave row
     * passes the ON residual.
     */
    void clear() {
        hasMasterRow = false;
        isPassingSlaveRowSought = false;
    }

    /**
     * Looks up the chain of the slave rows whose keys are all NULL, the key of {@code masterNullRecord}, which a
     * master row matches by the ON residual alone, see {@link #matchNullKeyRows}.
     */
    void findNullKeyChain(Record masterNullRecord) {
        nullKeyRowsToMatch = 0;
        hasNullKeyChain = false;
        if (nullExtension == JoinInput.NullExtension.BY_ON_RESIDUAL) {
            final MapKey key = joinKeyMap.withKey();
            key.put(masterNullRecord, masterSink);
            final MapValue value = key.findValue();
            if (value != null) {
                hasNullKeyChain = true;
                nullKeyChainHead = chains.chainHead(value);
                nullKeyRowsToMatch = nullKeyMatch != JoinInput.NullKeyMatch.PER_SLAVE_ROW ? chains.chainSize(value) : 0;
            }
        }
    }

    /**
     * Whether the join drops {@code masterRecord}, which no slave row on its key chain passes the ON residual with,
     * instead of NULL-extending it: a key of the row is not NULL, or some slave row passes the ON residual with it on
     * {@code joined}, which pairs the master row with the slave record. Per pair of rows, see
     * {@link JoinInput.NullKeyMatch#PER_PAIR}, a master row whose keys are all NULL costs one evaluation per slave row;
     * per slave row, the first such master row of an execution does, and every later one reuses its answer.
     */
    boolean isMasterRowDropped(Record masterRecord, Record joined) {
        if (nullExtension != JoinInput.NullExtension.BY_ON_RESIDUAL) {
            return false;
        }
        assert nullKeyCheck != null : "LEFT or FULL join NULL-extended by its ON residual has no NULL key check";
        if (!nullKeyCheck.getBool(masterRecord)) {
            return true;
        }
        if (nullKeyMatch != JoinInput.NullKeyMatch.PER_SLAVE_ROW) {
            return chains.hasPassingSlaveRow(filter, joined);
        }
        if (!isPassingSlaveRowSought) {
            hasPassingSlaveRow = chains.hasPassingSlaveRow(filter, joined);
            isPassingSlaveRowSought = true;
        }
        return hasPassingSlaveRow;
    }

    /**
     * Whether the join NULL-extends {@code slaveRecord}, a row of a chain it does not skip, see
     * {@link #isUnmatchedChainSkipped}, when it emits the slave rows no master row matched: {@code matchIdsMap} does
     * not record it as matched and, per slave row, see {@link JoinInput.NullKeyMatch#PER_SLAVE_ROW}, the master has no
     * row or the row fails the ON residual on {@code joined}, which pairs the NULL master record with the slave record.
     */
    boolean isSlaveRowUnmatched(Record joined, Record slaveRecord, Map matchIdsMap) {
        if (nullKeyMatch == JoinInput.NullKeyMatch.PER_SLAVE_ROW && hasMasterRow && filter.getBool(joined)) {
            return false;
        }
        final MapKey keys = matchIdsMap.withKey();
        keys.put(slaveRecord, RecordIdSink.RECORD_ID_SINK);
        return keys.findValue() == null;
    }

    /**
     * Whether the join skips the chain of slave rows at {@code chainHead} when it NULL-extends the slave rows no
     * master row matched: it NULL-extends only slave rows whose keys are all NULL, see {@link #findNullKeyChain}.
     */
    boolean isUnmatchedChainSkipped(long chainHead) {
        return nullExtension == JoinInput.NullExtension.BY_ON_RESIDUAL && (!hasNullKeyChain || chainHead != nullKeyChainHead);
    }

    /**
     * Records that the master has a row and, per pair of rows, see {@link JoinInput.NullKeyMatch#PER_PAIR}, records as
     * matched in {@code matchIdsMap} each slave row of the NULL key chain, see {@link #findNullKeyChain}, that passes
     * the ON residual with the current master row on {@code joined}, which pairs the master row with
     * {@code slaveRecord}. Per slave row, {@link #isSlaveRowUnmatched} evaluates the ON residual on each row of the
     * chain instead.
     */
    void matchNullKeyRows(Record joined, Record slaveRecord, Map matchIdsMap) {
        hasMasterRow = true;
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
         * Whether {@code filter} passes on {@code joined} with some slave row the key map refers to, which it moves the
         * slave record to.
         */
        boolean hasPassingSlaveRow(Function filter, Record joined);

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
