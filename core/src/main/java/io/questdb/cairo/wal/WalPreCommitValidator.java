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

/**
 * Optional hook that {@link WalWriter} consults right before sealing a transaction.
 * Implementations MUST throw {@link io.questdb.cairo.CairoException} to reject the
 * transaction; the writer rolls the transaction back and rethrows. Any other
 * Throwable from an implementation is treated as an internal validator fault: the
 * writer marks itself distressed and lets the Throwable propagate, so the next
 * acquisition of this token gets a fresh tenant. Implementations must be fast and
 * side-effect free -- a slow validator stretches every commit on the table it is
 * attached to.
 */
@FunctionalInterface
public interface WalPreCommitValidator {
    /**
     * @param txnType   one of {@link WalTxnType} constants -- the validator
     *                  typically only cares about {@link WalTxnType#DATA}.
     * @param dedupMode the WAL_DEDUP_MODE_* value the writer is committing
     *                  with. Refresh-job writes carry REPLACE_RANGE; plain
     *                  user INSERTs carry the default.
     * @param txnMinTs  minimum row timestamp in the txn (driver units),
     *                  Long.MAX_VALUE for an empty txn.
     * @param txnMaxTs  maximum row timestamp in the txn (driver units),
     *                  -1 for an empty txn.
     */
    void validate(byte txnType, byte dedupMode, long txnMinTs, long txnMaxTs);
}
