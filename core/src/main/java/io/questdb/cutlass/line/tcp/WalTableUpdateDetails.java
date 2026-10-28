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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.std.Pool;
import io.questdb.std.str.Utf8String;
import org.jetbrains.annotations.Nullable;

public class WalTableUpdateDetails extends TableUpdateDetails {
    // Highest seqTxn already handed to the ingress committed-txn consumer (the QWP
    // ack / durable-upload watermarks). Compared against getLastSeqTxn() so a commit
    // made ANYWHERE -- including QwpWalAppender's force-commit at the
    // max-uncommitted-rows cap, which happens outside QwpTudCache entirely -- still
    // reaches the consumer, and reaches it exactly once ONCE THE CONSUMER ACCEPTS IT.
    // reportCommittedTxn advances this watermark only after the consumer returns, so a
    // consumer that throws leaves the seqTxn behind for the cache's next pass to offer
    // again: the cache-level contract is at-least-once, exactly-once after acceptance.
    // The constructor seeds it from the writer, so a previous
    // owner of the pooled writer cannot have its advance reported on this connection.
    // The seed bounds WHOSE advances reach the consumer, not WHAT KIND of advance: an
    // implicit ALTER advances getLastSeqTxn() the same way a data commit does, so a
    // metadata-only txn can be reported on its own. QwpTudCache.reportCommittedTxn
    // accepts that rather than gating on it; the reasoning lives there.
    // Never reused across connections: QwpIngressProcessorState.onDisconnected() (and
    // QwpTudCache.close()) call QwpTudCache.reset(), which frees every TUD. The writer
    // underneath is pooled and outlives the TUD, which is what the constructor seed
    // handles. QwpTudCache.clear(), which runs between frames on a live connection, does
    // NOT free: it rolls uncommitted rows back and keeps the TUD so the next frame reuses
    // the cached writer, freeing only when the cache is distressed. That free is safe:
    // every setDistressed() caller also rejects the frame, so the unresolved-sequence gate
    // in QwpIngressUpgradeProcessor.handleBinaryMessage refuses every later data frame on
    // the connection and no re-seeded TUD is ever built for it.
    private long lastReportedSeqTxn;
    /**
     * Sequencer transaction this table's writer was last checked for pending
     * structure changes at. {@link Long#MIN_VALUE} forces the first check.
     * <p>
     * Read and written from the QWP ingest path only, which is single-threaded
     * per cache, so it needs no synchronisation.
     */
    private long lastStructureCheckSeqTxn = Long.MIN_VALUE;

    public WalTableUpdateDetails(
            CairoEngine engine,
            @Nullable SecurityContext securityContext,
            TableWriterAPI writer,
            DefaultColumnTypes defaultColumnTypes,
            Utf8String tableNameUtf8,
            Pool<SymbolCache> symbolCachePool,
            long commitInterval,
            boolean commitOnClose,
            long maxUncommittedRows
    ) {
        super(engine, securityContext, writer, -1, defaultColumnTypes, tableNameUtf8, symbolCachePool, commitInterval, commitOnClose, maxUncommittedRows);
        // Seed the watermark from the writer rather than from a fixed sentinel:
        // engine.getWalWriter() can hand back a pooled WalWriter whose lastSeqTxn still
        // carries the PREVIOUS owner's commit -- WalWriterPool.WalWriterTenant.refresh()
        // only calls goActive(), and neither goActive() nor rollback0() clears the field.
        // Without this seed the first reportCommittedTxn() on such a writer hands the ack
        // and durable-upload watermarks a seqTxn this connection never committed, parking
        // a table entry with no work behind it that a demote then waits out. A brand-new
        // writer reports TableSequencer.NO_TXN (Long.MIN_VALUE), and a non-WAL
        // TableWriterAPI reports -1, so fresh-table behaviour is unchanged.
        this.lastReportedSeqTxn = writer.getLastSeqTxn();
    }

    public long getLastReportedSeqTxn() {
        return lastReportedSeqTxn;
    }

    public long getLastStructureCheckSeqTxn() {
        return lastStructureCheckSeqTxn;
    }

    @Override
    public ThreadLocalDetails getThreadLocalDetails(int workerId) {
        return super.getThreadLocalDetails(0);
    }

    public void setLastReportedSeqTxn(long seqTxn) {
        this.lastReportedSeqTxn = seqTxn;
    }

    public void setLastStructureCheckSeqTxn(long seqTxn) {
        lastStructureCheckSeqTxn = seqTxn;
    }
}
