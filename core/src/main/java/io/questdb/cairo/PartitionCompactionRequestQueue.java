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

package io.questdb.cairo;

import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.std.Os;

/**
 * Bounded, lossy queue of requests to fold a logical partition into one plain folder, convert it into one parquet
 * folder, or build a parquet copy beside its native files, served by {@link PartitionCompactionScanJob} ahead of its
 * own sweep. A full queue refuses rather than blocks. Requests are not de-duplicated: a requester that wants the fold
 * keeps asking
 * until it sees it, and the job ignores a request whose logical partition is already plain or already has a swap
 * in flight.
 * <p>
 * A request's slot stays unconsumed until the job is done serving it, so a requester that keeps the sequence of its
 * request ({@link #tryRequestSeq}) can tell, with {@link #isConsumed}, when to hand over more work.
 */
public class PartitionCompactionRequestQueue {
    public static final int KIND_FOLD = 0;
    public static final int KIND_PARQUET_CONVERSION = 1;
    public static final int KIND_PARQUET_GENERATION = 2;
    private final MPSequence pubSeq;
    private final RingQueue<Request> queue;
    private final SCSequence subSeq;

    public PartitionCompactionRequestQueue(int capacity) {
        this.queue = new RingQueue<>(Request::new, capacity);
        this.pubSeq = new MPSequence(capacity);
        this.subSeq = new SCSequence();
        pubSeq.then(subSeq).then(pubSeq);
    }

    /**
     * Drops every queued request. Single consumer: not to be called while the job may be polling.
     */
    public void clear() {
        final Request sink = new Request();
        //noinspection StatementWithEmptyBody
        while (poll(sink)) {
        }
    }

    /**
     * Whether the job is done serving the request published at {@code seq}, as returned by
     * {@link #tryRequestSeq}. Any thread.
     */
    public boolean isConsumed(long seq) {
        return seq <= subSeq.current();
    }

    /**
     * Takes the oldest request into {@code sink} and consumes it. Single consumer.
     *
     * @return false when the queue is empty
     */
    public boolean poll(Request sink) {
        final long seq = take(sink);
        if (seq < 0) {
            return false;
        }
        release(seq);
        return true;
    }

    /**
     * Consumes the request {@link #take taken} at {@code seq}. Single consumer.
     */
    public void release(long seq) {
        // Not kept alive by a consumed slot.
        queue.get(seq).of(null, Long.MIN_VALUE, KIND_FOLD);
        subSeq.done(seq);
    }

    /**
     * Takes the oldest request into {@code sink} without consuming it: {@link #isConsumed} stays false for it until
     * {@link #release} is called with the returned sequence. Single consumer.
     *
     * @return the request's sequence, or -1 when the queue is empty
     */
    public long take(Request sink) {
        long seq;
        while ((seq = subSeq.next()) == -2) {
            Os.pause();
        }
        if (seq < 0) {
            return -1;
        }
        final Request request = queue.get(seq);
        sink.of(request.tableToken, request.logicalPartitionTimestamp, request.kind);
        return seq;
    }

    /**
     * Asks for the logical partition at {@code logicalPartitionTimestamp} to be folded into one plain folder.
     *
     * @return false when the queue is full and the request was not taken
     */
    public boolean tryRequest(TableToken tableToken, long logicalPartitionTimestamp) {
        return tryRequestSeq(tableToken, logicalPartitionTimestamp, KIND_FOLD) >= 0;
    }

    /**
     * Asks for the logical partition to be converted into one parquet folder; false when the queue is full.
     */
    public boolean tryRequestParquetConversion(TableToken tableToken, long logicalPartitionTimestamp) {
        return tryRequestSeq(tableToken, logicalPartitionTimestamp, KIND_PARQUET_CONVERSION) >= 0;
    }

    /**
     * Asks for a parquet copy installed beside the native files; false when the queue is full.
     */
    public boolean tryRequestParquetGeneration(TableToken tableToken, long logicalPartitionTimestamp) {
        return tryRequestSeq(tableToken, logicalPartitionTimestamp, KIND_PARQUET_GENERATION) >= 0;
    }

    /**
     * Asks for a request of {@code kind}: one of {@link #KIND_FOLD}, {@link #KIND_PARQUET_CONVERSION} or
     * {@link #KIND_PARQUET_GENERATION}.
     *
     * @return the request's sequence, for {@link #isConsumed}, or -1 when the queue is full
     */
    public long tryRequestSeq(TableToken tableToken, long logicalPartitionTimestamp, int kind) {
        long seq;
        while ((seq = pubSeq.next()) == -2) {
            Os.pause();
        }
        if (seq < 0) {
            return -1;
        }
        queue.get(seq).of(tableToken, logicalPartitionTimestamp, kind);
        pubSeq.done(seq);
        return seq;
    }

    public static class Request {
        private int kind = KIND_FOLD;
        private long logicalPartitionTimestamp = Long.MIN_VALUE;
        private TableToken tableToken;

        public int getKind() {
            return kind;
        }

        public long getLogicalPartitionTimestamp() {
            return logicalPartitionTimestamp;
        }

        public TableToken getTableToken() {
            return tableToken;
        }

        void of(TableToken tableToken, long logicalPartitionTimestamp, int kind) {
            this.tableToken = tableToken;
            this.logicalPartitionTimestamp = logicalPartitionTimestamp;
            this.kind = kind;
        }
    }
}
