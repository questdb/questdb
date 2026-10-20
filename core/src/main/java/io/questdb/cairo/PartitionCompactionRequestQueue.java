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
 * Bounded, lossy queue of requests to fold one logical partition into a single plain folder, to convert it into a
 * single parquet folder, or to build a parquet copy of it beside its native files, served by
 * {@link PartitionCompactionScanJob} ahead of its own sweep. Any thread may make one - the enterprise storage policy
 * does - and a full queue refuses rather than blocks. Requests are not de-duplicated: a requester that wants the fold keeps asking
 * until it sees it, and the job ignores a request whose logical partition is already plain or already has a swap
 * in flight.
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
     * Takes the oldest request into {@code sink}. Single consumer.
     *
     * @return false when the queue is empty
     */
    public boolean poll(Request sink) {
        long cursor;
        while ((cursor = subSeq.next()) == -2) {
            Os.pause();
        }
        if (cursor < 0) {
            return false;
        }
        final Request request = queue.get(cursor);
        sink.of(request.tableToken, request.logicalPartitionTimestamp, request.kind);
        // Not kept alive by a consumed slot.
        request.of(null, Long.MIN_VALUE, KIND_FOLD);
        subSeq.done(cursor);
        return true;
    }

    /**
     * Asks for the logical partition at {@code logicalPartitionTimestamp} to be folded into one plain folder.
     *
     * @return false when the queue is full and the request was not taken
     */
    public boolean tryRequest(TableToken tableToken, long logicalPartitionTimestamp) {
        return tryRequest(tableToken, logicalPartitionTimestamp, KIND_FOLD);
    }

    /**
     * Asks for the logical partition at {@code logicalPartitionTimestamp} - whatever its folders, plain or composite,
     * main directory or splits - to be converted into one parquet folder in a single pass.
     *
     * @return false when the queue is full and the request was not taken
     */
    public boolean tryRequestParquetConversion(TableToken tableToken, long logicalPartitionTimestamp) {
        return tryRequest(tableToken, logicalPartitionTimestamp, KIND_PARQUET_CONVERSION);
    }

    /**
     * Asks for a parquet copy of the logical partition at {@code logicalPartitionTimestamp} to be built and installed
     * beside its native files, the partition staying native - what cold storage uploads a native partition from. A
     * logical partition that is not one plain folder is folded first, and the requester asks again.
     *
     * @return false when the queue is full and the request was not taken
     */
    public boolean tryRequestParquetGeneration(TableToken tableToken, long logicalPartitionTimestamp) {
        return tryRequest(tableToken, logicalPartitionTimestamp, KIND_PARQUET_GENERATION);
    }

    private boolean tryRequest(TableToken tableToken, long logicalPartitionTimestamp, int kind) {
        long cursor;
        while ((cursor = pubSeq.next()) == -2) {
            Os.pause();
        }
        if (cursor < 0) {
            return false;
        }
        queue.get(cursor).of(tableToken, logicalPartitionTimestamp, kind);
        pubSeq.done(cursor);
        return true;
    }

    public static class Request {
        private int kind = KIND_FOLD;
        private long logicalPartitionTimestamp = Long.MIN_VALUE;
        private TableToken tableToken;

        /**
         * One of {@link #KIND_FOLD}, {@link #KIND_PARQUET_CONVERSION}, {@link #KIND_PARQUET_GENERATION}.
         */
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
