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

package io.questdb.cairo.wal;

import io.questdb.cairo.TableToken;

/**
 * Tracks the highest sequencer txn that has been durably persisted (uploaded to
 * the configured object store) for each WAL table. Used by QWP to emit a second
 * "durable" acknowledgment frame to clients that opt in.
 * <p>
 * The OSS server ships a no-op implementation ({@link DefaultDurableAckRegistry})
 * that reports nothing as durable; enterprise installations with primary
 * replication enabled install a real implementation backed by the upload
 * pipeline.
 */
public interface DurableAckRegistry {

    /**
     * Returns the highest seqTxn that has been durably uploaded for the given
     * table, or -1 if no upload has completed yet, the table is unknown to the
     * registry, or durable-ack tracking is not enabled on this server.
     *
     * @param tableDirName the directory name of the table (matches
     *                     {@code TableToken.getDirName()})
     * @return the highest durably-uploaded seqTxn, or -1
     */
    long getDurablyUploadedSeqTxn(CharSequence tableDirName);

    /**
     * Returns true when durable-ack tracking is wired up on this server (i.e.
     * primary replication to an object store is enabled). When false, QWP
     * silently ignores the {@code X-QWP-Request-Durable-Ack} opt-in header.
     */
    boolean isEnabled();

    /**
     * Invoked after a table, view or materialized view has been dropped so the
     * registry can release any state keyed by that table's directory name.
     * Late uploads arriving after the drop may re-create a short-lived entry;
     * that orphan is bounded by the uploader's in-flight queue depth at drop
     * time and is acceptable.
     */
    default void onTableDropped(TableToken tableToken) {
    }
}
