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

import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.str.Path;

/**
 * Reads a DETACHED composite partition artifact (a {@code <day>.detached} / {@code <day>.attachable}
 * directory) as ATTACH sees it.
 * <p>
 * {@code detachPartition} copies {@code _meta}, {@code _cv} and {@code _txn} into the artifact, and a
 * composite day keeps its data one level down in per-cell directories. The copied {@code _txn} carries
 * one attached-partition entry per cell, each with its cellKey, so the artifact's cells can be
 * enumerated authoritatively from metadata.
 * <p>
 * <b>Never parse a cell directory name to recover this.</b> Segment rendering is deliberately one-way:
 * it is path-safety encoded, has its own NULL token, and several dimension kinds render through
 * {@code putCellSegmentPathSafe}. Cell-qualified DROP already resolves names by RENDERING each attached
 * cellKey and comparing, precisely to avoid a reverse parse. A parser here would be a second, lossier
 * source of truth for the same mapping.
 */
public final class CompositeDetachedArtifact {

    private CompositeDetachedArtifact() {
    }

    /**
     * Collects, into {@code out}, the cellKey of every entry in the artifact's {@code _txn} whose
     * partition timestamp is {@code partitionTimestamp}. A plain artifact yields exactly one entry,
     * with cellKey 0.
     * <p>
     * The artifact's {@code _txn} is the whole table's {@code _txn} as of the detach, so it lists other
     * partitions too -- filtering by timestamp is required, not incidental.
     *
     * @param artifactRoot path to the artifact directory itself; left trimmed back to its original
     *                     length on return
     */
    public static void readCellKeys(
            FilesFacade ff,
            Path artifactRoot,
            int timestampType,
            int partitionBy,
            long partitionTimestamp,
            IntList out
    ) {
        out.clear();
        final int rootLen = artifactRoot.size();
        try (TxReader txReader = new TxReader(ff)) {
            txReader.ofRO(artifactRoot.concat(TableUtils.TXN_FILE_NAME).$(), timestampType, partitionBy);
            txReader.unsafeLoadAll();
            // getPartitionCount()/getPartitionCellKey() are stride-aware: the artifact's _txn carries the
            // self-describing stride marker, so a composite artifact reads back stride 8 and a plain one
            // stride 4 (cellKey always 0) without the caller knowing which it holds.
            for (int i = 0, n = txReader.getPartitionCount(); i < n; i++) {
                if (txReader.getPartitionTimestampByIndex(i) == partitionTimestamp) {
                    out.add(txReader.getPartitionCellKey(i));
                }
            }
        } finally {
            artifactRoot.trimTo(rootLen);
        }
    }
}
