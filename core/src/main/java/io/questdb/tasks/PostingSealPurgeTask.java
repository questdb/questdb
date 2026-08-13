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

package io.questdb.tasks;

import io.questdb.cairo.TableToken;
import io.questdb.std.Mutable;
import io.questdb.std.str.StringSink;

public class PostingSealPurgeTask implements Mutable {
    // The ON-DISK form of the artifacts THIS TASK names, int-valued and
    // PERSISTED (sys.posting_seal_purge_log's artifact_form column and the
    // spill file's format word 2). NOT interchangeable with
    // PostingIndexUtils.PARQUET_INDEX_FORMAT_*, which describes the same
    // native-vs-parquet distinction for the CONFIGURED writer form with a
    // different byte encoding (NATIVE 0, PARQUET 1) and no UNKNOWN. The two
    // overlap numerically -- ARTIFACT_FORM_NATIVE == PARQUET_INDEX_FORMAT_PARQUET
    // == 1 -- so a mix-up is silent. Being persisted, this family's values are
    // also not free to change.
    /**
     * The task names a {@code <col>.pv.<postingColumnNameTxn>.<sealTxn>} value
     * file and its {@code .pc*} covers. {@code sealTxn} is a per-column chain
     * generation counted by {@code PostingIndexChainWriter}.
     */
    public static final int ARTIFACT_FORM_NATIVE = 1;
    /**
     * The task names a {@code <col>.pidx.<sealTxn>.parquet} and its
     * {@code ._im}. {@code sealTxn} is a table txn -- the covering index txn
     * published in the partition's {@code _pm}.
     */
    public static final int ARTIFACT_FORM_PARQUET = 2;
    /**
     * A task recovered from a purge log or spill file written by a build that
     * did not record the artifact form. Its {@code sealTxn} cannot be attributed
     * to either namespace, so no artifact may be unlinked for it: a
     * {@code sealTxn} that belongs to one namespace can name a live file in the
     * other. Such tasks are dropped with a critical log rather than acted on.
     */
    public static final int ARTIFACT_FORM_UNKNOWN = 0;
    private final StringSink indexColumnName = new StringSink();
    private int artifactForm = ARTIFACT_FORM_UNKNOWN;
    private long fromTableTxn;
    private int partitionBy;
    private long partitionNameTxn;
    private long partitionTimestamp;
    private long postingColumnNameTxn;
    private long sealTxn;
    private TableToken tableToken;
    private int timestampType;
    private long toTableTxn;

    public static boolean isValidArtifactForm(int form) {
        return form == ARTIFACT_FORM_NATIVE || form == ARTIFACT_FORM_PARQUET;
    }

    @Override
    public void clear() {
        this.tableToken = null;
        this.indexColumnName.clear();
        this.artifactForm = ARTIFACT_FORM_UNKNOWN;
    }

    /**
     * Which namespace this task's {@link #getSealTxn()} belongs to. The two
     * namespaces are counted independently -- the native chain generation by
     * {@code PostingIndexChainWriter}'s per-column {@code genCounter}, the
     * parquet covering index txn by the table txn -- and one partition
     * directory can carry both forms for one column at once, so equal numbers
     * do not mean the same version. Without this, a task's scoreboard window,
     * which is derived from its own namespace's supersession point, would be
     * applied to an artifact in the other namespace whose reader-reachability
     * that window says nothing about.
     */
    public int getArtifactForm() {
        return artifactForm;
    }

    public long getFromTableTxn() {
        return fromTableTxn;
    }

    public CharSequence getIndexColumnName() {
        return indexColumnName;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    public long getPartitionNameTxn() {
        return partitionNameTxn;
    }

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    public long getPostingColumnNameTxn() {
        return postingColumnNameTxn;
    }

    public long getSealTxn() {
        return sealTxn;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public int getTimestampType() {
        return timestampType;
    }

    public long getToTableTxn() {
        return toTableTxn;
    }

    public boolean isEmpty() {
        return tableToken == null;
    }

    public void of(
            TableToken tableToken,
            CharSequence indexColumnName,
            long postingColumnNameTxn,
            long sealTxn,
            int artifactForm,
            long partitionTimestamp,
            long partitionNameTxn,
            int partitionBy,
            int timestampType,
            long fromTableTxn,
            long toTableTxn
    ) {
        this.tableToken = tableToken;
        this.indexColumnName.clear();
        this.indexColumnName.put(indexColumnName);
        this.postingColumnNameTxn = postingColumnNameTxn;
        this.sealTxn = sealTxn;
        this.artifactForm = artifactForm;
        this.partitionTimestamp = partitionTimestamp;
        this.partitionNameTxn = partitionNameTxn;
        this.partitionBy = partitionBy;
        this.timestampType = timestampType;
        this.fromTableTxn = fromTableTxn;
        this.toTableTxn = toTableTxn;
    }
}
