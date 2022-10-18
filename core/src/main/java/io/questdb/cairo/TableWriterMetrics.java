/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.metrics.Counter;
import io.questdb.metrics.MetricsRegistry;

public class TableWriterMetrics {

    // Includes all types of commits (in-order and o3)
    private final Counter commitCounter;
    private final Counter o3CommitCounter;
    private final Counter committedRowCounter;
    private final Counter rollbackCounter;

    // For write amplification metric, `physicallyWrittenRowCounter / committedRowCounter`.
    private final Counter physicallyWrittenRowCounter;

    public TableWriterMetrics(MetricsRegistry metricsRegistry) {
        this.commitCounter = metricsRegistry.newCounter("commits");
        this.o3CommitCounter = metricsRegistry.newCounter("o3_commits");
        this.committedRowCounter = metricsRegistry.newCounter("committed_rows");
        this.rollbackCounter = metricsRegistry.newCounter("rollbacks");
        this.physicallyWrittenRowCounter = metricsRegistry.newCounter("physically_written_rows");
    }

    public void addCommittedRows(long rows) {
        committedRowCounter.add(rows);
    }

    public void addPhysicallyWrittenRows(long rows) {
        physicallyWrittenRowCounter.add(rows);
    }

    public long getCommitCount() {
        return commitCounter.getValue();
    }

    public long getO3CommitCount() {
        return o3CommitCounter.getValue();
    }

    public long getCommittedRows() {
        return committedRowCounter.getValue();
    }

    public long getRollbackCount() {
        return rollbackCounter.getValue();
    }

    public long getPhysicallyWrittenRows() {
        return physicallyWrittenRowCounter.getValue();
    }

    public void incrementCommits() {
        commitCounter.inc();
    }

    public void incrementO3Commits() {
        o3CommitCounter.inc();
    }

    public void incrementRollbacks() {
        rollbackCounter.inc();
    }
}
