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

package io.questdb.cairo;

import io.questdb.metrics.AtomicLongGauge;
import io.questdb.metrics.Counter;
import io.questdb.metrics.MetricsRegistry;
import io.questdb.std.Mutable;

public class TableWriterMetrics implements Mutable {

    // Includes all types of commits (in-order and o3)
    private final Counter commitCounter;
    private final Counter committedRowCounter;
    private final Counter o3CommitCounter;
    // O3 partition work that could not be dispatched to the shared pool and so was run inline on the
    // committing thread instead. Split by cause, because the remedies differ: `queue_full` means the
    // o3 partition queue is undersized for the workload (cairo.o3.partition.queue.capacity), whereas
    // `contended` means publishers merely lost the CAS and extra capacity would not have helped.
    // Without these, the fallback is invisible: it emits no log line, and the only external evidence
    // is the *absence* of the "o3 partition task" message.
    private final Counter o3PartitionInlineContendedCounter;
    private final Counter o3PartitionInlineQueueFullCounter;
    // For write amplification metric, `physicallyWrittenRowCounter / committedRowCounter`.
    private final Counter physicallyWrittenRowCounter;
    private final Counter rollbackCounter;
    private final AtomicLongGauge suspendedTablesGauge;

    public TableWriterMetrics(MetricsRegistry metricsRegistry) {
        this.commitCounter = metricsRegistry.newCounter("commits");
        this.o3CommitCounter = metricsRegistry.newCounter("o3_commits");
        this.committedRowCounter = metricsRegistry.newCounter("committed_rows");
        this.o3PartitionInlineQueueFullCounter = metricsRegistry.newCounter("o3_partitions_inline_queue_full");
        this.o3PartitionInlineContendedCounter = metricsRegistry.newCounter("o3_partitions_inline_contended");
        this.physicallyWrittenRowCounter = metricsRegistry.newCounter("physically_written_rows");
        this.rollbackCounter = metricsRegistry.newCounter("rollbacks");
        this.suspendedTablesGauge = metricsRegistry.newAtomicLongGauge("suspended_tables");
    }

    public void addCommittedRows(long rows) {
        committedRowCounter.add(rows);
    }

    public void addPhysicallyWrittenRows(long rows) {
        physicallyWrittenRowCounter.add(rows);
    }

    @Override
    public void clear() {
        commitCounter.reset();
        committedRowCounter.reset();
        o3CommitCounter.reset();
        o3PartitionInlineContendedCounter.reset();
        o3PartitionInlineQueueFullCounter.reset();
        physicallyWrittenRowCounter.reset();
        rollbackCounter.reset();
        suspendedTablesGauge.setValue(0);
    }

    public void decSuspendedTables() {
        suspendedTablesGauge.dec();
    }

    public long getCommitCount() {
        return commitCounter.getValue();
    }

    public long getCommittedRows() {
        return committedRowCounter.getValue();
    }

    public long getO3CommitCount() {
        return o3CommitCounter.getValue();
    }

    public long getO3PartitionsInlineContended() {
        return o3PartitionInlineContendedCounter.getValue();
    }

    public long getO3PartitionsInlineQueueFull() {
        return o3PartitionInlineQueueFullCounter.getValue();
    }

    public long getPhysicallyWrittenRows() {
        return physicallyWrittenRowCounter.getValue();
    }

    public long getRollbackCount() {
        return rollbackCounter.getValue();
    }

    public void incSuspendedTables() {
        suspendedTablesGauge.inc();
    }

    public void incrementCommits() {
        commitCounter.inc();
    }

    public void incrementO3Commits() {
        o3CommitCounter.inc();
    }

    /**
     * Records an o3 partition that was processed inline because the partition queue could not accept
     * it. {@code cursor} is the value returned by the publisher sequence: {@code -1} means the queue
     * was full, {@code -2} means the publish CAS was lost to another producer.
     */
    public void incrementO3PartitionsInline(long cursor) {
        if (cursor == -1) {
            o3PartitionInlineQueueFullCounter.inc();
        } else {
            o3PartitionInlineContendedCounter.inc();
        }
    }

    public void incrementRollbacks() {
        rollbackCounter.inc();
    }
}
