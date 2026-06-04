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

import io.questdb.cairo.idx.IndexWriter;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.mp.RingQueue;
import io.questdb.mp.Sequence;
import io.questdb.std.FilesFacade;
import io.questdb.tasks.O3CopyTask;
import io.questdb.tasks.O3OpenColumnTask;

/**
 * Narrow capability view used by O3 worker jobs.
 * <p>
 * The type is public only so task payloads in {@code io.questdb.tasks} can carry
 * it instead of a full {@link TableWriter}. Instances are owned and reused by
 * {@link TableWriter}; O3 code must not construct its own view or retain it past
 * the owning writer lifetime.
 * <p>
 * Methods on this class are part of the O3 threading contract. They may expose
 * only commit-frozen scalar state, immutable configuration values, explicit O3
 * dispatch queues, atomic completion/error accounting, and read-only views of
 * worker-safe memory. They must not expose the full writer or raw writer-owned
 * mutable collaborators such as metadata objects, column-version readers, symbol
 * map writers, dedup address owners, or configuration objects.
 * <p>
 * Add a method here only for a concrete O3 job call site. If a capability is not
 * generally worker-safe but is temporarily required to preserve existing
 * behavior, give it an {@code Unsafe} name and keep the caller count explicit.
 */
public final class O3TableWriterView {
    private final TableWriter tableWriter;

    O3TableWriterView(TableWriter tableWriter) {
        this.tableWriter = tableWriter;
    }

    void addDedupRowsRemoved(long rows) {
        tableWriter.addDedupRowsRemoved(rows);
    }

    void addPhysicallyWrittenRows(long rows) {
        tableWriter.addPhysicallyWrittenRows(rows);
    }

    boolean allowMixedIO() {
        return tableWriter.allowMixedIO();
    }

    boolean checkDedupCommitIdenticalToPartition(
            long partitionTimestamp,
            long partitionNameTxn,
            long partitionRowCount,
            long partitionLo,
            long partitionHi,
            long commitLo,
            long commitHi,
            long mergeIndexAddr,
            long mergeIndexRows
    ) {
        return tableWriter.checkDedupCommitIdenticalToPartition(
                partitionTimestamp,
                partitionNameTxn,
                partitionRowCount,
                partitionLo,
                partitionHi,
                commitLo,
                commitHi,
                mergeIndexAddr,
                mergeIndexRows
        );
    }

    boolean checkReplaceCommitIdenticalToPartition(
            long partitionTimestamp,
            long partitionNameTxn,
            long partitionRowCount,
            long partitionLo,
            long partitionHi,
            long commitLo,
            long commitHi
    ) {
        return tableWriter.checkReplaceCommitIdenticalToPartition(
                partitionTimestamp,
                partitionNameTxn,
                partitionRowCount,
                partitionLo,
                partitionHi,
                commitLo,
                commitHi
        );
    }

    long getColumnNameTxn(long partitionTimestamp, int columnIndex) {
        return tableWriter.getColumnNameTxn(partitionTimestamp, columnIndex);
    }

    int getColumnCount() {
        return tableWriter.getMetadata().getColumnCount();
    }

    byte getColumnIndexType(int columnIndex) {
        return tableWriter.getMetadata().getColumnIndexType(columnIndex);
    }

    String getColumnName(int columnIndex) {
        return tableWriter.getMetadata().getColumnName(columnIndex);
    }

    int getColumnParquetEncodingConfig(int columnIndex) {
        return tableWriter.getMetadata().getColumnMetadata(columnIndex).getParquetEncodingConfig();
    }

    long getColumnTop(int columnIndex) {
        return tableWriter.getColumnTop(columnIndex);
    }

    long getColumnTop(long partitionTimestamp, int columnIndex, long defaultValue) {
        return tableWriter.getColumnTop(partitionTimestamp, columnIndex, defaultValue);
    }

    int getColumnType(int columnIndex) {
        return tableWriter.getMetadata().getColumnType(columnIndex);
    }

    int getColumnWriterIndex(int columnIndex) {
        return tableWriter.getMetadata().getWriterIndex(columnIndex);
    }

    int getDedupColumnCount() {
        final DedupColumnCommitAddresses dedupColumnCommitAddresses = tableWriter.getDedupCommitAddresses();
        return dedupColumnCommitAddresses != null ? dedupColumnCommitAddresses.getColumnCount() : 0;
    }

    FilesFacade getFilesFacade() {
        return tableWriter.getFilesFacade();
    }

    /**
     * Opens an O3 task-owned index writer on file descriptors prepared by the
     * open-column job.
     * <p>
     * The {@code indexWriter} argument must be detached from {@link TableWriter}
     * state, typically supplied by {@link O3Basket#nextIndexer(byte)} and carried
     * through the O3 task payload. This method mutates the writer passed to it,
     * so it must not be used with a shared writer returned from
     * {@link TableWriter#getIndexWriter(int)}. Shared last-partition index writer
     * access is intentionally isolated behind
     * {@link #o3UnsafeGetIndexWriterForLastPartitionOnly(int)} until that path is
     * removed.
     */
    void openIndexWriter(IndexWriter indexWriter, long keyFd, long valueFd, boolean isInit, int indexBlockCapacity) {
        indexWriter.of(tableWriter.getConfiguration(), keyFd, valueFd, isInit, indexBlockCapacity);
    }

    int getIndexValueBlockCapacity(int columnIndex) {
        return tableWriter.getMetadata().getIndexValueBlockCapacity(columnIndex);
    }

    Sequence getO3CopyPubSeq() {
        return tableWriter.getO3CopyPubSeq();
    }

    RingQueue<O3CopyTask> getO3CopyQueue() {
        return tableWriter.getO3CopyQueue();
    }

    Sequence getO3OpenColumnPubSeq() {
        return tableWriter.getO3OpenColumnPubSeq();
    }

    RingQueue<O3OpenColumnTask> getO3OpenColumnQueue() {
        return tableWriter.getO3OpenColumnQueue();
    }

    int getPartitionBy() {
        return tableWriter.getPartitionBy();
    }

    int getPartitionIndexByTimestamp(long timestamp) {
        return tableWriter.getPartitionIndexByTimestamp(timestamp);
    }

    long getPartitionO3SplitThreshold() {
        return tableWriter.getPartitionO3SplitThreshold();
    }

    long getPartitionParquetFileSize(int partitionIndex) {
        return tableWriter.getPartitionParquetFileSize(partitionIndex);
    }

    TableToken getTableToken() {
        return tableWriter.getTableToken();
    }

    int getTimestampIndex() {
        return tableWriter.getMetadata().getTimestampIndex();
    }

    int getTimestampType() {
        return tableWriter.getTimestampType();
    }

    long getTxn() {
        return tableWriter.getTxn();
    }

    boolean isColumnIndexed(int columnIndex) {
        return tableWriter.getMetadata().isColumnIndexed(columnIndex);
    }

    boolean isCommitDedupMode() {
        return tableWriter.isCommitDedupMode();
    }

    boolean isCommitPlainInsert() {
        return tableWriter.isCommitPlainInsert();
    }

    boolean isCommitReplaceMode() {
        return tableWriter.isCommitReplaceMode();
    }

    boolean isDedupKey(int columnIndex) {
        return tableWriter.getMetadata().isDedupKey(columnIndex);
    }

    void o3BumpErrorCount(boolean oom) {
        tableWriter.o3BumpErrorCount(oom);
    }

    void o3ClearDedupBlock(long dedupColSinkAddr) {
        final DedupColumnCommitAddresses dedupColumnCommitAddresses = tableWriter.getDedupCommitAddresses();
        if (dedupColumnCommitAddresses != null) {
            dedupColumnCommitAddresses.clear(dedupColSinkAddr);
        }
    }

    void o3ClockDownPartitionUpdateCount() {
        tableWriter.o3ClockDownPartitionUpdateCount();
    }

    void o3CountDownDoneLatch() {
        tableWriter.o3CountDownDoneLatch();
    }

    long o3ColumnTop(long partitionTimestamp, int columnIndex, long defaultValue) {
        return getColumnTop(partitionTimestamp, columnIndex, defaultValue);
    }

    int o3CommitMode() {
        return tableWriter.getConfiguration().getCommitMode();
    }

    int o3DedupColumnCount() {
        return getDedupColumnCount();
    }

    String o3DbRoot() {
        return tableWriter.getConfiguration().getDbRoot();
    }

    FilesFacade o3FilesFacade() {
        return getFilesFacade();
    }

    void o3OpenIndexWriter(IndexWriter indexWriter, long keyFd, long valueFd, boolean isInit, int indexBlockCapacity) {
        openIndexWriter(indexWriter, keyFd, valueFd, isInit, indexBlockCapacity);
    }

    int o3MkDirMode() {
        return tableWriter.getConfiguration().getMkDirMode();
    }

    int o3PartitionEncoderParquetCompressionCodec() {
        return tableWriter.getConfiguration().getPartitionEncoderParquetCompressionCodec();
    }

    int o3PartitionEncoderParquetCompressionLevel() {
        return tableWriter.getConfiguration().getPartitionEncoderParquetCompressionLevel();
    }

    int o3PartitionEncoderParquetDataPageSize() {
        return tableWriter.getConfiguration().getPartitionEncoderParquetDataPageSize();
    }

    double o3PartitionEncoderParquetBloomFilterFpp() {
        return tableWriter.getConfiguration().getPartitionEncoderParquetBloomFilterFpp();
    }

    double o3PartitionEncoderParquetMinCompressionRatio() {
        return tableWriter.getConfiguration().getPartitionEncoderParquetMinCompressionRatio();
    }

    long o3PartitionEncoderParquetO3RewriteUnusedMaxBytes() {
        return tableWriter.getConfiguration().getPartitionEncoderParquetO3RewriteUnusedMaxBytes();
    }

    double o3PartitionEncoderParquetO3RewriteUnusedRatio() {
        return tableWriter.getConfiguration().getPartitionEncoderParquetO3RewriteUnusedRatio();
    }

    boolean o3PartitionEncoderParquetRawArrayEncoding() {
        return tableWriter.getConfiguration().isPartitionEncoderParquetRawArrayEncoding();
    }

    int o3PartitionEncoderParquetRowGroupSize() {
        return tableWriter.getConfiguration().getPartitionEncoderParquetRowGroupSize();
    }

    boolean o3PartitionEncoderParquetStatisticsEnabled() {
        return tableWriter.getConfiguration().isPartitionEncoderParquetStatisticsEnabled();
    }

    boolean o3SymbolHasNull(int columnIndex) {
        return tableWriter.getSymbolMapWriter(columnIndex).getNullFlag();
    }

    int o3SymbolCount(int columnIndex) {
        return tableWriter.getSymbolMapWriter(columnIndex).getSymbolCount();
    }

    MemoryR o3SymbolOffsetsMemory(int columnIndex) {
        return tableWriter.getSymbolMapWriter(columnIndex).getSymbolOffsetsMemory();
    }

    MemoryR o3SymbolValuesMemory(int columnIndex) {
        return tableWriter.getSymbolMapWriter(columnIndex).getSymbolValuesMemory();
    }

    IndexWriter o3UnsafeGetIndexWriterForLastPartitionOnly(int columnIndex) {
        return tableWriter.getIndexWriter(columnIndex);
    }

    int o3WriterFileOpenOpts() {
        return tableWriter.getConfiguration().getWriterFileOpenOpts();
    }
}
