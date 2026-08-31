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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

public final class IndexFactory {

    private IndexFactory() {
    }

    /**
     * Builds a reader over one partition's index for one column.
     * <p>
     * {@code path} is the partition DIRECTORY, which is where both forms' files
     * live: the native chain's {@code .pk} / {@code .pv} / {@code .pc*} and the
     * parquet form's {@code <col>.pidx.<indexTxn>.parquet} plus its {@code _im}
     * sit side by side in it, and a parquet partition's {@code data.parquet} is
     * that same directory plus a name. Only the file NAME distinguishes the two
     * forms, and {@code indexForm} is what selects it.
     * <p>
     * <b>{@code indexForm} is the partition's published token, never the
     * configured format.</b> {@code cairo.posting.index.parquet.partition.format}
     * says what the NEXT seal will write; it says nothing about what this
     * partition already carries, and the two disagree in both directions -- see
     * {@code TableReader.getPartitionIndexForm}, which is the only thing callers
     * should pass here.
     * <p>
     * {@code IndexType.BITMAP} ignores {@code indexForm}, {@code indexTxn} and
     * {@code imFileSize}: only a posting seal publishes a covering-index token,
     * so a bitmap index never becomes parquet. That is also what keeps the
     * unguarded {@code getKeyBaseAddress} / {@code getValueBaseAddress} consumer
     * -- {@code LatestByAllIndexedRecordCursor}, whose factory is built only for
     * {@code IndexType.BITMAP} -- away from a reader that answers those with 0.
     *
     * @param indexForm  {@link PostingIndexUtils#PARQUET_INDEX_FORMAT_NATIVE} or
     *                   {@link PostingIndexUtils#PARQUET_INDEX_FORMAT_PARQUET}
     * @param indexTxn   the {@code index_txn} naming the parquet-form artifact pair,
     *                   or {@code -1} under the native form
     * @param imFileSize the {@code _im} size the same token publishes, or {@code 0}
     *                   under the native form
     */
    public static IndexReader createReader(
            byte indexType,
            int direction,
            CairoConfiguration configuration,
            Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long pinnedTableTxn,
            byte indexForm,
            long indexTxn,
            long imFileSize
    ) {
        return switch (indexType) {
            case IndexType.BITMAP -> direction == IndexReader.DIR_FORWARD
                    ? new BitmapIndexFwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop)
                    : new BitmapIndexBwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop);
            case IndexType.POSTING, IndexType.POSTING_DELTA, IndexType.POSTING_EF -> {
                if (indexForm == PostingIndexUtils.PARQUET_INDEX_FORMAT_PARQUET) {
                    final AbstractParquetPostingIndexReader reader = direction == IndexReader.DIR_FORWARD
                            ? new ParquetPostingIndexFwdReader()
                            : new ParquetPostingIndexBwdReader();
                    try {
                        reader.ofParquet(
                                configuration, path, columnName, columnNameTxn, partitionTxn, columnTop,
                                metadata, columnVersionReader, partitionTimestamp, indexTxn, imFileSize
                        );
                    } catch (Throwable th) {
                        // ofParquet already released whatever it had bound; this
                        // only stops the half-built reader escaping to the caller,
                        // which would otherwise leak the instance itself.
                        reader.close();
                        throw th;
                    }
                    reader.setPinnedTableTxn(pinnedTableTxn);
                    yield reader;
                }
                yield direction == IndexReader.DIR_FORWARD
                        ? new PostingIndexFwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp, pinnedTableTxn)
                        : new PostingIndexBwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp, pinnedTableTxn);
            }
            default -> throw unsupportedIndexType(indexType);
        };
    }

    public static IndexWriter createWriter(byte indexType, CairoConfiguration configuration) {
        return switch (indexType) {
            case IndexType.BITMAP -> new BitmapIndexWriter(configuration);
            case IndexType.POSTING -> new PostingIndexWriter(configuration);
            case IndexType.POSTING_DELTA -> new PostingIndexWriter(configuration, PostingIndexUtils.ENCODING_DELTA);
            case IndexType.POSTING_EF -> new PostingIndexWriter(configuration, PostingIndexUtils.ENCODING_EF);
            default -> throw unsupportedIndexType(indexType);
        };
    }

    // blockCapacity is only meaningful for BITMAP; POSTING uses its own constant.
    public static void initKeyMemory(byte indexType, MemoryMA keyMem, int blockCapacity) {
        switch (indexType) {
            case IndexType.BITMAP -> BitmapIndexWriter.initKeyMemory(keyMem, blockCapacity);
            case IndexType.POSTING, IndexType.POSTING_DELTA, IndexType.POSTING_EF ->
                    PostingIndexWriter.initKeyMemory(keyMem);
            default -> throw unsupportedIndexType(indexType);
        }
    }

    public static LPSZ keyFileName(byte indexType, Path path, CharSequence columnName, long columnNameTxn) {
        return switch (indexType) {
            case IndexType.BITMAP -> BitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
            case IndexType.POSTING, IndexType.POSTING_DELTA, IndexType.POSTING_EF ->
                    PostingIndexUtils.keyFileName(path, columnName, columnNameTxn);
            default -> throw unsupportedIndexType(indexType);
        };
    }

    public static LPSZ valueFileName(byte indexType, Path path, CharSequence columnName, long columnNameTxn, long sealTxn) {
        return switch (indexType) {
            case IndexType.BITMAP -> BitmapIndexUtils.valueFileName(path, columnName, columnNameTxn);
            case IndexType.POSTING, IndexType.POSTING_DELTA, IndexType.POSTING_EF ->
                    PostingIndexUtils.valueFileName(path, columnName, columnNameTxn, sealTxn);
            default -> throw unsupportedIndexType(indexType);
        };
    }

    private static CairoException unsupportedIndexType(byte indexType) {
        return CairoException.critical(0).put("unsupported index type: ").put(IndexType.nameOf(indexType));
    }
}
