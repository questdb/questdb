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
            long partitionTimestamp
    ) {
        return switch (indexType) {
            case IndexType.BITMAP -> direction == IndexReader.DIR_FORWARD
                    ? new BitmapIndexFwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop)
                    : new BitmapIndexBwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop);
            case IndexType.POSTING, IndexType.POSTING_DELTA, IndexType.POSTING_EF ->
                    direction == IndexReader.DIR_FORWARD
                            ? new PostingIndexFwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp)
                            : new PostingIndexBwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop, metadata, columnVersionReader, partitionTimestamp);
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
