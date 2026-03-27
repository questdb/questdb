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

package io.questdb.cairo.idx;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

import static io.questdb.cairo.idx.PostingIndexUtils.BLOCK_CAPACITY;


/**
 * Factory for creating index readers and writers based on index type.
 * <p>
 * This class provides a centralized way to instantiate the appropriate
 * index implementation based on the column's index type.
 */
public final class IndexFactory {

    private IndexFactory() {
        // Utility class, no instances
    }

    /**
     * Returns the key file name for the given index type.
     *
     * @param indexType     the type of index (BITMAP, POSTING, FSST)
     * @param path          the path to append the file name to
     * @param columnName    the column name
     * @param columnNameTxn the column name transaction number
     * @return the path with the key file name appended
     */
    public static LPSZ keyFileName(byte indexType, Path path, CharSequence columnName, long columnNameTxn) {
        return switch (indexType) {
            case IndexType.BITMAP -> BitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
            case IndexType.POSTING -> PostingIndexUtils.keyFileName(path, columnName, columnNameTxn);
            case IndexType.FSST -> FSSTBitmapIndexUtils.keyFileName(path, columnName, columnNameTxn);
            default -> throw CairoException.critical(0)
                    .put("unsupported index type for key file: ").put(IndexType.nameOf(indexType));
        };
    }

    /**
     * Returns the value file name for the given index type.
     *
     * @param indexType     the type of index (BITMAP, POSTING, FSST)
     * @param path          the path to append the file name to
     * @param columnName    the column name
     * @param columnNameTxn the column name transaction number
     * @return the path with the value file name appended
     */
    public static LPSZ valueFileName(byte indexType, Path path, CharSequence columnName, long columnNameTxn) {
        return switch (indexType) {
            case IndexType.BITMAP -> BitmapIndexUtils.valueFileName(path, columnName, columnNameTxn);
            case IndexType.POSTING -> PostingIndexUtils.valueFileName(path, columnName, columnNameTxn);
            case IndexType.FSST -> FSSTBitmapIndexUtils.valueFileName(path, columnName, columnNameTxn);
            default -> throw CairoException.critical(0)
                    .put("unsupported index type for value file: ").put(IndexType.nameOf(indexType));
        };
    }

    /**
     * Initializes the key memory for a new index of the given type.
     *
     * @param indexType     the type of index (BITMAP, POSTING, FSST)
     * @param keyMem        the memory to initialize
     * @param blockCapacity the value block capacity (used by BITMAP type)
     * @throws CairoException if the index type is not supported
     */
    public static void initKeyMemory(byte indexType, MemoryMA keyMem, int blockCapacity) {
        switch (indexType) {
            case IndexType.BITMAP -> BitmapIndexWriter.initKeyMemory(keyMem, blockCapacity);
            case IndexType.POSTING -> PostingIndexWriter.initKeyMemory(keyMem, BLOCK_CAPACITY);
            case IndexType.FSST -> FSSTBitmapIndexWriter.initKeyMemory(keyMem, FSSTBitmapIndexUtils.DEFAULT_BLOCK_VALUES);
            case IndexType.NONE -> throw CairoException.critical(0)
                    .put("cannot initialize key memory for index type NONE");
            default -> throw CairoException.critical(0)
                    .put("unsupported index type: ").put(IndexType.nameOf(indexType));
        }
    }

    /**
     * Creates a new index reader for the given index type and direction.
     *
     * @param indexType     the type of index (BITMAP, POSTING, FSST)
     * @param direction     the read direction (BitmapIndexReader.DIR_FORWARD or DIR_BACKWARD)
     * @param configuration Cairo configuration
     * @param path          base path to the index files
     * @param columnName    name of the column
     * @param columnNameTxn column name transaction number
     * @param partitionTxn  partition transaction number
     * @param columnTop     column top value
     * @return a new BitmapIndexReader instance
     * @throws CairoException if the index type is not supported
     */
    public static BitmapIndexReader createReader(
            byte indexType,
            int direction,
            CairoConfiguration configuration,
            Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    ) {
        return switch (indexType) {
            case IndexType.BITMAP -> direction == BitmapIndexReader.DIR_FORWARD
                    ? new BitmapIndexFwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop)
                    : new BitmapIndexBwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop);
            case IndexType.POSTING -> direction == BitmapIndexReader.DIR_FORWARD
                    ? new PostingIndexFwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop)
                    : new PostingIndexBwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop);
            case IndexType.FSST -> direction == BitmapIndexReader.DIR_FORWARD
                    ? new FSSTBitmapIndexFwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop)
                    : new FSSTBitmapIndexBwdReader(configuration, path, columnName, columnNameTxn, partitionTxn, columnTop);
            case IndexType.NONE -> throw CairoException.critical(0)
                    .put("cannot create reader for index type NONE");
            default -> throw CairoException.critical(0)
                    .put("unsupported index type: ").put(IndexType.nameOf(indexType));
        };
    }

    /**
     * Creates a new, uninitialized index writer for the given index type.
     * The writer must be initialized using one of the {@code of()} methods before use.
     *
     * @param indexType     the type of index (BITMAP, POSTING, FSST)
     * @param configuration Cairo configuration
     * @return a new IndexWriter instance
     * @throws CairoException if the index type is not supported
     */
    public static IndexWriter createWriter(byte indexType, CairoConfiguration configuration) {
        return switch (indexType) {
            case IndexType.BITMAP -> new BitmapIndexWriter(configuration);
            case IndexType.POSTING -> new PostingIndexWriter(configuration);
            case IndexType.FSST -> new FSSTBitmapIndexWriter(configuration);
            case IndexType.NONE -> throw CairoException.critical(0)
                    .put("cannot create writer for index type NONE");
            default -> throw CairoException.critical(0)
                    .put("unsupported index type: ").put(IndexType.nameOf(indexType));
        };
    }
}
