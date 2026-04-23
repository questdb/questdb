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

import io.questdb.MessageBus;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableToken;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Mutable;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

import java.io.Closeable;

/**
 * Interface for column index writers.
 * <p>
 * Different index types (BITMAP, POSTING, POSTING DELTA) have different writer
 * implementations that all conform to this interface.
 */
public interface IndexWriter extends Closeable, Mutable {

    /**
     * Adds a key-value pair to the index.
     *
     * @param key   the symbol key (must be >= 0)
     * @param value the row ID value
     */
    void add(int key, long value);

    default void clearCovering() {
    }

    /**
     * Closes the index without truncating files.
     * Default implementation just calls close().
     */
    void closeNoTruncate();

    /**
     * Commits the index to disk based on the configuration's commit mode.
     */
    void commit();

    default void configureCovering(
            ObjList<CharSequence> coveredColumnNames,
            LongList coveredColumnNameTxns,
            LongList coveredColumnTops,
            IntList coveredColumnShifts,
            IntList coveredColumnIndices,
            IntList coveredColumnTypes,
            int timestampColumnIndex
    ) {
    }

    default void configureCovering(
            LongList coveredColumnAddrs,
            LongList coveredColumnTops,
            IntList coveredColumnShifts,
            IntList coveredColumnIndices,
            IntList coveredColumnTypes,
            int coverCount
    ) {
    }

    /**
     * Returns the index type for this writer.
     *
     * @return the index type constant from {@link IndexType}
     */
    byte getIndexType();

    /**
     * Returns the number of distinct keys in the index.
     *
     * @return key count
     */
    int getKeyCount();

    /**
     * Returns the maximum row ID value stored in the index.
     *
     * @return max value, or -1 if no values have been written
     */
    long getMaxValue();

    /**
     * Returns true if the index is open and ready for writing.
     *
     * @return true if open
     */
    boolean isOpen();

    /**
     * Opens the index writer for the given column using file descriptors.
     * <p>
     * This method is only supported by BitmapIndexWriter. Other implementations
     * should throw UnsupportedOperationException.
     *
     * @param configuration Cairo configuration
     * @param keyFd         file descriptor for the key file
     * @param valueFd       file descriptor for the value file
     * @param init          true to initialize a new index, false to open existing
     * @param blockCapacity the value block capacity (for new index initialization)
     */
    void of(CairoConfiguration configuration, long keyFd, long valueFd, boolean init, int blockCapacity);

    /**
     * Opens the index writer for the given column using file paths.
     *
     * @param path          base path
     * @param name          column name
     * @param columnNameTxn column transaction number
     */
    void of(Path path, CharSequence name, long columnNameTxn);

    /**
     * Opens the index writer for the given column using file paths, optionally creating a new index.
     * <p>
     * The semantics of the last parameter varies by implementation:
     * - BitmapIndexWriter: uses it as blockCapacity (0 = open existing)
     * - Other writers: interpret non-zero as "create new"
     *
     * @param path          base path
     * @param name          column name
     * @param columnNameTxn column transaction number
     * @param create        for BitmapIndexWriter: blockCapacity; for others: true to create new
     */
    default void of(Path path, CharSequence name, long columnNameTxn, int create) {
        of(path, name, columnNameTxn, create != 0);
    }

    /**
     * Opens the index writer for the given column using file paths, optionally creating a new index.
     *
     * @param path          base path
     * @param name          column name
     * @param columnNameTxn column transaction number
     * @param create        true to create a new index, false to open existing
     */
    default void of(Path path, CharSequence name, long columnNameTxn, boolean create) {
        // Default implementation opens without create flag
        of(path, name, columnNameTxn);
    }

    default void publishPendingPurges(
            MessageBus messageBus,
            TableToken tableToken,
            long partitionTimestamp,
            long partitionNameTxn,
            int partitionBy,
            int timestampType,
            long currentTableTxn
    ) {
    }

    default void rebuildSidecars() {
    }

    /**
     * Rolls back values that are strictly greater than the given row.
     * <p>
     * This operation is only supported by some index implementations.
     * Unsupported implementations throw UnsupportedOperationException.
     *
     * @param row the maximum row to keep (exclusive)
     */
    void rollbackConditionally(long row);

    /**
     * Rolls back values to keep only values less than or equal to maxValue.
     * <p>
     * This operation is only supported by some index implementations.
     * Unsupported implementations throw UnsupportedOperationException.
     *
     * @param maxValue maximum value allowed in index (inclusive)
     */
    void rollbackValues(long maxValue);

    default void seal() {
    }

    default void sealIfMultiGen(int threshold) {
    }

    default void setCoveredColumnNameTxns(LongList txns) {
    }

    void setMaxValue(long maxValue);

    /**
     * Announces the publish-table-txn for the next seal that may trigger
     * during this commit cycle. POSTING uses it to bound the scoreboard
     * window when scheduling a purge of the previous sealed files. Default
     * no-op for index types without seal-versioned files (BITMAP).
     */
    default void setPendingPublishTableTxn(long publishTableTxn) {
    }

    /**
     * Syncs the index files to disk.
     *
     * @param async true for async sync, false for sync
     */
    void sync(boolean async);

    default void tombstoneCover(int writerIdx) {
    }

    /**
     * Truncates the index, removing all data.
     */
    void truncate();
}
