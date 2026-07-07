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
import io.questdb.std.ObjectStackPool;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.tasks.PostingSealPurgeTask;

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

    default void commitDense() {
        commit();
    }

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
            LongList coveredColumnAuxAddrs,
            LongList coveredColumnTops,
            IntList coveredColumnShifts,
            IntList coveredColumnIndices,
            IntList coveredColumnTypes,
            int coverCount,
            int timestampColumnIndex
    ) {
    }

    /**
     * Discards the writer's in-memory state so the caller can re-add entries
     * from authoritative source data. Used to evict stale (key, rowId) pairs
     * left by replace-range, dedup-replace, or O3 splits where the same row
     * id takes a different value across writes -- cases the chain cannot
     * surgically rewind because the stale entries sit inside the live row
     * range.
     * <p>
     * Expected caller pattern:
     * <pre>
     *     writer.discardForRebuild();
     *     for (...) writer.add(key, rowId);   // re-add from .d file
     *     writer.commit();                    // flush as fresh gen 0
     *     writer.seal();                      // rotate the value file
     * </pre>
     * The call rotates the writer's value file to a fresh sealTxn so the
     * subsequent {@code commit()} appends a new chain entry rather than
     * mutating the existing head in place. The OLD chain entry stays on
     * disk as a prev entry until concurrent readers release it; the OLD
     * value file is queued for the seal-purge job after the new entry
     * lands. The trailing {@code seal()} rotates again and writes the
     * dense final form.
     * <p>
     * Unlike {@link #truncate()}, this call preserves the chain head and
     * the OLD value file so concurrent readers with active mmaps stay
     * safe. The .pk header is not rewritten; only the writer's value-file
     * mapping moves to the new sealTxn.
     * <p>
     * Default is no-op for index types that don't accumulate stale state
     * (e.g. BitmapIndexWriter persists every add immediately and has no
     * chain).
     */
    default void discardForRebuild() {
    }

    /**
     * Moves unsafe finite-future purge entries into a TableWriter-owned queue
     * before this index writer is closed or reopened.
     */
    default void drainPendingFuturePurges(
            ObjList<PostingSealPurgeTask> sink,
            ObjectStackPool<PostingSealPurgeTask> pool,
            TableToken tableToken,
            int partitionBy,
            int timestampType,
            long currentTableTxn
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
     * Folds any tentative on-disk state produced by an fd-based O3 commit
     * into this writer's view so the subsequent seal sees the full set of
     * gens. Must be called after {@code of()} and before
     * {@code seal()}/{@code rebuildSidecars()} on the seal path. No-op for
     * index types that don't produce tentative state.
     */
    default void mergeTentativeIntoActiveIfAny() {
    }

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

    default void of(Path path, CharSequence name, long columnNameTxn, long partitionTimestamp, long partitionNameTxn) {
        of(path, name, columnNameTxn);
    }

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

    /**
     * Opens the writer using path context previously installed via
     * {@link #setO3PathContext}. Used by the O3 copy path for index types
     * (currently POSTING) that prefer path-based file management to fd
     * preopen. Default is no-op; index types that don't override stick to
     * fd-based {@link #of(CairoConfiguration, long, long, boolean, int)}.
     */
    default void openFromO3Context(boolean isInit) {
    }

    default void publishPendingPurges(
            MessageBus messageBus,
            TableToken tableToken,
            int partitionBy,
            int timestampType,
            long currentTableTxn
    ) {
    }

    default void rebuildSidecars() {
    }

    /**
     * Drop the read-side state set up for the most recent seal but keep
     * the covering schema intact. See
     * {@link io.questdb.cairo.idx.PostingIndexWriter#releaseCoveredColumnReadMappings()}.
     */
    default void releaseCoveredColumnReadMappings() {
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

    default void setCoveredColumnAddrSizes(LongList dataSizes, LongList auxSizes) {
    }

    default void setCoveredColumnNameTxns(LongList txns) {
    }

    /**
     * Set the table-level {@code _txn} that the next {@link #of(Path, CharSequence, long)}
     * (or any of its overloads) should treat as the most recently committed
     * transaction. Posting v2 readers writers use this to drive the
     * crash-recovery walk: any chain entry with {@code txnAtSeal > currentTableTxn}
     * was published by a previous attempt that distressed before
     * {@code txWriter.commit()}, and is dropped on the next open.
     * <p>
     * Default is no-op: BitmapIndexWriter has no chain to recover. The setter
     * must be called <i>before</i> {@code of(...)} on every reopen that
     * follows a possibly-distressed close. Pass a value &lt; 0 (or never call
     * the setter) to skip recovery.
     */
    default void setCurrentTableTxn(long currentTableTxn) {
    }

    void setMaxValue(long maxValue);

    /**
     * Set the table-level {@code _txn} that the next chain entry this writer
     * publishes (during {@link #commit()} or {@link #seal()}) should record
     * as its {@code txnAtSeal}. Posting v2 readers pin via the scoreboard
     * and pick the chain entry with {@code txnAtSeal <= pinnedTxn}, so the
     * value supplied here defines when the upcoming entry becomes visible.
     * <p>
     * Conventionally callers pass {@code txWriter.getTxn() + 1} (the
     * forthcoming commit's txn) so the entry takes effect exactly when its
     * encompassing transaction commits.
     * <p>
     * Default is no-op: BitmapIndexWriter has no chain. The setter must be
     * called <i>before</i> the publish that should consume it (typically
     * {@code commit()}); the writer reads the value once and resets the
     * field, so a stale value cannot drive a later publish unless the
     * setter is called again.
     */
    default void setNextTxnAtSeal(long txnAtSeal) {
    }

    /**
     * Installs partition path, column name, columnNameTxn and the upcoming
     * table txn that the next {@link #openFromO3Context} call will consume.
     * Lets the O3 copy path defer the actual {@code of(...)} to the worker
     * while plumbing path information from the publisher.
     * <p>
     * Default is no-op: BitmapIndexWriter still uses fd-based of() in the
     * O3 path. POSTING overrides to stash these for the path-based open
     * that follows in {@link #openFromO3Context}.
     */
    default void setO3PathContext(Path path, CharSequence name, long columnNameTxn, long upcomingTxn) {
    }

    /**
     * Records the partition timestamp and name-txn on the writer so a deferred
     * seal-purge task can reconstruct the value-file directory after the writer
     * is freed. The path-based of(...) overloads used by the parquet rebuild do
     * not carry these. No-op for writers without a seal-purge outbox (e.g. BITMAP).
     */
    default void setPartitionContext(long partitionTimestamp, long partitionNameTxn) {
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
