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
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexMetaFileReader;
import io.questdb.cairo.IndexMetaFileWriter;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.Transient;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Path;

/**
 * Reads a covering index that was sealed into a partition's
 * {@code <col>.pidx.<indexTxn>.parquet} plus its {@code <col>.pidx.<indexTxn>._im}
 * sidecar, rather than into the native {@code .pk} / {@code .pv} / {@code .pc*}
 * chain that {@link AbstractPostingIndexReader} serves.
 * <p>
 * The artifact pair is named by an {@code index_txn}, which the nine-argument
 * {@link IndexReader#of} does not carry -- so this reader is bound through
 * {@link #ofParquet} instead, off the token
 * {@code TableReader.getPartitionIndexForm} resolved from the partition's own
 * {@code _pm} mapping at partition-open time. {@link #of} therefore throws.
 * <p>
 * <b>This class answers structure only.</b> The four {@link PostingIndexReader}
 * primitives return their "cannot answer, walk a cursor" sentinels and
 * {@link #getCursor} throws; Phase 2C Task 4 fills them in. The throw is
 * deliberate: an empty cursor here would turn every indexed query over a
 * parquet-sealed partition into a silent empty result, which is exactly the
 * failure the refusal this dispatch replaces existed to prevent.
 */
public abstract class AbstractParquetPostingIndexReader implements PostingIndexReader {
    private static final Log LOG = LogFactory.getLog(AbstractParquetPostingIndexReader.class);
    protected final ParquetFileDecoder decoder = new ParquetFileDecoder();
    protected final IndexMetaFileReader imReader = new IndexMetaFileReader();
    protected final DirectIntList projection = new DirectIntList(4, MemoryTag.NATIVE_DEFAULT);
    // keepClosed: a pooled reader is closed and rebound many times, and close()
    // destroys the native buffers. Allocating eagerly here would leave every
    // rebound reader with a destroyed pointer, which the decoder reports as
    // "row group buffers pointer is null". reopen() before each decode is the
    // pattern ParquetTimestampFinder and ReadParquetRecordCursor use.
    protected final RowGroupBuffers rowGroupBuffers =
            new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER, true);
    protected long columnTop;
    protected long indexTxn = -1;
    protected long partitionTimestamp;
    protected long pidxAddr;
    protected long pidxSize;
    private long columnNameTxn = -1;
    protected CharSequence columnName;
    private FilesFacade ff;
    private boolean frozen;
    private long imFileSize;
    private boolean open;
    private long partitionTxn = -1;
    private long pinnedTableTxn = Long.MAX_VALUE;

    @Override
    public void close() {
        open = false;
        Misc.free(decoder);
        Misc.free(rowGroupBuffers);
        Misc.free(projection);
        if (pidxAddr != 0) {
            ff.munmap(pidxAddr, pidxSize, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            pidxAddr = 0;
            pidxSize = 0;
        }
        // Releases the _im mapping this reader owns; safe on a reader that was
        // never bound and safe to repeat.
        imReader.clear();
    }

    /**
     * Projects exactly the two synthetic columns every index parquet carries:
     * {@code key_id} as INT and {@code row_id} as LONG. Their positions come
     * from the {@code _im} header rather than being assumed, because the seal
     * is free to move them and the header is what names them.
     * <p>
     * Rebuilt on each call rather than cached because {@code DirectIntList} is
     * cheap to refill and a stale projection would decode the wrong columns
     * after a rebind.
     */
    protected DirectIntList decodeProjection() {
        projection.clear();
        projection.add(imReader.getKeyIdColumn());
        projection.add(ColumnType.INT);
        projection.add(imReader.getRowIdColumn());
        projection.add(ColumnType.LONG);
        return projection;
    }

    /**
     * Resolves {@code key} to its inclusive index row-group run through the
     * {@code _im} directory. Pruning level 1: exact, and it reads no byte of
     * the index parquet.
     * <p>
     * The directory answers "which row groups COULD hold k", not "does k
     * exist": the key space is dense and occupancy sparse, so a key falling
     * inside a packed group's key range returns a range whether or not it has
     * postings. Confirming absence costs one row-group decode, which the
     * cursor performs anyway.
     */
    protected long rowGroupRangeForKey(int key) {
        return imReader.getRowGroupRangeForKey(key);
    }

    /**
     * The sentinel is {@link Numbers#LONG_NULL}, NOT {@code -1}: the sole caller
     * tests {@code c != Numbers.LONG_NULL} and then does {@code total += c}, so
     * {@code -1} does not signal a fallback -- it silently subtracts one from a
     * {@code count(*)} answer.
     */
    @Override
    public long countMatchesClamped(int key, long minValue, long nullMaxValue, long maxValueClamped) {
        return Numbers.LONG_NULL;
    }

    @Override
    public long getColumnTop() {
        return columnTop;
    }

    @Override
    public long getColumnTxn() {
        return columnNameTxn;
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        // Direction is the subclass's business: the forward reader overrides
        // this, and the backward one still refuses until Task 6. Leaving the
        // base throwing rather than defaulting to ascending keeps a backward
        // caller from silently receiving forward order.
        throw CairoException.critical(0)
                .put("parquet-form posting index cursor is not implemented for this direction [column=")
                .put(columnName).put(", indexTxn=").put(indexTxn).put(']');
    }

    /**
     * Negative means "no entry". Unlike the two {@code LONG_NULL} sentinels this
     * one IS negative by contract, and {@link AbstractPostingIndexReader} spells
     * it {@code -1}.
     */
    @Override
    public long getEntryMaxValue() {
        return -1;
    }

    /**
     * The committed {@code IM_FILE_SIZE} of the {@code _im} this reader is bound
     * to, as the published token names it.
     */
    public long getImFileSize() {
        return imFileSize;
    }

    /**
     * The {@code index_txn} naming the artifact pair this reader is bound to.
     * {@code TableReader} compares it with the partition's published token to
     * decide whether a cached reader still describes the right generation: a
     * token-only publish moves neither {@code columnNameTxn} nor
     * {@code partitionTxn}, so nothing else here would notice it.
     */
    public long getIndexTxn() {
        return indexTxn;
    }

    /**
     * Native-mmap-shaped and meaningless for a parquet-backed reader, so
     * {@code 0}. Audited callers, both of which tolerate it:
     * {@code LatestByAllIndexedRecordCursor}, whose factory is gated on
     * {@code IndexType.BITMAP} at both construction sites in
     * {@code SqlCodeGenerator} so a POSTING reader never reaches it; and
     * {@code TouchTableFunctionFactory}, whose {@code touchMemory} returns 0
     * pages for {@code baseAddress == 0}, degrading {@code touch_table()} over
     * this index to a no-op rather than dereferencing anything.
     */
    @Override
    public long getKeyBaseAddress() {
        return 0;
    }

    @Override
    public int getKeyCount() {
        // KEY_SPACE_SIZE: the exclusive upper bound on key ids, equal to the
        // native reader's keyCountIncludingNulls. NOT a distinct-key count --
        // occupancy is sparse, and a distinct count would make every key above
        // the first report absent with no error anywhere.
        return imReader.getKeySpaceSize();
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getKeyMemorySize() {
        return 0;
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    /**
     * The table {@code _txn} this reader is pinned at, for the snapshot
     * isolation Task 4 onwards needs.
     */
    public long getPinnedTableTxn() {
        return pinnedTableTxn;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getValueBaseAddress() {
        return 0;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    /**
     * @see #getKeyBaseAddress()
     */
    @Override
    public long getValueMemorySize() {
        return 0;
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Always throws. The nine-argument form carries no {@code index_txn} and so
     * cannot name {@code <col>.pidx.<indexTxn>.parquet} or its {@code _im}; use
     * {@link #ofParquet} instead. Unreachable from production --
     * {@code TableReader.getIndexReader} rebinds a parquet-form reader through
     * {@code ofParquet}, and {@code reloadColumnAt} drops one rather than
     * rebinding it -- so this is a programming-error guard, not a code path.
     */
    @Override
    public void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp
    ) {
        throw CairoException.critical(0)
                .put("a parquet-form posting index reader must be bound through ofParquet, which carries the index txn [column=")
                .put(columnName).put(']');
    }

    /**
     * Binds this reader to {@code <col>.pidx.<indexTxn>._im} and
     * {@code <col>.pidx.<indexTxn>.parquet} in the partition directory
     * {@code path} names.
     *
     * @param path       positioned at the partition directory; restored on return
     * @param indexTxn   the {@code index_txn} the partition's {@code _pm} publishes
     *                   for this column
     * @param imFileSize the {@code _im} size the same token publishes, cross-checked
     *                   against the one the file itself commits
     */
    public void ofParquet(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop,
            RecordMetadata metadata,
            ColumnVersionReader columnVersionReader,
            long partitionTimestamp,
            long indexTxn,
            long imFileSize
    ) {
        close();
        this.ff = configuration.getFilesFacade();
        this.columnName = columnName;
        this.columnNameTxn = columnNameTxn;
        this.partitionTxn = partitionTxn;
        this.columnTop = columnTop;
        this.partitionTimestamp = partitionTimestamp;
        this.indexTxn = indexTxn;
        this.imFileSize = imFileSize;
        final int plen = path.size();
        try {
            final LPSZ imFile = ParquetIndexSeal.indexMetaFileName(path, columnName, indexTxn);
            if (IndexMetaFileReader.openAndMapRO(ff, imFile, imReader) == 0) {
                // The token names this pair, so a missing or uncommitted _im is
                // not a "nothing published yet" state the way it is for a
                // writer-side probe: it is the artifact this snapshot was told
                // to read.
                throw CairoException.critical(0)
                        .put("could not read the covering index _im named by the partition metadata [file=")
                        .put(imFile).put(']');
            }
            if (imReader.getPayloadKind() != IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING) {
                // Only arm N is written today. Decoding an arm B payload with
                // arm N's reader is a wrong-answer class, not a crash, so it
                // has to be refused rather than attempted.
                throw CairoException.critical(0)
                        .put("unsupported covering index payload kind [payloadKind=").put(imReader.getPayloadKind())
                        .put(", expected=").put(IndexMetaFileWriter.PAYLOAD_ROW_PER_POSTING)
                        .put(", file=").put(imFile).put(']');
            }
            if (imReader.getFileSize() != imFileSize) {
                // The token records the _im size the seal committed. A file
                // committing a different one under the same index_txn is
                // corruption: the artifacts are named by index_txn, so nothing
                // legitimate can rewrite one in place at a different size.
                throw CairoException.critical(0)
                        .put("covering index _im size disagrees with the published token [tokenImFileSize=").put(imFileSize)
                        .put(", imFileSize=").put(imReader.getFileSize())
                        .put(", file=").put(imFile).put(']');
            }
            path.trimTo(plen);
            final LPSZ pidxFile = ParquetIndexSeal.indexParquetFileName(path, columnName, indexTxn);
            // The _im's recorded size, never ff.length(): the file on disk can
            // carry bytes past the committed footer, and mapping those would
            // hand the decoder a footer the seal never committed.
            final long size = imReader.getPidxFileSize();
            if (size <= 0) {
                throw CairoException.critical(0)
                        .put("covering index parquet size is not addressable [pidxFileSize=").put(size)
                        .put(", file=").put(pidxFile).put(']');
            }
            pidxAddr = TableUtils.mapRO(ff, pidxFile, LOG, size, MemoryTag.MMAP_PARQUET_PARTITION_DECODER);
            pidxSize = size;
            decoder.of(pidxAddr, pidxSize, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
            open = true;
        } catch (Throwable th) {
            close();
            throw th;
        } finally {
            path.trimTo(plen);
        }
    }

    @Override
    public void populateCacheForKey(int key) {
    }

    @Override
    public void reloadConditionally() {
        // Nothing can move under this reader. The artifact pair is named by
        // index_txn and is never rewritten in place, so a reseal produces a NEW
        // pair rather than growing this one -- unlike the native chain, whose
        // value file grows and whose generation count advances beneath a bound
        // reader. A moved token is therefore a rebind, not a reload, and
        // TableReader.getIndexReader does it by comparing getIndexTxn() with
        // the partition's published token and calling ofParquet.
    }

    @Override
    public long selectKthMatch(int key, long minValue, long nullMaxValue, long maxValueClamped, long k) {
        // As for countMatchesClamped the sentinel is LONG_NULL and NOT -1: a
        // caller that accepts -1 consumes it as an absolute row id.
        return Numbers.LONG_NULL;
    }

    @Override
    public void setFrozen(boolean frozen) {
        this.frozen = frozen;
    }

    @Override
    public void setPinnedTableTxn(long pinnedTableTxn) {
        this.pinnedTableTxn = pinnedTableTxn;
    }
}
