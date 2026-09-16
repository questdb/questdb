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

package io.questdb.cairo;

import io.questdb.std.FilesFacade;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.str.Path;

import java.io.Closeable;

/**
 * Resolves a COMPOSITE partition's PIECES on demand, and is the only thing in the engine that knows a partition can
 * have more than one.
 */
public class PartitionGeometry implements Closeable, Mutable {
    public static final int NO_PARTITION = -1;
    private static final long FLAG_DIRTY = 1L;
    /**
     * Stride of {@link #pieces}: the six longs of the on-disk piece entry, then the piece's cumulative row -
     * the running sum of the row counts before it.
     */
    private static final int LONGS_PER_PIECE = 7;
    private static final int MIN_PIECE_HOLES = 1024;
    private static final int MIN_RESOLVED_BEFORE_EVICT = 256;
    /**
     * Stride of {@link #resolved}, kept sorted by {@link #RES_PARTITION_TS} so the cache is keyed on values that never
     * change for a directory - unlike a partition index, which shifts whenever a partition is inserted or removed.
     */
    private static final int LONGS_PER_RESOLVED = 12;
    private static final int PIECE_CUMULATIVE_LO = 6;
    private static final int PIECE_LAST_WRITE_MICROS = 5;
    private static final int PIECE_ROW_COUNT = 3;
    private static final int PIECE_ROW_OFFSET = 2;
    private static final int PIECE_TS_HI = 1;
    private static final int PIECE_TS_LO = 0;
    private static final int PIECE_WRITER_TXN = 4;
    private static final int RES_COMMITTED_RECORD_SIZE = 7;
    private static final int RES_PARTITION_TS = 0;
    private static final int RES_E = 4;
    private static final int RES_GEOMETRY_VERSION = 11;
    private static final int RES_FLAGS = 9;
    private static final int RES_GEOMETRY_REF = 8;
    private static final int RES_LAST_WRITE_MICROS = 5;
    private static final int RES_NAME_TXN = 1;
    private static final int RES_PIECE_COUNT = 3;
    private static final int RES_PIECE_LO = 2;
    private static final int RES_SEQ_TXN = 10;
    private static final int RES_WRITER_TXN = 6;
    /**
     * The piece array being built by {@link #beginUpdate}/{@link #addPiece}, not yet installed.
     */
    private final LongList pending = new LongList();
    private final LongList pieces = new LongList();
    private final LongList resolved = new LongList();
    private final LongList scratch = new LongList();
    private int dirtyCount;
    private int pendingRec = NO_PARTITION;
    private FilesFacade ff;
    private PartitionGeometryFile geometryFile;
    private int memoryTag;
    private int partitionBy;
    /**
     * Longs of {@link #pieces} no resolved directory points at any more.
     */
    private int pieceHoles;
    /**
     * Slot count {@link #resolved} has to reach before {@link #evictRetiredDirectories} walks it again.
     */
    private int resolvedEvictWatermark = MIN_RESOLVED_BEFORE_EVICT;
    private String tableRoot;
    private int timestampType;
    private TxReader txReader;

    @Override
    public void clear() {
        assert dirtyCount == 0 : "geometry cleared with " + dirtyCount + " dirty directories";
        discard();
    }

    @Override
    public void close() {
        discard();
        geometryFile = Misc.free(geometryFile);
    }

    /**
     * Splits the directory-cumulative row range {@code [rowLo, rowHi)} of {@code partitionIndex} into the FILE row
     * ranges of the pieces it overlaps, appending them to {@code out} as {@code (fileLo, fileHi)} pairs, {@code fileHi}
     * exclusive, in ascending cumulative-row order.
     */
    public void collectPieceFileRanges(int partitionIndex, long rowLo, long rowHi, LongList out) {
        final int pieceCount = getPieceCount(partitionIndex);
        for (int ordinal = findPieceByRow(partitionIndex, rowLo); ordinal < pieceCount && rowLo < rowHi; ordinal++) {
            final long pieceCumLo = getPieceCumulativeLo(partitionIndex, ordinal);
            final long pieceCumHi = pieceCumLo + getPieceRowCount(partitionIndex, ordinal);
            final long subLo = Math.max(rowLo, pieceCumLo);
            final long subHi = Math.min(rowHi, pieceCumHi);
            if (subLo < subHi) {
                final long shift = getPieceShift(partitionIndex, ordinal);
                out.add(subLo + shift, subHi + shift);
                rowLo = subHi;
            }
        }
    }

    /**
     * Drops every cached resolution without the dirty assertion, for a rollback that abandons the
     * in-memory state wholesale and is about to re-read {@code _txn}.
     */
    public void discard() {
        pieces.clear();
        resolved.clear();
        pieceHoles = 0;
        dirtyCount = 0;
        resolvedEvictWatermark = MIN_RESOLVED_BEFORE_EVICT;
    }

    /**
     * The ordinal of the piece owning {@code ts} inside {@code partitionIndex}, by the same floor rule the record level
     * uses: the piece at or below the timestamp.
     */
    public int findPiece(int partitionIndex, long ts) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return 0;
        }
        int lo = 0;
        int hi = (int) resolved.getQuick(res + RES_PIECE_COUNT) - 1;
        int found = 0;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            if (pieceLong(res, mid, PIECE_TS_LO) <= ts) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return found;
    }

    /**
     * The ordinal of the piece holding directory-cumulative row {@code row}.
     */
    public int findPieceByRow(int partitionIndex, long row) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return 0;
        }
        final int count = (int) resolved.getQuick(res + RES_PIECE_COUNT);
        int lo = 0;
        int hi = count - 1;
        int found = count - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            if (pieceLong(res, mid, PIECE_CUMULATIVE_LO) + pieceLong(res, mid, PIECE_ROW_COUNT) > row) {
                found = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return found;
    }

    public long getCommittedRecordSize(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? 0 : resolved.getQuick(res + RES_COMMITTED_RECORD_SIZE);
    }

    /**
     * {@code E}, the furthest file row this directory has ever held, live or dead.
     */
    public long getE(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? txReader.getPartitionSize(partitionIndex) : resolved.getQuick(res + RES_E);
    }

    public long getLastWriteMicros(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? 0 : resolved.getQuick(res + RES_LAST_WRITE_MICROS);
    }

    /**
     * The highest FILE row a live piece of {@code partitionIndex} reaches: {@code max(rowOffset + rowCount)} over its
     * pieces. This, and not {@link #getE}, is how far a reader's mapping of the directory's column files has to go - a
     * reader only ever resolves a row that sits inside a piece, so the dead space between the last live piece and
     * {@code E} is bytes nothing can reach. Keeping the two apart is what lets TRIM-FILES cut those bytes off while
     * readers of the partition's current shape are still running.
     */
    public long getLiveFileExtent(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return txReader.getPartitionSize(partitionIndex);
        }
        final int count = (int) resolved.getQuick(res + RES_PIECE_COUNT);
        long extent = 0;
        for (int p = 0; p < count; p++) {
            extent = Math.max(extent, pieceLong(res, p, PIECE_ROW_OFFSET) + pieceLong(res, p, PIECE_ROW_COUNT));
        }
        return extent;
    }

    public long getLiveRows(int partitionIndex) {
        return txReader.getPartitionSize(partitionIndex);
    }

    public int getPieceCount(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? 1 : (int) resolved.getQuick(res + RES_PIECE_COUNT);
    }

    /**
     * Cumulative row at which piece {@code ordinal} of {@code partitionIndex} starts, in the directory's own
     * {@code [0, liveRows)} space.
     */
    public long getPieceCumulativeLo(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return 0;
        }
        return pieceLong(res, ordinal, PIECE_CUMULATIVE_LO);
    }

    /**
     * When this piece's bytes last moved. Unlike {@link #getLastWriteMicros(int)}, which every commit
     * refreshes for the whole partition, this stays put while the piece does.
     */
    public long getPieceLastWriteMicros(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return Numbers.LONG_NULL;
        }
        return pieceLong(res, ordinal, PIECE_LAST_WRITE_MICROS);
    }

    public long getPieceRowCount(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            assert ordinal == 0;
            return txReader.getPartitionSize(partitionIndex);
        }
        return pieceLong(res, ordinal, PIECE_ROW_COUNT);
    }

    public long getPieceRowOffset(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            assert ordinal == 0;
            return 0;
        }
        return pieceLong(res, ordinal, PIECE_ROW_OFFSET);
    }

    /**
     * The signed shift that turns a directory-cumulative row into a file row for this piece: {@code rowOffset -
     * cumulativeLo}.
     */
    public long getPieceShift(int partitionIndex, int ordinal) {
        return getPieceRowOffset(partitionIndex, ordinal) - getPieceCumulativeLo(partitionIndex, ordinal);
    }

    public long getPieceTimestampHi(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            assert ordinal == 0;
            return Numbers.LONG_NULL;
        }
        return pieceLong(res, ordinal, PIECE_TS_HI);
    }

    public long getPieceTimestampLo(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            assert ordinal == 0;
            return txReader.getPartitionTimestampByIndex(partitionIndex);
        }
        return pieceLong(res, ordinal, PIECE_TS_LO);
    }

    /**
     * The txn that last moved this piece's bytes, or -1 when unknown.
     */
    public long getPieceWriterTxn(int partitionIndex, int ordinal) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return -1L;
        }
        return pieceLong(res, ordinal, PIECE_WRITER_TXN);
    }

    /**
     * The partition's last-modifying seqTxn, as recorded in its committed {@code _geometry} record, or -1 when unknown.
     * <p>
     * Resolves, like every other accessor here. A cache-only lookup would answer -1 for a partition the caller has not
     * already opened - and {@code ShowPartitionsRecordCursorFactory} reads this BEFORE the accessors that do resolve, so
     * the same query would report the stamp on a warm reader and null on a cold one. It would also miss a re-read: the
     * cache is keyed on timestamp and name txn alone, so a slot left over from a superseded geometry generation answers
     * with the stamp that generation carried.
     */
    public long getSeqTxn(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? -1 : resolved.getQuick(res + RES_SEQ_TXN);
    }

    public long getWriterTxn(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        return res < 0 ? -1 : resolved.getQuick(res + RES_WRITER_TXN);
    }

    public boolean hasDirty() {
        return dirtyCount > 0;
    }

    /**
     * Whether a {@link #publish} of a {@code pieceCount}-piece record for {@code partitionIndex} has a generation to
     * land on. Answers the SAME question {@link #publish} answers for itself a moment later, off the same {@link
     * #nextPublishGeneration} decision, so the two cannot drift: {@code false} here means that publish would throw
     * "partition geometry generations exhausted".
     * <p>
     * Callers ask BEFORE they write the commit's bytes, because the answer chooses which of two ways the commit is
     * written: growing the chain in this directory, or assembling the partition afresh under a new {@code nameTxn},
     * where no {@code _geometry} file exists and all sixteen generations are free again. A caller that does not ask
     * is not silently wrong - it simply meets publish's exception, which is the pre-existing behaviour.
     * <p>
     * {@code pieceCount} may overstate the record publish will actually write (a caller counting planned actions
     * cannot know which of them fold or drop). That is safe in one direction only, and this is that direction: a
     * larger record can only bring the size-cap rotation forward, so a {@code true} answer holds for every smaller
     * record too.
     */
    public boolean hasGenerationForNextPublish(int partitionIndex, int pieceCount) {
        final long committedRef = txReader.getGeometryRef(partitionIndex);
        final long committedRecordSize = getCommittedRecordSize(partitionIndex);
        final Path path = Path.getThreadLocal(tableRoot);
        TableUtils.setPathForNativePartition(
                path,
                timestampType,
                partitionBy,
                txReader.getPartitionTimestampByIndex(partitionIndex),
                txReader.getPartitionNameTxn(partitionIndex)
        );
        return nextPublishGeneration(path, committedRef, committedRecordSize, PartitionGeometryFile.recordSize(pieceCount)) > -1;
    }

    public boolean isComposite(int partitionIndex) {
        final int res = resolveInternal(partitionIndex);
        if (res < 0) {
            return false;
        }
        return resolved.getQuick(res + RES_PIECE_COUNT) > 1
                || resolved.getQuick(res + RES_E) > txReader.getPartitionSize(partitionIndex)
                || pieceLong(res, 0, PIECE_ROW_OFFSET) > 0;
    }

    /**
     * {@code memoryTag} should name the caller's own subsystem - {@code NATIVE_TABLE_READER} for a
     * {@link TableReader}, {@code NATIVE_TABLE_WRITER} for a {@link TxWriter}, {@code NATIVE_O3} for a
     * job-scoped consumer - so the scratch buffer this resolves into is attributed to whoever holds it.
     */
    public PartitionGeometry of(FilesFacade ff, TxReader txReader, String tableRoot, int timestampType, int partitionBy, int memoryTag) {
        this.ff = ff;
        this.txReader = txReader;
        this.tableRoot = tableRoot;
        this.timestampType = timestampType;
        this.partitionBy = partitionBy;
        this.memoryTag = memoryTag;
        discard();
        return this;
    }

    /**
     * Makes {@code partitionIndex}'s pieces resident.
     */
    public void resolve(int partitionIndex) {
        resolveInternal(partitionIndex);
    }

    public void beginUpdate(int partitionIndex) {
        resolveInternal(partitionIndex);
        pending.clear();
        pendingRec = partitionIndex;
    }

    /**
     * Drops what {@link #beginUpdate} opened without installing it.
     */
    public void abandonUpdate() {
        pending.clear();
        pendingRec = NO_PARTITION;
    }

    public void addPiece(long tsLo, long tsHi, long rowOffset, long rowCount, long writerTxn, long lastWriteMicros) {
        assert pendingRec != NO_PARTITION : "addPiece outside beginUpdate/commitUpdate";
        assert pending.size() == 0 || tsLo > pending.getQuick(pending.size() - LONGS_PER_PIECE + PIECE_TS_LO)
                : "pieces must ascend by tsLo";
        final long cumulativeLo = pending.size() == 0
                ? 0
                : pending.getQuick(pending.size() - LONGS_PER_PIECE + PIECE_CUMULATIVE_LO)
                  + pending.getQuick(pending.size() - LONGS_PER_PIECE + PIECE_ROW_COUNT);
        pending.add(tsLo, tsHi, rowOffset, rowCount);
        pending.add(writerTxn, lastWriteMicros);
        pending.add(cumulativeLo);
    }

    /**
     * Folds every run of list-adjacent pieces in the in-flight {@link #beginUpdate}/{@link #addPiece} list that is ALSO
     * file-adjacent ({@code rowOffset == prevRowOffset + prevRowCount}, hence carrying one shift and tiling one
     * contiguous file run) into one, in a single forward pass - the same fold {@code O3PartitionJob.foldAdjacentPieces}
     * runs, kept here so callers that build a pending list (the squash publish branch) commit the folded shape every
     * other publish path already holds. The survivor keeps the earlier piece's {@code tsLo}, {@code rowOffset} and
     * cumulative row, takes the run's last non-empty {@code tsHi}, sums the row counts, and carries the freshest
     * {@code writerTxn}/{@code lastWriteMicros} pair. Zero-GC: rewrites {@link #pending} in place.
     */
    public void foldPending() {
        assert pendingRec != NO_PARTITION : "foldPending outside beginUpdate/commitUpdate";
        final int n = pending.size();
        if (n <= LONGS_PER_PIECE) {
            return;
        }
        int w = 0;
        for (int r = 0; r < n; r += LONGS_PER_PIECE) {
            final long tsLo = pending.getQuick(r + PIECE_TS_LO);
            final long tsHi = pending.getQuick(r + PIECE_TS_HI);
            final long rowOffset = pending.getQuick(r + PIECE_ROW_OFFSET);
            final long rowCount = pending.getQuick(r + PIECE_ROW_COUNT);
            final long writerTxn = pending.getQuick(r + PIECE_WRITER_TXN);
            final long lastWriteMicros = pending.getQuick(r + PIECE_LAST_WRITE_MICROS);
            final long cumulativeLo = pending.getQuick(r + PIECE_CUMULATIVE_LO);
            if (w > 0
                    && rowOffset == pending.getQuick(w - LONGS_PER_PIECE + PIECE_ROW_OFFSET)
                    + pending.getQuick(w - LONGS_PER_PIECE + PIECE_ROW_COUNT)) {
                if (rowCount > 0) {
                    // Must not keep an earlier, smaller tsHi: an empty piece carries no bound, and a tsHi
                    // cut short makes the transaction clusterer stop the survivor's range early.
                    pending.setQuick(w - LONGS_PER_PIECE + PIECE_TS_HI, tsHi);
                }
                pending.setQuick(w - LONGS_PER_PIECE + PIECE_ROW_COUNT,
                        pending.getQuick(w - LONGS_PER_PIECE + PIECE_ROW_COUNT) + rowCount);
                // A fold is as recent as its freshest input.
                pending.setQuick(w - LONGS_PER_PIECE + PIECE_WRITER_TXN,
                        Math.max(pending.getQuick(w - LONGS_PER_PIECE + PIECE_WRITER_TXN), writerTxn));
                pending.setQuick(w - LONGS_PER_PIECE + PIECE_LAST_WRITE_MICROS,
                        Math.max(pending.getQuick(w - LONGS_PER_PIECE + PIECE_LAST_WRITE_MICROS), lastWriteMicros));
                // The survivor keeps its own cumulative row; the absorbed rows change no later piece's, so
                // every kept cumulativeLo below stays valid unchanged.
                continue;
            }
            if (w != r) {
                pending.setQuick(w + PIECE_TS_LO, tsLo);
                pending.setQuick(w + PIECE_TS_HI, tsHi);
                pending.setQuick(w + PIECE_ROW_OFFSET, rowOffset);
                pending.setQuick(w + PIECE_ROW_COUNT, rowCount);
                pending.setQuick(w + PIECE_WRITER_TXN, writerTxn);
                pending.setQuick(w + PIECE_LAST_WRITE_MICROS, lastWriteMicros);
                pending.setQuick(w + PIECE_CUMULATIVE_LO, cumulativeLo);
            }
            w += LONGS_PER_PIECE;
        }
        pending.setPos(w);
    }

    /**
     * Replaces {@code partitionIndex}'s piece array with what {@link #addPiece} built and raises its {@code E}.
     */
    public void commitUpdate(int partitionIndex, long e) {
        assert pendingRec == partitionIndex : "commitUpdate for a record that beginUpdate did not open";
        int slot = findResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
        if (slot < 0) {
            slot = insertResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
            resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, 0);
            resolved.setQuick(slot + RES_GEOMETRY_REF, -1L);
            resolved.setQuick(slot + RES_WRITER_TXN, -1L);
            resolved.setQuick(slot + RES_SEQ_TXN, -1L);
            resolved.setQuick(slot + RES_GEOMETRY_VERSION, txReader.getGeometryVersion());
        } else {
            pieceHoles += (int) resolved.getQuick(slot + RES_PIECE_COUNT) * LONGS_PER_PIECE;
        }
        final int lo = pieces.size();
        pieces.add(pending);
        resolved.setQuick(slot + RES_PIECE_LO, lo);
        resolved.setQuick(slot + RES_PIECE_COUNT, pending.size() / LONGS_PER_PIECE);
        resolved.setQuick(slot + RES_E, Math.max(resolved.getQuick(slot + RES_E), e));
        if ((resolved.getQuick(slot + RES_FLAGS) & FLAG_DIRTY) == 0) {
            resolved.setQuick(slot + RES_FLAGS, resolved.getQuick(slot + RES_FLAGS) | FLAG_DIRTY);
            dirtyCount++;
        }
        pending.clear();
        pendingRec = NO_PARTITION;
    }

    public boolean isDirty(int partitionIndex) {
        final int slot = findResolved(txReader.getPartitionTimestampByIndex(partitionIndex), txReader.getPartitionNameTxn(partitionIndex));
        return slot > -1 && (resolved.getQuick(slot + RES_FLAGS) & FLAG_DIRTY) != 0;
    }

    /**
     * Appends {@code partitionIndex}'s geometry as one full-snapshot record and returns the slot-3 word {@code _txn}
     * must publish for it.
     */
    public long publish(int partitionIndex, long writerTxn, long seqTxn, long nowMicros, int commitMode) {
        final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
        final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
        final int slot = findResolved(partitionTimestamp, nameTxn);
        assert slot > -1 : "publish of an unresolved directory";
        if (geometryFile == null) {
            geometryFile = new PartitionGeometryFile(memoryTag);
        }
        final int count = (int) resolved.getQuick(slot + RES_PIECE_COUNT);
        final int lo = (int) resolved.getQuick(slot + RES_PIECE_LO);
        long liveRows = 0;
        geometryFile.beginRecord(writerTxn, seqTxn, count);
        for (int p = 0; p < count; p++) {
            final int at = lo + p * LONGS_PER_PIECE;
            geometryFile.addPiece(
                    pieces.getQuick(at + PIECE_TS_LO),
                    pieces.getQuick(at + PIECE_TS_HI),
                    pieces.getQuick(at + PIECE_ROW_OFFSET),
                    pieces.getQuick(at + PIECE_ROW_COUNT),
                    pieces.getQuick(at + PIECE_WRITER_TXN),
                    pieces.getQuick(at + PIECE_LAST_WRITE_MICROS)
            );
            liveRows += pieces.getQuick(at + PIECE_ROW_COUNT);
        }
        final long e = resolved.getQuick(slot + RES_E);
        geometryFile.setPhysicalRows(e);
        geometryFile.setLiveRows(liveRows);
        geometryFile.setLastWriteMicros(nowMicros);

        final Path path = Path.getThreadLocal(tableRoot);
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        final long committedRef = resolved.getQuick(slot + RES_GEOMETRY_REF);
        final long committedRecordSize = resolved.getQuick(slot + RES_COMMITTED_RECORD_SIZE);
        final int generation = nextPublishGeneration(path, committedRef, committedRecordSize, geometryFile.getRecordSize());
        if (generation < 0) {
            // Reachable only for a caller that did not consult hasGenerationForNextPublish, or could not act on
            // the answer. O3PartitionJob's commit path does both and assembles a fresh partition version instead,
            // so this stays as the loud last resort rather than the ordinary outcome.
            throw CairoException.critical(0)
                    .put("partition geometry generations exhausted [partitionTimestamp=").put(partitionTimestamp)
                    .put(", nameTxn=").put(nameTxn)
                    .put(", generations=").put(TxReader.PARTITION_GEOMETRY_MAX_GENERATION + 1)
                    .put(']');
        }
        // A generation the committed record does not already own opens at offset 0, whether this is a chain start
        // or a size-cap rotation; growing the committed generation appends strictly past that record.
        final long offset = committedRef != -1L && generation == TxReader.geometryGeneration(committedRef)
                ? TxReader.geometryOffset(committedRef) + committedRecordSize
                : 0;
        assert (offset & ((1L << TxReader.PARTITION_GEOMETRY_OFFSET_UNIT_SHIFT) - 1)) == 0
                : "geometry offset must be 8-byte aligned";
        final long packedOffset = offset >>> TxReader.PARTITION_GEOMETRY_OFFSET_UNIT_SHIFT;
        if ((packedOffset & ~TxReader.PARTITION_GEOMETRY_OFFSET_MASK) != 0) {
            throw CairoException.critical(0)
                    .put("partition geometry file is full [partitionTimestamp=").put(partitionTimestamp)
                    .put(", nameTxn=").put(nameTxn)
                    .put(", offset=").put(offset)
                    .put(']');
        }
        final long size = geometryFile.append(ff, path, generation, offset, commitMode);

        resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, size);
        resolved.setQuick(slot + RES_WRITER_TXN, writerTxn);
        resolved.setQuick(slot + RES_SEQ_TXN, seqTxn);
        resolved.setQuick(slot + RES_LAST_WRITE_MICROS, nowMicros);
        final long ref = TxReader.PARTITION_COMPOSITE_FLAG
                | ((long) generation << TxReader.PARTITION_GEOMETRY_GENERATION_BIT_OFFSET)
                | packedOffset;
        resolved.setQuick(slot + RES_GEOMETRY_REF, ref);
        resolved.setQuick(slot + RES_GEOMETRY_VERSION, txReader.getGeometryVersion());
        if ((resolved.getQuick(slot + RES_FLAGS) & FLAG_DIRTY) != 0) {
            resolved.setQuick(slot + RES_FLAGS, resolved.getQuick(slot + RES_FLAGS) & ~FLAG_DIRTY);
            dirtyCount--;
        }
        compactPiecesIfNeeded();
        return ref;
    }

    private void compactPieces() {
        scratch.clear();
        for (int i = 0, n = resolved.size(); i < n; i += LONGS_PER_RESOLVED) {
            final int lo = (int) resolved.getQuick(i + RES_PIECE_LO);
            final int count = (int) resolved.getQuick(i + RES_PIECE_COUNT);
            final int newLo = scratch.size();
            for (int p = 0, m = count * LONGS_PER_PIECE; p < m; p++) {
                scratch.add(pieces.getQuick(lo + p));
            }
            resolved.setQuick(i + RES_PIECE_LO, newLo);
        }
        pieces.clear();
        pieces.add(scratch);
        pieceHoles = 0;
    }

    /**
     * Amortised reclaim of {@link #pieces}.
     */
    private void compactPiecesIfNeeded() {
        if (pieceHoles > MIN_PIECE_HOLES && pieceHoles > pieces.size() - pieceHoles) {
            compactPieces();
        }
    }

    /**
     * Drops resolutions for directories {@code _txn} no longer names - a partition that was dropped, or one a rewrite
     * retired under a fresh {@code nameTxn}.
     */
    private void evictRetiredDirectories() {
        int keep = 0;
        for (int i = 0, n = resolved.size(); i < n; i += LONGS_PER_RESOLVED) {
            final long partitionTimestamp = resolved.getQuick(i + RES_PARTITION_TS);
            final int indexRaw = txReader.findAttachedPartitionRawIndexByLoTimestamp(partitionTimestamp);
            if (indexRaw > -1 && txReader.getPartitionNameTxnByRawIndex(indexRaw) == resolved.getQuick(i + RES_NAME_TXN)) {
                if (keep != i) {
                    for (int f = 0; f < LONGS_PER_RESOLVED; f++) {
                        resolved.setQuick(keep + f, resolved.getQuick(i + f));
                    }
                }
                keep += LONGS_PER_RESOLVED;
            } else {
                pieceHoles += (int) resolved.getQuick(i + RES_PIECE_COUNT) * LONGS_PER_PIECE;
            }
        }
        resolved.setPos(keep);
        resolvedEvictWatermark = Math.max(MIN_RESOLVED_BEFORE_EVICT, 2 * (keep / LONGS_PER_RESOLVED));
    }

    /**
     * Binary search of {@link #resolved} by {@code partitionTimestamp}, then a linear walk over the equal-timestamp
     * run for {@code nameTxn}. Returns the slot, or {@code -1}.
     */
    private int findResolved(long partitionTimestamp, long nameTxn) {
        final int blocks = resolved.size() / LONGS_PER_RESOLVED;
        int lo = 0;
        int hi = blocks - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long ts = resolved.getQuick(mid * LONGS_PER_RESOLVED + RES_PARTITION_TS);
            if (ts < partitionTimestamp) {
                lo = mid + 1;
            } else if (ts > partitionTimestamp) {
                hi = mid - 1;
            } else {
                int i = mid;
                while (i > 0 && resolved.getQuick((i - 1) * LONGS_PER_RESOLVED + RES_PARTITION_TS) == partitionTimestamp) {
                    i--;
                }
                for (; i < blocks && resolved.getQuick(i * LONGS_PER_RESOLVED + RES_PARTITION_TS) == partitionTimestamp; i++) {
                    if (resolved.getQuick(i * LONGS_PER_RESOLVED + RES_NAME_TXN) == nameTxn) {
                        return i * LONGS_PER_RESOLVED;
                    }
                }
                return -1;
            }
        }
        return -1;
    }

    /**
     * The lowest generation at or above {@code from} that no {@code _geometry.<generation>} file in {@code
     * partitionDir} occupies, or {@code -1} when every generation up to {@link
     * TxReader#PARTITION_GEOMETRY_MAX_GENERATION} is taken. A generation whose file still holds a record may be one a
     * pinned reader resolves records out of, so it is not a generation a new chain may open on.
     * <p>
     * A file shorter than one complete record does NOT occupy its generation. Such a file is what an {@link
     * PartitionGeometryFile#append} that failed its write leaves behind - it creates the file before writing it - and
     * nothing reclaims it afterwards: the ordinary partition purge never sees it (same directory, same {@code
     * nameTxn}), {@code VACUUM TABLE} does not know the name, and a transaction that rolled back never queues
     * {@link ColumnPurgeOperator} the retirement note it would work from. Left occupying a generation it would spend
     * one of the sixteen permanently.
     * <p>
     * The length is what makes that safe, rather than the weaker "the record at offset 0 does not verify": a reader
     * names a record by {@code (generation, offset)} out of a {@code _txn} that COMMITTED it, and
     * {@link #publish} writes a generation's first record at offset 0 and every later one strictly past it, so a
     * generation any reader can name holds at least one whole record. Reusing a generation whose offset-0 record
     * merely fails to verify would be the wider rule {@code ColumnPurgeOperator.readGeometryGenerationFirstWriterTxn}
     * deletes on - that rule deletes a generation whose offset-0 record does not verify and leaves alone, for a later
     * retry, one it could not read at all - but deleting a file a reader still names fails that reader loudly, while
     * re-opening a chain on it hands the reader a valid checksum over a record its own transaction never pointed at,
     * which is the silent corruption this method exists to prevent.
     */
    private int firstFreeGeneration(Path partitionDir, int from) {
        final int dirLen = partitionDir.size();
        try {
            for (int generation = from; generation <= TxReader.PARTITION_GEOMETRY_MAX_GENERATION; generation++) {
                // -1 when there is no file at all, which is the ordinary case.
                if (ff.length(PartitionGeometryFile.geometryFileName(partitionDir, generation)) < PartitionGeometryFile.recordSize(1)) {
                    return generation;
                }
                partitionDir.trimTo(dirLen);
            }
            return -1;
        } finally {
            partitionDir.trimTo(dirLen);
        }
    }

    /**
     * The generation a {@link #publish} of a {@code recordSize}-byte record lands on, given the directory's committed
     * geometry ref and the size of the record that ref names, or {@code -1} when no generation is free. The single
     * decision {@code publish} and {@link #hasGenerationForNextPublish} both go through.
     * <p>
     * A chain STARTING in this directory is not the same thing as generation 0 being free. MAKE-PLAIN, and a JOIN
     * that folds a partition back to the ordinary shape, clear the geometry ref while the directory stays put; a
     * reader pinned before that commit still names the record it was resolving by {@code (generation, offset)} alone,
     * and resolves it lazily, long afterwards. Read validation checks structure and checksum, never identity, so
     * opening a fresh chain on a generation still on disk would write over that record and hand the reader a shape
     * its own transaction never pointed at - the same rows resolved against different pieces. {@link
     * ColumnPurgeOperator} removes a retired generation only once no reader can still resolve it, so a generation
     * holding no record is a generation no live transaction can reach.
     * <p>
     * Every record is a full snapshot (see the class doc), so rotating past {@link PartitionGeometryFile#MAX_FILE_SIZE}
     * costs nothing beyond starting a fresh file: the new record, not a copy of what came before, is what the new
     * generation opens with. The rotation skips occupied generations for the same reason a fresh chain does, and it
     * only ever moves UP - a lower generation is free because the purge released it, and a chain that walked back
     * down into it would leave the reader window {@code ColumnPurgeOperator} derives from a generation's offset-0
     * record no longer monotonic in the generation number.
     */
    private int nextPublishGeneration(Path partitionDir, long committedRef, long committedRecordSize, long recordSize) {
        if (committedRef == -1L) {
            return firstFreeGeneration(partitionDir, 0);
        }
        final int generation = TxReader.geometryGeneration(committedRef);
        if (TxReader.geometryOffset(committedRef) + committedRecordSize + recordSize > PartitionGeometryFile.MAX_FILE_SIZE) {
            return firstFreeGeneration(partitionDir, generation + 1);
        }
        return generation;
    }

    private int insertResolved(long partitionTimestamp, long nameTxn) {
        // Binary search for the insertion point, matching findResolved's own binary search over the SAME
        // (RES_PARTITION_TS, RES_NAME_TXN) ordering rather than a linear scan from index 0. The compaction
        // sweep resolves composite partitions on an ascending partition walk, so a linear scan would always
        // traverse every slot already present before appending at the tail - O(C) per insert, O(C^2) to
        // resolve C composite partitions, paid fresh every sweep because of()->discard() drops the cache.
        // insertResolved runs only for a key findResolved did not find, so the search lands on the first slot
        // ordered strictly after the key - exactly where the linear scan broke - keeping resolved ascending,
        // the invariant findResolved depends on.
        final int blocks = resolved.size() / LONGS_PER_RESOLVED;
        int lo = 0;
        int hi = blocks;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            final long ts = resolved.getQuick(mid * LONGS_PER_RESOLVED + RES_PARTITION_TS);
            if (ts < partitionTimestamp
                    || (ts == partitionTimestamp && resolved.getQuick(mid * LONGS_PER_RESOLVED + RES_NAME_TXN) <= nameTxn)) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        final int at = lo * LONGS_PER_RESOLVED;
        resolved.insert(at, LONGS_PER_RESOLVED);
        for (int s = 0; s < LONGS_PER_RESOLVED; s++) {
            resolved.setQuick(at + s, 0);
        }
        // The key the slot is found by, stamped here rather than by each caller: a slot carrying a zeroed
        // key is a slot findResolved cannot return, and the caller that inserted it would be the only one
        // ever able to reach it.
        resolved.setQuick(at + RES_PARTITION_TS, partitionTimestamp);
        resolved.setQuick(at + RES_NAME_TXN, nameTxn);
        return at;
    }

    private long pieceLong(int res, int ordinal, int field) {
        assert ordinal > -1 && ordinal < resolved.getQuick(res + RES_PIECE_COUNT);
        final int lo = (int) resolved.getQuick(res + RES_PIECE_LO);
        return pieces.getQuick(lo + ordinal * LONGS_PER_PIECE + field);
    }

    private int readInto(int slot, long partitionTimestamp, long nameTxn, long ref) {
        if (geometryFile == null) {
            geometryFile = new PartitionGeometryFile(memoryTag);
        }
        final Path path = Path.getThreadLocal(tableRoot);
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        geometryFile.read(ff, path, TxReader.geometryGeneration(ref), TxReader.geometryOffset(ref));
        final int count = geometryFile.getPieceCount();
        final int lo = pieces.size();
        pieces.setPos(lo + count * LONGS_PER_PIECE);
        long cumulativeLo = 0;
        for (int p = 0; p < count; p++) {
            final int at = lo + p * LONGS_PER_PIECE;
            pieces.setQuick(at + PIECE_TS_LO, geometryFile.getPieceTimestampLo(p));
            pieces.setQuick(at + PIECE_TS_HI, geometryFile.getPieceTimestampHi(p));
            pieces.setQuick(at + PIECE_ROW_OFFSET, geometryFile.getPieceRowOffset(p));
            pieces.setQuick(at + PIECE_ROW_COUNT, geometryFile.getPieceRowCount(p));
            pieces.setQuick(at + PIECE_WRITER_TXN, geometryFile.getPieceWriterTxn(p));
            pieces.setQuick(at + PIECE_LAST_WRITE_MICROS, geometryFile.getPieceLastWriteMicros(p));
            pieces.setQuick(at + PIECE_CUMULATIVE_LO, cumulativeLo);
            cumulativeLo += geometryFile.getPieceRowCount(p);
        }
        if (slot < 0) {
            slot = insertResolved(partitionTimestamp, nameTxn);
        }
        resolved.setQuick(slot + RES_PIECE_LO, lo);
        resolved.setQuick(slot + RES_PIECE_COUNT, count);
        resolved.setQuick(slot + RES_E, geometryFile.getPhysicalRows());
        resolved.setQuick(slot + RES_LAST_WRITE_MICROS, geometryFile.getLastWriteMicros());
        resolved.setQuick(slot + RES_WRITER_TXN, geometryFile.getWriterTxn());
        resolved.setQuick(slot + RES_SEQ_TXN, geometryFile.getSeqTxn());
        resolved.setQuick(slot + RES_COMMITTED_RECORD_SIZE, PartitionGeometryFile.recordSize(count));
        resolved.setQuick(slot + RES_GEOMETRY_REF, ref);
        resolved.setQuick(slot + RES_FLAGS, 0);
        resolved.setQuick(slot + RES_GEOMETRY_VERSION, txReader.getGeometryVersion());
        return slot;
    }

    /**
     * Finds the resolved slot for {@code partitionIndex}, reading {@code _geometry} when it is not resident at the
     * record's current {@code geometryRef}.
     */
    private int resolveInternal(int partitionIndex) {
        if (!txReader.isPartitionComposite(partitionIndex)) {
            return -1;
        }
        // Reclaim BEFORE any slot or piece index is taken below - both caches renumber themselves.
        if (pendingRec == NO_PARTITION && dirtyCount == 0 && resolved.size() / LONGS_PER_RESOLVED > resolvedEvictWatermark) {
            evictRetiredDirectories();
        }
        compactPiecesIfNeeded();
        final long partitionTimestamp = txReader.getPartitionTimestampByIndex(partitionIndex);
        final long nameTxn = txReader.getPartitionNameTxn(partitionIndex);
        final long ref = txReader.getGeometryRef(partitionIndex);
        final int slot = findResolved(partitionTimestamp, nameTxn);
        if (slot > -1) {
            if (resolved.getQuick(slot + RES_GEOMETRY_REF) == ref) {
                if (resolved.getQuick(slot + RES_GEOMETRY_VERSION) == txReader.getGeometryVersion()) {
                    return slot;
                }
                if (resolved.getQuick(slot + RES_WRITER_TXN) == readWriterTxn(partitionTimestamp, nameTxn, ref)) {
                    resolved.setQuick(slot + RES_GEOMETRY_VERSION, txReader.getGeometryVersion());
                    return slot;
                }
            }
            // Resident at a superseded or potentially reused geometry: re-read in place, and the old
            // piece span becomes a hole. The geometry version changes when a geometry file chain is
            // created or rotated; the header writer txn check catches a composite/plain/composite cycle
            // that reuses both the directory and the geometry reference without re-reading pieces for
            // unaffected partitions.
            pieceHoles += (int) resolved.getQuick(slot + RES_PIECE_COUNT) * LONGS_PER_PIECE;
        }
        return readInto(slot, partitionTimestamp, nameTxn, ref);
    }

    private long readWriterTxn(long partitionTimestamp, long nameTxn, long ref) {
        if (geometryFile == null) {
            geometryFile = new PartitionGeometryFile(memoryTag);
        }
        final Path path = Path.getThreadLocal(tableRoot);
        TableUtils.setPathForNativePartition(path, timestampType, partitionBy, partitionTimestamp, nameTxn);
        return geometryFile.readWriterTxn(ff, path, TxReader.geometryGeneration(ref), TxReader.geometryOffset(ref));
    }
}
