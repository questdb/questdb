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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.RecordToRowCopier;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.Chars;
import io.questdb.std.IntHashSet;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One closed anchor segment's keyed repair: the keys its correction touched, and the
 * merge that supplies every other key's row from the view's own stored output.
 * <p>
 * A whole-segment repair reads every base row of the segment, evaluates the window over
 * all of them and re-emits the segment's whole output. Only the keys the correction
 * carried have changed output, so a keyed repair reads only those keys' rows - through
 * the base's posting index, which
 * {@code PageFrameRecordCursorFactory.getCursorInTimestampRangeForwardIndexed} follows -
 * and evaluates the window over those alone. {@link LiveViewCheckpointKeyedScanCost}
 * prices that read against the whole-segment one and decides which route a segment takes.
 *
 * <h2>Why the merge exists</h2>
 * The read localizes; the <b>publication</b> does not. A repair publishes with
 * {@code WalWriter.commitLiveViewWithReplaceRange}, which deletes {@code [lowTs, hiTs)}
 * wholesale, so a commit carrying only the affected keys' rows would delete every
 * unaffected key's stored row inside the segment. The block therefore has to carry the
 * segment's full row set - unless the view's own table carries the dedup keys a sparse
 * publication upserts on, which is what the section below is about - and this supplies
 * the half the keyed replay did not compute: it reads the view's own durable rows over
 * that range, drops the ones whose key the replay is recomputing, and appends the rest
 * into the same commit in timestamp order.
 * <p>
 * So a keyed repair publishing a replacement writes exactly what a whole-segment repair
 * writes. What it saves is the window evaluation and the base column reads for the keys
 * the correction did not touch; what it costs is one sequential read of the view's own
 * output for the segment.
 *
 * <h2>What it changes about a repair</h2>
 * A copied-forward row is no longer recomputed from the base. A whole-segment repair is a
 * from-base recompute of its range and corrects any divergence it finds there; a keyed
 * one preserves the stored row for every key outside {@code Q}. That is a property change
 * rather than an optimization, which is why
 * {@code cairo.live.view.checkpoint.repair.keyed.replay.enabled} exists at all. It defaults
 * to true, so an operator who wants a from-base recompute of the whole range sets it to
 * false; the route reaches only views whose key column is an indexed SYMBOL, so an
 * unindexed view keeps exactly what it had either way.
 *
 * <h2>Ordering</h2>
 * The merged rows are appended in timestamp order against the replay's own, because the
 * repair's cadence boundaries carry cumulative live-view row positions and a boundary's
 * position is the count of emitted rows at or below it. The replay drains this up to each
 * boundary before freezing it, and up to each replayed row before appending that row, so
 * at every freeze the emitted set is exactly the rows at or below the boundary. See
 * {@link #drainUpTo}.
 *
 * <h2>The sparse attempt, and what the merge does instead of writing</h2>
 * A view whose own table carries the {@code (designated timestamp, projected partition
 * key)} dedup keys can publish the repair as an <b>upsert</b> on that pair rather than as
 * a replacement of the interval, and then the block carries only the rows the replay
 * recomputed - every other key's stored row stays where it stands rather than being
 * rewritten as itself. {@link #bindOutput} takes that decision, and what it changes here
 * is one thing: the merge still walks every stored row and still <b>accounts</b> for it,
 * it just does not write it. The accounting is what the boundary positions are made of -
 * a cadence boundary records the count of live-view rows at or below it, and a row this
 * merge leaves alone is still a row below that boundary - so a sparse publication's
 * ladder is the merged publication's ladder, to the row.
 * <p>
 * That is also why the fallback is not a rollback. A repair whose output turns out to
 * repeat a pair cannot be published sparsely at all - the upsert would collapse the
 * repeat - and the whole-segment replacement it falls back to needs the rows this merge
 * accounted for and skipped. {@link #materializeMerge} is what supplies them: it rewinds
 * the stored cursor and writes the same row set it counted, which it then proves it
 * re-read to the row. The rows come out after the replay's own rather than interleaved
 * with them, which the WAL carries as any other out-of-order block.
 * <p>
 * An open-segment resume walks nothing to begin with: its boundary positions come from
 * the durable ones plus the exact count of inserted rows, so it accounts for no stored
 * row and does not advance this cursor at all. Its fallback is
 * {@link #materializeUnaccountedMerge}, which writes the whole stored merge in the one
 * pass that would otherwise only have counted it.
 *
 * <h2>Why the two key spaces</h2>
 * The base table and the view keep separate symbol maps over the same strings, so
 * neither's integers name a key in the other, and the checkpoint roots name keys in a
 * third encoding again. {@link #arm} resolves {@code Q} in all three, and refuses the
 * route rather than dropping a key it cannot resolve against the base: a key missing from
 * a keyed scan is a key whose rows the repair would not correct.
 */
public final class LiveViewCheckpointKeyedReplay implements BoundaryFreezingCursor.RowDrain, QuietCloseable {
    // The reader-local base symbol keys the indexed scan follows, in the order it takes
    // them. Never holds a duplicate: two cursors over one key would each yield its rows.
    private final IntList baseSymbolKeys = new IntList();
    private final MemoryCARW keyBuffer;
    // Q's logical values, kept because the view's symbol map is only reachable once the
    // merge's own cursor is open - the two tables keep separate maps over the same strings.
    private final CharSequenceHashSet logicalKeys = new CharSequenceHashSet();
    // Q in the encoding a checkpoint partition map keys an entry by, which is what lets
    // the boundary roots this repair re-versions keep every key outside it exactly as the
    // old root wrote it.
    private final LiveViewCheckpointOutputKeyDomain outputKeys = new LiveViewCheckpointOutputKeyDomain();
    // Q resolved against the VIEW's own symbol map, which is what its stored rows carry.
    private final IntHashSet storedSymbolKeys = new IntHashSet();
    private boolean armed;
    private int baseKeyColumnIndex = -1;
    private RecordToRowCopier copier;
    private SqlExecutionContext executionContext;
    private boolean hasNullKey;
    private boolean hasPendingRow;
    private LiveViewInstance instance;
    private long mergedMaxTs = Numbers.LONG_NULL;
    private long mergedMinTs = Numbers.LONG_NULL;
    private long mergedRows;
    private long pendingRowTs = Numbers.LONG_NULL;
    // Whether this repair is attempting a sparse publication, which is what decides
    // whether the merge writes the rows it accounts for. Retracted by materializeMerge,
    // which is the abandoning half of the fallback.
    private boolean sparse;
    // Stored rows in the range whose key the replay recomputes. A replacement deletes
    // them outright; a sparse upsert replaces each with the block row carrying its pair,
    // which is why they are the rows the publication's row arithmetic turns on.
    private long supersededRows;
    private int storedKeyColumnIndex = -1;
    private Record storedRecord;
    private RecordCursor storedRowCursor;
    private int storedTimestampIndex = -1;
    private WalWriter walWriter;

    public LiveViewCheckpointKeyedReplay() {
        this.keyBuffer = Vm.getCARWInstance(1024, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    }

    /**
     * Arms this repair with one segment's key domain.
     * <p>
     * The caller has already proved the view admits a keyed replay at all - one indexed
     * SYMBOL partition column, projected into the view's own schema - and that the cost
     * model prefers it for this segment. What is left is the domain itself, which has to
     * resolve in three spaces at once: the base reader's, so the posting index can be
     * sought; the checkpoint encoding, so a frozen root can name it; and the view's own,
     * which {@link #bindStoredRows} does once the merge's cursor - and with it the view's
     * symbol map - is open.
     *
     * @param baseKeyColumnIndex the key column's index in the base scan's metadata
     * @param baseSymbols        the base reader's symbol map for that column
     * @param checkpointKeyTypes the key shape a checkpoint partition map keys by, which a
     *                           keyed replay requires to be the single STRING one SYMBOL
     *                           partition column encodes to
     * @param keys               the segment's logical key values
     * @param hasNullKey         whether the correction also touched the null key, which
     *                           both indexes name under a key of its own
     * @return false when the domain could not be resolved, leaving this disarmed and the
     * segment reading whole
     */
    public boolean arm(
            int baseKeyColumnIndex,
            @NotNull StaticSymbolTable baseSymbols,
            @NotNull ColumnTypes checkpointKeyTypes,
            @NotNull CharSequenceHashSet keys,
            boolean hasNullKey
    ) {
        clear();
        if (checkpointKeyTypes.getColumnCount() != 1
                || ColumnType.tagOf(checkpointKeyTypes.getColumnType(0)) != ColumnType.STRING) {
            // A single SYMBOL partition column encodes to a single STRING checkpoint key,
            // and the gate that admitted this repair proved there is exactly one such
            // column. Anything else is a projector this route cannot name a key through.
            return false;
        }
        if (keys.size() == 0 && !hasNullKey) {
            // A segment whose corrections carried no key at all. The decomposition does
            // not produce one, and a keyed scan over an empty key set reads nothing.
            return false;
        }
        this.baseKeyColumnIndex = baseKeyColumnIndex;
        this.hasNullKey = hasNullKey;
        if (hasNullKey) {
            // A partition key like any other: the base index names the null value's rows
            // under its own key, and the view stores them under its own null key.
            baseSymbolKeys.add(SymbolTable.VALUE_IS_NULL);
            addOutputKey(null);
        }
        for (int i = 0, n = keys.size(); i < n; i++) {
            final CharSequence key = keys.get(i);
            if (key == null) {
                // The change set holds the null key beside its set rather than in it, and
                // hasNullKey above is what carries it. A second VALUE_IS_NULL here would
                // put two cursors over one key into the scan and yield its rows twice.
                continue;
            }
            final int baseKey = baseSymbols.keyOf(key);
            if (baseKey == SymbolTable.VALUE_NOT_FOUND) {
                // Impossible for a reader pinned at or above the commit that introduced
                // the value, and refused rather than dropped: a key silently missing from
                // a keyed scan is a key whose rows it would not repair.
                clear();
                return false;
            }
            baseSymbolKeys.add(baseKey);
            addOutputKey(key);
            // Copied rather than referenced: the change set these come from is refilled by
            // the next repair this worker classifies, and the merge resolves them against
            // the view's map after the replay has begun.
            logicalKeys.add(Chars.toString(key));
        }
        armed = true;
        return true;
    }

    /**
     * Binds the writer this merge appends into, and the copier that puts one stored row
     * back into the view's own table.
     * <p>
     * Separate from {@link #bindStoredRows} because the two become available at different
     * points of a repair: the stored rows have to be resolved before the repair opens its
     * timeline capture, which is what carries {@code Q}, while the writer is opened after.
     *
     * @param copier           copies one stored row into a row of the same table
     * @param walWriter        the writer this repair is staging its replacement in
     * @param executionContext the copier's context, which only a DECIMAL column reads
     * @param instance         the view being repaired, whose batch-minimum window every
     *                         emitted row moves - a merged row is one a whole-segment
     *                         replay would have emitted itself, so it has to move it too,
     *                         sparse publication or not: the seal that follows must not be
     *                         able to tell the two publications apart
     * @param sparse           whether this repair is attempting a sparse publication, in
     *                         which case the merge accounts for every stored row it walks
     *                         and writes none of them
     */
    public void bindOutput(
            @NotNull RecordToRowCopier copier,
            @NotNull WalWriter walWriter,
            @NotNull SqlExecutionContext executionContext,
            @NotNull LiveViewInstance instance,
            boolean sparse
    ) {
        this.copier = copier;
        this.walWriter = walWriter;
        this.executionContext = executionContext;
        this.instance = instance;
        this.sparse = sparse;
    }

    /**
     * Binds the stored rows this repair copies forward.
     * <p>
     * The cursor is the caller's: it comes off the view's own table at the pre-repair
     * transaction - the only moment those rows exist, since the replacement about to be
     * committed deletes them - and the caller closes it along with the reader behind it.
     *
     * @param storedRowCursor      the view's own rows over the replacement's range, ascending
     * @param storedTimestampIndex the designated timestamp's index in that cursor's metadata
     * @param storedKeyColumnIndex the key column's index in that cursor's metadata
     * @return false when the view's key column is not one this merge can compare, which
     * leaves the caller to read the segment whole
     */
    public boolean bindStoredRows(
            @NotNull RecordCursor storedRowCursor,
            int storedTimestampIndex,
            int storedKeyColumnIndex
    ) {
        if (!armed) {
            throw CairoException.critical(0).put("live view keyed replay merge opened without a key domain");
        }
        if (!(storedRowCursor.getSymbolTable(storedKeyColumnIndex) instanceof StaticSymbolTable storedSymbols)) {
            // The view stores its copy of the key as something other than a SYMBOL off its
            // own map, so a stored row's integer names nothing this can compare Q against.
            return false;
        }
        storedSymbolKeys.clear();
        if (hasNullKey) {
            storedSymbolKeys.add(SymbolTable.VALUE_IS_NULL);
        }
        for (int i = 0, n = logicalKeys.size(); i < n; i++) {
            final int storedKey = storedSymbols.keyOf(logicalKeys.get(i));
            if (storedKey != SymbolTable.VALUE_NOT_FOUND) {
                // A key the view has never stored has no row for the merge to drop. That
                // is what a correction introducing a key looks like, and it is not an
                // error - the replay emits its rows and the merge keeps none.
                storedSymbolKeys.add(storedKey);
            }
        }
        this.storedRowCursor = storedRowCursor;
        this.storedRecord = storedRowCursor.getRecord();
        this.storedTimestampIndex = storedTimestampIndex;
        this.storedKeyColumnIndex = storedKeyColumnIndex;
        this.hasPendingRow = false;
        this.pendingRowTs = Numbers.LONG_NULL;
        this.mergedRows = 0;
        this.mergedMinTs = Numbers.LONG_NULL;
        this.mergedMaxTs = Numbers.LONG_NULL;
        this.supersededRows = 0;
        return true;
    }

    public void clear() {
        releaseMergeState();
        armed = false;
        hasNullKey = false;
        baseKeyColumnIndex = -1;
        baseSymbolKeys.clear();
        storedSymbolKeys.clear();
        logicalKeys.clear();
        outputKeys.clear();
        mergedRows = 0;
        mergedMinTs = Numbers.LONG_NULL;
        mergedMaxTs = Numbers.LONG_NULL;
        supersededRows = 0;
        sparse = false;
    }

    @Override
    public void close() {
        clear();
        Misc.free(keyBuffer);
    }

    /**
     * Accounts for every stored row this merge still owes, whatever its timestamp. The
     * replay calls it once its own scan is exhausted and before it commits: the rows above
     * the last replayed one are still inside the range the replacement deletes.
     *
     * @return how many rows it accounted for
     */
    public long drainRemaining() {
        return drainUpTo(Long.MAX_VALUE);
    }

    /**
     * Accounts for every stored row this merge still owes at or below
     * {@code tsInclusive}, and writes it too unless this repair is attempting a sparse
     * publication.
     * <p>
     * Called from two places, and the pair is what keeps the accounted set exactly the
     * rows at or below whatever it is measured against: from the boundary freeze, with the
     * boundary's own timestamp, so the cumulative row position that boundary records
     * counts them; and from the replay's row loop, with the timestamp of the row about to
     * be appended, so the block's rows come out in timestamp order.
     *
     * @return how many rows it accounted for
     */
    @Override
    public long drainUpTo(long tsInclusive) {
        if (storedRowCursor == null || walWriter == null) {
            return 0;
        }
        long appended = 0;
        while (hasPendingRow || advance()) {
            if (pendingRowTs > tsInclusive) {
                break;
            }
            append();
            appended++;
        }
        return appended;
    }

    public int getBaseKeyColumnIndex() {
        return baseKeyColumnIndex;
    }

    public @NotNull IntList getBaseSymbolKeys() {
        return baseSymbolKeys;
    }

    /**
     * @return the highest timestamp this merge accounted for, or
     * {@link Numbers#LONG_NULL} when it accounted for nothing
     */
    public long getMergedMaxTs() {
        return mergedMaxTs;
    }

    /**
     * @return the lowest timestamp this merge accounted for, or
     * {@link Numbers#LONG_NULL} when it accounted for nothing
     */
    public long getMergedMinTs() {
        return mergedMinTs;
    }

    /**
     * @return stored rows this merge accounted for - written into the block by a repair
     * publishing a replacement, and left exactly where they stand by one publishing
     * sparsely. Either way they are rows of the repaired range, which is what the
     * boundary positions and the publication's row arithmetic count them as.
     */
    public long getMergedRows() {
        return mergedRows;
    }

    public @NotNull LiveViewCheckpointOutputKeyDomain getOutputKeys() {
        return outputKeys;
    }

    /**
     * @return stored rows in the repaired range whose key the replay recomputes. A
     * replacement deletes them; a sparse upsert replaces each with the block row carrying
     * its pair.
     */
    public long getSupersededRows() {
        return supersededRows;
    }

    public boolean isArmed() {
        return armed;
    }

    public boolean isMerging() {
        return storedRowCursor != null;
    }

    /**
     * @return whether this repair is still attempting a sparse publication, which is what
     * makes {@link #getMergedRows()} a count of rows nothing wrote
     */
    public boolean isSparse() {
        return sparse;
    }

    /**
     * Abandons a sparse publication and writes the rows the merge had only accounted for,
     * so the repair can publish its whole range with a replacement instead.
     * <p>
     * Not a rollback: a sparse attempt reads the view's stored rows to count them and
     * writes none, so what a replacement needs is precisely what this repair has already
     * walked past. The cursor is rewound and the same row set written out - proved to be
     * the same by counting it again, because a re-read that produced a different set would
     * put a block into the WAL that the frozen boundary positions do not describe, and no
     * reader detects that.
     * <p>
     * The rows come out above the replay's own rather than interleaved with them. That is
     * a WAL block whose rows are not in timestamp order, which the apply sorts like any
     * other out-of-order commit; the boundary positions are unaffected, because they count
     * the rows the merge accounted for and it accounted for exactly these.
     *
     * @return false when there was no sparse attempt to abandon
     */
    public boolean materializeMerge() {
        if (!sparse) {
            return false;
        }
        if (storedRowCursor == null || walWriter == null) {
            throw CairoException.critical(0)
                    .put("live view sparse publication abandoned without the merge it has to fall back on");
        }
        final long accountedRows = mergedRows;
        final long accountedSupersededRows = supersededRows;
        storedRowCursor.toTop();
        sparse = false;
        hasPendingRow = false;
        pendingRowTs = Numbers.LONG_NULL;
        mergedRows = 0;
        mergedMinTs = Numbers.LONG_NULL;
        mergedMaxTs = Numbers.LONG_NULL;
        supersededRows = 0;
        drainRemaining();
        if (mergedRows != accountedRows || supersededRows != accountedSupersededRows) {
            throw CairoException.critical(0)
                    .put("live view sparse publication fallback re-read a different row set [accountedRows=")
                    .put(accountedRows).put(", mergedRows=").put(mergedRows)
                    .put(", accountedSupersededRows=").put(accountedSupersededRows)
                    .put(", supersededRows=").put(supersededRows).put(']');
        }
        return true;
    }

    /**
     * Abandons a sparse publication that has accounted for nothing, writing the whole
     * stored merge in one pass.
     * <p>
     * The counterpart of {@link #materializeMerge} for a repair whose row positions never
     * came from this merge at all: an open-segment resume derives them from the durable
     * positions plus the exact count of inserted rows, so it leaves the stored cursor
     * where {@link #bindStoredRows} put it. There is nothing to rewind and nothing to
     * re-prove - the first walk is the one that writes.
     *
     * @return false when there was no sparse attempt to abandon
     * @throws CairoException when the merge has already walked part of the stored
     *                        interval, which would make this write a row set the
     *                        accounted one no longer describes
     */
    public boolean materializeUnaccountedMerge() {
        if (!sparse) {
            return false;
        }
        if (storedRowCursor == null || walWriter == null) {
            throw CairoException.critical(0)
                    .put("live view sparse publication abandoned without the merge it has to fall back on");
        }
        if (mergedRows != 0 || supersededRows != 0 || hasPendingRow) {
            throw CairoException.critical(0)
                    .put("live view sparse publication fallback cannot write a merge it has already walked")
                    .put(" [mergedRows=").put(mergedRows)
                    .put(", supersededRows=").put(supersededRows).put(']');
        }
        sparse = false;
        drainRemaining();
        return true;
    }

    /**
     * Drops the merge's hold on the caller's cursor and writer, keeping the counts it
     * ended on so the replay can report and check them.
     */
    public void releaseMergeState() {
        storedRowCursor = null;
        storedRecord = null;
        storedTimestampIndex = -1;
        storedKeyColumnIndex = -1;
        copier = null;
        walWriter = null;
        executionContext = null;
        instance = null;
        hasPendingRow = false;
        pendingRowTs = Numbers.LONG_NULL;
    }

    private void addOutputKey(@Nullable CharSequence value) {
        keyBuffer.jumpTo(0);
        // The one encoding both sides have to be comparable in: a live-view partition-by
        // RecordSink rewrites a SYMBOL partition column as its resolved STRING, so this is
        // the byte image LiveViewSnapshotKeyCodec writes off a window function's own map
        // record for the same key.
        keyBuffer.putStr(value);
        final long length = keyBuffer.getAppendOffset();
        if (length > Integer.MAX_VALUE) {
            throw CairoException.critical(0)
                    .put("live view keyed replay partition key is too long, length=").put(length);
        }
        final byte[] key = new byte[(int) length];
        for (int i = 0; i < key.length; i++) {
            key[i] = keyBuffer.getByte(i);
        }
        outputKeys.add(key);
    }

    /**
     * Positions the cursor on the next stored row the replay does not recompute.
     *
     * @return false once the range holds no such row
     */
    private boolean advance() {
        while (storedRowCursor.hasNext()) {
            if (storedSymbolKeys.contains(storedRecord.getInt(storedKeyColumnIndex))) {
                // A key the replay recomputes. Its stored row is deleted by the
                // replacement and replaced by the one the replay emits - or, under a
                // sparse publication, upserted over by exactly the block row carrying its
                // pair, which is what makes the count the publication's row arithmetic.
                supersededRows++;
                continue;
            }
            pendingRowTs = storedRecord.getTimestamp(storedTimestampIndex);
            hasPendingRow = true;
            return true;
        }
        hasPendingRow = false;
        return false;
    }

    private void append() {
        // The same stamp the replay's own row loop makes: a merged row is one a
        // whole-segment replay would have emitted here, and the batch-minimum window the
        // next seal measures must not be able to tell the two routes apart. A sparse
        // publication leaves the row where it stands rather than rewriting it, which is a
        // difference in what the block carries and not in what the segment then holds.
        instance.setLatestSeenTs(pendingRowTs);
        if (!sparse) {
            final TableWriter.Row row = walWriter.newRow(pendingRowTs);
            copier.copy(executionContext, storedRecord, row);
            row.append();
        }
        if (mergedMinTs == Numbers.LONG_NULL) {
            mergedMinTs = pendingRowTs;
        }
        mergedMaxTs = pendingRowTs;
        mergedRows++;
        hasPendingRow = false;
    }
}
