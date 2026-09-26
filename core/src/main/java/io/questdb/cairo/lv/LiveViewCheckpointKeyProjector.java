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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The partition identity every window function in one live view shares, compiled once
 * against the window's input metadata and read by everything that has to name a key.
 * <p>
 * A live view whose functions disagree on their PARTITION BY has no such identity and
 * gets no projector; {@link LiveViewSegmentRepairEnvelope#GATE_MIXED_PARTITION_KEYS} is the
 * static form of the same question. Where one identity does exist it is the only key
 * schema the view has - the anchor window, the checkpoint roots and every repair speak
 * it - so deriving it twice would be two chances to disagree.
 * <p>
 * Two consumers today, and they need different halves of it:
 * <ul>
 *     <li>{@link LiveViewCheckpointRowsPlan} projects a base record into a map key, to
 *     count rows per key while discovering a ROWS dependency's bounds. That is what the
 *     two sinks are for, and the plan held them itself until an anchor-only view - which
 *     compiles no ROWS plan at all - needed the same identity.</li>
 *     <li>A keyed repair names the keys a correction touched, so its replay can follow
 *     those keys' rows through the base's posting index rather than reading every row of
 *     the segment. That needs one thing the sinks do not carry: <b>which base column the
 *     index is on</b>, which is {@link #getIndexedSymbolColumnIndex()}.</li>
 * </ul>
 *
 * <h2>The two sinks</h2>
 * {@link #getKeySink()} writes a SYMBOL key column as the reader's table-local integer,
 * which is stable for one reader's lifetime - exactly the scope one repair plans and
 * replays in. {@link #getCheckpointKeySink()} writes it as the resolved string instead,
 * because that is what a window function's own partition map holds and what a checkpoint
 * partition map keys an entry by. On a projector with no SYMBOL key column the two are
 * one object.
 *
 * <h2>What it deliberately does not do</h2>
 * It encodes no checkpoint key bytes. {@link LiveViewSnapshotKeyCodec} does that, off a
 * map record and into a caller-owned buffer, and a projector shared by every worker
 * draining the view may own no such buffer. A keyed <i>publication</i> - which is the
 * first thing that needs the bytes rather than the identity - is Stage 3's, and it will
 * encode through the codec exactly as {@link LiveViewCheckpointRowsBounds} already does.
 *
 * <h2>Ownership</h2>
 * An expression-keyed projector owns its key functions, which the compiling factory frees
 * with the projector. Consumers hold non-owning references and must not close it.
 */
public final class LiveViewCheckpointKeyProjector implements QuietCloseable {
    private final ColumnTypes checkpointKeyColumnTypes;
    private final RecordSink checkpointKeySink;
    private final int indexedSymbolColumnIndex;
    private final ColumnTypes keyColumnTypes;
    private final ObjList<Function> keyFunctions;
    private final RecordSink keySink;
    private final IntList partitionByColumnIndexes = new IntList();
    private final String partitionSignature;

    /**
     * @param partitionByColumnIndexes the PARTITION BY terms' column indexes in the
     *                                 window's input metadata, or null when one term is
     *                                 an expression and every term is therefore projected
     *                                 through a key function instead
     * @param keyFunctions             the expression-keyed projector's own functions,
     *                                 owned by this projector, or null on the column path
     * @param inputMetadata            the window's input metadata, which the indexed
     *                                 SYMBOL column below is read off
     */
    public LiveViewCheckpointKeyProjector(
            @Nullable IntList partitionByColumnIndexes,
            @Nullable ObjList<Function> keyFunctions,
            @NotNull ColumnTypes keyColumnTypes,
            @NotNull RecordSink keySink,
            @NotNull ColumnTypes checkpointKeyColumnTypes,
            @NotNull RecordSink checkpointKeySink,
            @NotNull CharSequence partitionSignature,
            @NotNull RecordMetadata inputMetadata
    ) {
        if (keyColumnTypes.getColumnCount() < 1) {
            // A window with no PARTITION BY has no identity to share. Its functions keep
            // no per-key state, so nothing downstream would have a key to ask about.
            throw new IllegalArgumentException("invalid live view checkpoint key projector");
        }
        if (partitionByColumnIndexes != null) {
            this.partitionByColumnIndexes.addAll(partitionByColumnIndexes);
        }
        this.keyFunctions = keyFunctions;
        this.partitionSignature = partitionSignature.toString();
        this.keyColumnTypes = keyColumnTypes;
        this.keySink = keySink;
        this.checkpointKeyColumnTypes = checkpointKeyColumnTypes;
        this.checkpointKeySink = checkpointKeySink;
        this.indexedSymbolColumnIndex = resolveIndexedSymbolColumn(this.partitionByColumnIndexes, inputMetadata);
    }

    /**
     * Frees the key functions an expression-keyed projector owns. Called by the factory
     * that compiled it, and by nothing else.
     */
    @Override
    public void close() {
        Misc.freeObjList(keyFunctions);
    }

    /**
     * Returns the map key shape the {@link #getCheckpointKeySink() checkpoint projector}
     * writes: this projector's key types with every SYMBOL rewritten to STRING.
     */
    public @NotNull ColumnTypes getCheckpointKeyColumnTypes() {
        return checkpointKeyColumnTypes;
    }

    /**
     * Returns the projector that writes one base record's partition key the way the view's
     * window functions key their own maps, so a key a repair collects and a key a
     * checkpoint partition map holds encode to the same bytes.
     * <p>
     * {@link #initKeyFunctions} must have bound the key functions to the cursor the
     * records come from.
     */
    public @NotNull RecordSink getCheckpointKeySink() {
        return checkpointKeySink;
    }

    /**
     * Returns the window-input column index of the single indexed SYMBOL column this
     * identity partitions on, or -1 when it is anything else - a compound key, an
     * expression key, a key of another type, or a SYMBOL column carrying no index.
     * <p>
     * -1 is not a denial. It says only that a repair of this view cannot follow one key's
     * rows through a posting index and must read the whole range instead, which costs the
     * same write and a larger read.
     * {@link LiveViewSegmentRepairEnvelope#keyedScanGate} answers
     * the same question in the vocabulary {@code live_views()} reports it in, and adds the
     * two the projector cannot see: whether the base scan admits the substitution at all,
     * and whether the view's own output still carries the key.
     */
    public int getIndexedSymbolColumnIndex() {
        return indexedSymbolColumnIndex;
    }

    /**
     * Returns the map key shape the {@link #getKeySink() projector} writes.
     */
    public @NotNull ColumnTypes getKeyColumnTypes() {
        return keyColumnTypes;
    }

    /**
     * Returns the projector that writes one base record's partition key into a map key. It
     * reads the window input's own column indexes - or evaluates this projector's key
     * functions over them - so it must only be handed records from that cursor, and only
     * after {@link #initKeyFunctions} has bound those functions to it.
     * <p>
     * A SYMBOL key column is written as its table-local integer, not as a string. Those
     * integers are stable for the lifetime of one reader, and a repair plans and replays
     * against one pinned reader, so the key identity holds for exactly as long as it is
     * used. A SYMBOL-typed key <i>function</i> is written as its resolved string instead:
     * the integers a function hands out index its own map rather than the reader's.
     */
    public @NotNull RecordSink getKeySink() {
        return keySink;
    }

    /**
     * Returns how many PARTITION BY terms are plain columns of the window's input, which
     * is every term or none: one expression among them puts the whole projector on key
     * functions. Zero therefore also means no term names a column an index could be sought
     * through.
     */
    public int getPartitionByColumnCount() {
        return partitionByColumnIndexes.size();
    }

    /**
     * Returns the window-input column index of the {@code n}-th PARTITION BY column.
     */
    public int getPartitionByColumnIndex(int n) {
        return partitionByColumnIndexes.getQuick(n);
    }

    /**
     * Returns the compiler's own encoding of the PARTITION BY terms this projector was
     * built from. Two functions carry the same identity exactly when their dependencies
     * report the same signature, so a caller holding both compares them rather than
     * re-deriving either one's key layout.
     */
    public String getPartitionSignature() {
        return partitionSignature;
    }

    /**
     * Binds an expression-keyed projector's functions to the cursor whose records the
     * sinks are about to read, and does nothing for a column-keyed one. Every cursor a
     * caller opens needs its own call: a symbol-reading key function resolves through the
     * cursor's symbol tables, and those belong to the cursor rather than to the reader
     * behind it.
     */
    public void initKeyFunctions(
            @NotNull SymbolTableSource symbolTableSource,
            @NotNull SqlExecutionContext executionContext
    ) throws SqlException {
        if (keyFunctions != null) {
            Function.init(keyFunctions, symbolTableSource, executionContext, null);
        }
    }

    private static int resolveIndexedSymbolColumn(IntList columnIndexes, RecordMetadata inputMetadata) {
        if (columnIndexes.size() != 1) {
            return -1;
        }
        final int columnIndex = columnIndexes.getQuick(0);
        if (columnIndex < 0 || columnIndex >= inputMetadata.getColumnCount()) {
            return -1;
        }
        if (!ColumnType.isSymbol(inputMetadata.getColumnType(columnIndex))) {
            return -1;
        }
        return inputMetadata.isColumnIndexed(columnIndex) ? columnIndex : -1;
    }
}
