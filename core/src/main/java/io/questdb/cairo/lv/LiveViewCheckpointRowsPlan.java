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

package io.questdb.cairo.lv;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.sql.Function;
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
 * Immutable compiler-owned union of the finite ROWS dependencies in one live view.
 * The ROWS counterpart of {@link LiveViewCheckpointRangePlan}, present when the view's
 * {@code ROWS N PRECEDING ... CURRENT ROW} functions share one partition/order domain
 * and each holds frame-local state, and silent about window functions of another kind.
 * <p>
 * The union takes the widest look-behind of any ROWS function in the view, because the
 * dependency floor has to satisfy every one of them at once. Where the two plans part
 * company is what that width means. A RANGE width is a timestamp offset, so both
 * repair bounds follow from arithmetic and never touch the data. {@code Nmax} is a
 * per-key <b>row</b> count, so neither bound has a closed form: how far back
 * {@code Nmax} rows of one key sit, and how far forward the change reaches, depend on
 * where that key's rows actually are. {@link LiveViewCheckpointRowsBounds} discovers
 * them by counting rows per key over the bounded page-frame scans.
 * <p>
 * That is why this plan carries a key projector - the partition-by key list, its types
 * and a {@link RecordSink} over them - and the range plan does not. The projector is
 * built at compile time against the base factory's metadata, so the discovery scan
 * reads keys out of a page-frame record with no codegen of its own and no dependency
 * on the live window functions' own partition maps.
 * <p>
 * The projector comes in two shapes, because a {@code PARTITION BY} term is not always
 * a column:
 * <ul>
 *     <li><b>Plain base columns.</b> The projector is a sink over their column indexes,
 *     and those indexes are also what lets the discovery seek one key's predecessors
 *     through a SYMBOL index instead of walking every key's rows. A SYMBOL column is
 *     projected as its table-local integer, which is stable for one reader's lifetime -
 *     exactly the scope one repair plans and replays in.</li>
 *     <li><b>Expressions.</b> One key written as {@code upper(sym)} or {@code x % 10}
 *     has no column index to read or to seek through, so <i>every</i> key is projected
 *     through a compiled {@link Function} instead and the discovery falls back to the
 *     unrestricted backward walk. These functions belong to the plan alone - the window
 *     runtime keeps its own copies - and they are bound to each discovery cursor by
 *     {@link #initKeyFunctions}. A SYMBOL-typed key function is projected through its
 *     resolved string rather than an integer, because a function's key space is its own
 *     rather than the reader's.</li>
 * </ul>
 * Either shape only has to agree with itself: the discovery counts rows per key across
 * its own scans, and hands the replay timestamps rather than keys.
 * <p>
 * Every plan is keyed. A keyless ROWS frame compiles to a scalar window function that
 * carries no checkpoint state at all, so no live view can hold one - and a discovery
 * with nothing to count per key would have to count over the whole cursor, which is a
 * different contract rather than a degenerate case of this one.
 */
public final class LiveViewCheckpointRowsPlan implements QuietCloseable {
    private final int functionCount;
    private final ColumnTypes keyColumnTypes;
    private final ObjList<Function> keyFunctions;
    private final RecordSink keySink;
    private final long maxPrecedingRows;
    private final String orderSignature;
    private final IntList partitionByColumnIndexes;
    private final String partitionSignature;
    private final int timestampIndex;
    private final int timestampType;

    public LiveViewCheckpointRowsPlan(
            int functionCount,
            long maxPrecedingRows,
            @NotNull CharSequence partitionSignature,
            @NotNull CharSequence orderSignature,
            @Nullable IntList partitionByColumnIndexes,
            @Nullable ObjList<Function> keyFunctions,
            @NotNull ColumnTypes keyColumnTypes,
            @NotNull RecordSink keySink,
            int timestampIndex,
            int timestampType
    ) {
        // Nmax below 1 is a frame with no look-behind, and a key shape with no column in
        // it is a window with no PARTITION BY. Neither has a checkpoint-capable window
        // function behind it today, and both would put the discovery on a path it cannot
        // count over, so they are refused here rather than half-supported at the scan.
        if (functionCount < 1 || maxPrecedingRows < 1 || timestampIndex < 0 || keyColumnTypes.getColumnCount() < 1) {
            throw new IllegalArgumentException("invalid ROWS dependency plan");
        }
        this.functionCount = functionCount;
        this.maxPrecedingRows = maxPrecedingRows;
        this.partitionSignature = partitionSignature.toString();
        this.orderSignature = orderSignature.toString();
        // Null on the expression path, where no term names a column.
        final int columnKeyCount = partitionByColumnIndexes != null ? partitionByColumnIndexes.size() : 0;
        this.partitionByColumnIndexes = new IntList(columnKeyCount);
        if (columnKeyCount > 0) {
            this.partitionByColumnIndexes.addAll(partitionByColumnIndexes);
        }
        this.keyFunctions = keyFunctions;
        this.keyColumnTypes = keyColumnTypes;
        this.keySink = keySink;
        this.timestampIndex = timestampIndex;
        this.timestampType = timestampType;
    }

    /** Frees the key functions an expression-keyed projector owns. Called by the factory. */
    @Override
    public void close() {
        Misc.freeObjList(keyFunctions);
    }

    public int getFunctionCount() {
        return functionCount;
    }

    /** Returns the map key shape the {@link #getKeySink() projector} writes. */
    public @NotNull ColumnTypes getKeyColumnTypes() {
        return keyColumnTypes;
    }

    /**
     * Returns the projector that writes one base record's partition key into a map key.
     * It reads the base factory's own column indexes - or evaluates the plan's own key
     * functions over them - so it must only be handed records from that factory's
     * cursors, and only after {@link #initKeyFunctions} has bound those functions to the
     * cursor the records come from.
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
     * Returns the widest finite look-behind {@code Nmax} across the view's ROWS
     * functions, in rows of one partition key. A dependency floor that satisfies this
     * count satisfies every function in the view.
     */
    public long getMaxPrecedingRows() {
        return maxPrecedingRows;
    }

    public String getOrderSignature() {
        return orderSignature;
    }

    /**
     * Returns how many PARTITION BY terms are plain base columns, which is every term or
     * none: one expression among them puts the whole projector on key functions. Zero
     * therefore also means the discovery has no column to seek an index through.
     */
    public int getPartitionByColumnCount() {
        return partitionByColumnIndexes.size();
    }

    /** Returns the base-factory column index of the {@code n}-th PARTITION BY column. */
    public int getPartitionByColumnIndex(int n) {
        return partitionByColumnIndexes.getQuick(n);
    }

    public String getPartitionSignature() {
        return partitionSignature;
    }

    /** Returns the designated timestamp's column index in the base factory's metadata. */
    public int getTimestampIndex() {
        return timestampIndex;
    }

    public int getTimestampType() {
        return timestampType;
    }

    /**
     * Binds an expression-keyed projector's functions to the cursor whose records the
     * {@link #getKeySink() sink} is about to read, and does nothing for a column-keyed
     * one. Every cursor the discovery opens needs its own call: a symbol-reading key
     * function resolves through the cursor's symbol tables, and those belong to the
     * cursor rather than to the reader behind it.
     */
    public void initKeyFunctions(
            @NotNull SymbolTableSource symbolTableSource,
            @NotNull SqlExecutionContext executionContext
    ) throws SqlException {
        if (keyFunctions != null) {
            Function.init(keyFunctions, symbolTableSource, executionContext, null);
        }
    }
}
