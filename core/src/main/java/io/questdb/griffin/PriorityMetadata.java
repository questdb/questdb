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

package io.questdb.griffin;

import io.questdb.cairo.AbstractRecordMetadata;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.BitSet;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * This class supports generation of VirtualRecordCursorFactory in allowing functions
 * reference previously used function of the same projection. It contains projection columns
 * but also references the base table metadata as the delegate. It facilitates the
 * priority system in case there is name collision between projection aliases and the
 * base table columns. Such collisions are resolved by preferring the base table. As is the case with
 * other major databases.
 */
public class PriorityMetadata extends AbstractRecordMetadata {
    private final BitSet baseIntWidthUnstable;
    private final RecordMetadata baseMetadata;
    private final BitSet baseRowStable;
    private final ObjList<Function> projectionFunctions;
    private final int virtualColumnReservedSlots;

    public PriorityMetadata(int virtualColumnReservedSlots, RecordMetadata baseMetadata) {
        this(virtualColumnReservedSlots, baseMetadata, null, null);
    }

    /**
     * @param virtualColumnReservedSlots index of the first base column in this metadata's index space
     * @param baseMetadata               the base factory's metadata
     * @param baseFactory                the base factory, or null when the width answers are not
     *                                   needed. Only read here, in the constructor: the base's
     *                                   answers are fixed once it is built, and snapshotting them
     *                                   keeps this metadata usable after the base is peeled or
     *                                   closed (see {@code VirtualRecordCursorFactory#rewrapOverTopK})
     * @param projectionFunctions        the projection's own function list, or null. Held LIVE
     *                                   rather than snapshotted, because it grows as the projection
     *                                   is parsed and a column can reference an earlier one. A
     *                                   later replacement of an entry (the SYMBOL null constant, a
     *                                   memoizer wrapper) must preserve that entry's
     *                                   {@code isIntWidthStable} / {@code isRowStable} answers, or
     *                                   a column function already emitted against them reads at the
     *                                   wrong width
     */
    public PriorityMetadata(
            int virtualColumnReservedSlots,
            RecordMetadata baseMetadata,
            @Nullable RecordCursorFactory baseFactory,
            @Nullable ObjList<Function> projectionFunctions
    ) {
        this.virtualColumnReservedSlots = virtualColumnReservedSlots;
        // hold on to the base metadata, in case this is a join metadata, and it is able to
        // resolve column names containing table aliases.
        this.baseMetadata = baseMetadata;
        this.projectionFunctions = projectionFunctions;
        final int baseColumnCount = baseMetadata.getColumnCount();
        this.baseIntWidthUnstable = new BitSet(baseColumnCount);
        this.baseRowStable = new BitSet(baseColumnCount);
        if (baseFactory != null) {
            for (int i = 0; i < baseColumnCount; i++) {
                if (!baseFactory.isColumnIntWidthStable(i)) {
                    baseIntWidthUnstable.set(i);
                    if (baseFactory.isColumnRowStable(i)) {
                        baseRowStable.set(i);
                    }
                }
            }
        }
    }

    public void add(TableColumnMetadata m) {
        int keyIndex = columnNameIndexMap.keyIndex(m.getColumnName());
        if (keyIndex < 0) {
            throw CairoException.duplicateColumn(m.getColumnName());
        }
        int pos = columnMetadata.size();
        columnMetadata.add(m);
        columnNameIndexMap.putAt(keyIndex, m.getColumnName(), pos);
    }

    public int getBaseColumnIndex(int index) {
        if (index < virtualColumnReservedSlots) {
            return -1;
        }
        return index - virtualColumnReservedSlots;
    }

    public int getVirtualColumnReservedSlots() {
        return virtualColumnReservedSlots;
    }

    @Override
    public int getColumnIndexQuiet(CharSequence columnName, int lo, int hi) {
        int index = baseMetadata.getColumnIndexQuiet(columnName, lo, hi);
        if (index == -1) {
            int keyIndex = columnNameIndexMap.keyIndex(columnName, lo, hi);
            if (keyIndex < 0) {
                return columnNameIndexMap.valueAt(keyIndex);
            }
            // The base splits a composed name on the dot, so this metadata reports splitsOnDot and
            // SqlUtil.getColumnIndexQuiet skips its quote-strip retry for a DOTTED protected alias to
            // avoid mis-splitting it against the base. But this metadata's own projection columns store a
            // content-dotted name clean (a.b) and match it verbatim, so retry the LOCAL map with the
            // protective quotes stripped - only for a dotted interior (an operator token has no dot and
            // is already handled by SqlUtil's retry, which must keep preferring the base). Checking only
            // the local map here cannot mis-split against the base.
            if (SqlUtil.quoteProtectedInteriorDot(columnName, lo, hi) > -1) {
                keyIndex = columnNameIndexMap.keyIndex(columnName, lo + 1, hi - 1);
                if (keyIndex < 0) {
                    return columnNameIndexMap.valueAt(keyIndex);
                }
            }
            return -1;
        }
        return index + virtualColumnReservedSlots;
    }

    @Override
    public TableColumnMetadata getColumnMetadata(int index) {
        if (index < virtualColumnReservedSlots) {
            return columnMetadata.getQuick(index);
        }
        return baseMetadata.getColumnMetadata(index - virtualColumnReservedSlots);
    }

    /**
     * A projection resolves a column reference against this metadata, so this is where the
     * width question is answered for the {@link io.questdb.griffin.engine.functions.columns.IntWideColumn}
     * substitution in {@link FunctionParser#createColumn(int, CharSequence, RecordMetadata)}.
     * A reference to an earlier column of the same projection reads that column's own function -
     * the parser only resolves what it has already parsed, so the function is present - and a
     * reference to a base column reads the base factory's snapshotted answer.
     */
    @Override
    public boolean isColumnIntWidthStable(int columnIndex) {
        final Function projectionFunction = projectionFunction(columnIndex);
        if (projectionFunction != null) {
            return projectionFunction.isIntWidthStable();
        }
        // The short-circuit is load-bearing: BitSet.get computes bitIndex >> 6 and would index
        // words[-1] for a negative argument, so the range test has to run first.
        return columnIndex < virtualColumnReservedSlots
                || !baseIntWidthUnstable.get(columnIndex - virtualColumnReservedSlots);
    }

    /**
     * Answered from the same two sources as {@link #isColumnIntWidthStable(int)}, and consulted
     * only where that one says false. The conservative direction here is false, so an unknown
     * column reads as row-unstable.
     */
    @Override
    public boolean isColumnRowStable(int columnIndex) {
        final Function projectionFunction = projectionFunction(columnIndex);
        if (projectionFunction != null) {
            return projectionFunction.isRowStable();
        }
        // Short-circuits before BitSet.get for the same reason as isColumnIntWidthStable above.
        return columnIndex >= virtualColumnReservedSlots
                && baseRowStable.get(columnIndex - virtualColumnReservedSlots);
    }

    @Override
    public boolean splitsOnDot() {
        // getColumnIndexQuiet delegates the ranged lookup to the base, so a wrapped join splits on
        // the dot too; forward the flag so the compiler's quote-strip retry skips it (as it does for
        // a bare join) instead of mis-splitting a dotted alias into an unrelated table.column.
        return baseMetadata.splitsOnDot();
    }

    /**
     * Returns the projection's own function behind {@code columnIndex}, or null when the index
     * names a base column, names a reserved slot no column was parsed into, or the function list
     * was not supplied.
     */
    private Function projectionFunction(int columnIndex) {
        if (projectionFunctions == null || columnIndex < 0 || columnIndex >= virtualColumnReservedSlots) {
            return null;
        }
        return columnIndex < projectionFunctions.size() ? projectionFunctions.getQuick(columnIndex) : null;
    }
}
