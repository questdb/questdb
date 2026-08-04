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

package io.questdb.griffin.engine.window;

import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.griffin.SqlUtil;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The normalized physical identity of the window several functions may share one partition
 * {@link io.questdb.cairo.map.Map} for: the same key domain, the same row order, the same
 * frame, traversed once.
 * <p>
 * It is deliberately <b>not</b> the SQL the user wrote, and not the compiler's existing
 * {@code groupedWindow} key either. That key is the ORDER BY index list alone - a
 * sort-sharing group - and two functions in it may still partition by different columns or
 * carry different frames, so reusing it as the state-sharing proof would put unrelated
 * accumulators in one map value. Two separately written or separately named windows share
 * a spec when everything below is equal; two spellings of one window are one spec, and one
 * window name over two different resolved specifications is two.
 *
 * <h2>What the identity carries, and why each part is in it</h2>
 * <ul>
 *     <li><b>the direct-column partition indexes, in order, and the map key column types.</b>
 *     The indexes are the key's semantics and the types are its physical layout; the two are
 *     separate because a live-view compile writes a SYMBOL key through its resolved string,
 *     so one column list can produce two key layouts and those cannot share a map;</li>
 *     <li><b>the effective order column indexes and directions, and whether the order was
 *     dismissed</b> against the base cursor's own scan. A cumulative frame's contents are
 *     the rows the traversal has already passed, so two functions agree on that frame only
 *     while they agree on the order the rows arrive in;</li>
 *     <li><b>the framing mode and the normalized bounds</b>, read after
 *     {@code WindowContextImpl} has converted a RANGE bound into the designated timestamp's
 *     units and folded {@code EXCLUDE CURRENT ROW} into the high bound - the numbers the
 *     runtime evaluates rather than the ones the model records;</li>
 *     <li><b>the exclusion kind</b>, beside the bound it was folded into. The fold covers
 *     the one shape that reads as a lagging high bound; carrying the kind as well is what
 *     keeps a future exclusion mode from being invisible here;</li>
 *     <li><b>the pass kind and pass-1 scan direction</b>, which are the function's rather
 *     than the window's. Two functions share a traversal only while they agree on how many
 *     passes it takes and which way the first one runs;</li>
 *     <li><b>the designated timestamp index and type</b>, which a RANGE frame's bounds are
 *     expressed in.</li>
 * </ul>
 * Function-specific {@code IGNORE NULLS} is absent on purpose: it changes what an
 * accumulator absorbs, which is the component identity's business, not what the group
 * traverses. It belongs here only if an implementation is ever found whose group traversal
 * it changes.
 *
 * <h2>What declines outright</h2>
 * {@link #of} answers null - there is no group to join - for:
 * <ul>
 *     <li><b>an unpartitioned window.</b> A cumulative function with no PARTITION BY keeps
 *     its state in scalar fields and owns no map at all, so there is nothing to co-locate
 *     and a group would only add a probe;</li>
 *     <li><b>a PARTITION BY term that is not a direct compiled column</b> of the base
 *     metadata's own type. There is no canonical, type-resolved fingerprint that proves two
 *     compiled expressions equivalent, and rendering the SQL text is not that proof;</li>
 *     <li><b>an ORDER BY term that is not a direct base column</b>, for the same reason: the
 *     spec would then claim two windows are ordered alike on the strength of two expressions
 *     nothing compared.</li>
 * </ul>
 * Both expression cases are the conservative first slice rather than a permanent rule, and
 * both are what a canonical compiled expression identity would later admit.
 */
public final class WindowMapSpec {
    private final int exclusionKind;
    private final int framingMode;
    private final IntList keyColumnTypes;
    private final IntList orderColumnIndexes;
    private final IntList orderDirections;
    private final boolean orderDismissed;
    private final IntList partitionColumnIndexes;
    private final WindowFunction.Pass1ScanDirection pass1ScanDirection;
    private final int passCount;
    private final long rowsHi;
    private final long rowsLo;
    private final int scanDirection;
    private final int timestampIndex;
    private final int timestampType;

    private WindowMapSpec(
            IntList partitionColumnIndexes,
            IntList keyColumnTypes,
            IntList orderColumnIndexes,
            IntList orderDirections,
            boolean orderDismissed,
            int scanDirection,
            int framingMode,
            long rowsLo,
            long rowsHi,
            int exclusionKind,
            int passCount,
            WindowFunction.Pass1ScanDirection pass1ScanDirection,
            int timestampIndex,
            int timestampType
    ) {
        this.partitionColumnIndexes = partitionColumnIndexes;
        this.keyColumnTypes = keyColumnTypes;
        this.orderColumnIndexes = orderColumnIndexes;
        this.orderDirections = orderDirections;
        this.orderDismissed = orderDismissed;
        this.scanDirection = scanDirection;
        this.framingMode = framingMode;
        this.rowsLo = rowsLo;
        this.rowsHi = rowsHi;
        this.exclusionKind = exclusionKind;
        this.passCount = passCount;
        this.pass1ScanDirection = pass1ScanDirection;
        this.timestampIndex = timestampIndex;
        this.timestampType = timestampType;
    }

    /**
     * Snapshots the window {@code function} was just compiled under, or returns null when
     * the shape is one this build does not group - see the class javadoc for the three
     * declines and what each costs.
     * <p>
     * It must be called while the context is still live. {@code WindowContextImpl} is a
     * mutable per-function scratch the compiler clears after every window column, and the
     * key types it exposes are the compiler's own reused list, so everything read here is
     * copied rather than referenced.
     *
     * @param context           the window context {@code function} was compiled under,
     *                          still configured
     * @param orderBy           the window's ORDER BY terms, as written
     * @param orderByDirections one direction per term
     * @param orderDismissed    whether the compiler proved the base cursor already
     *                          produces this order, so no sort stands between the two
     * @param function          the compiled window function, read for its pass structure
     * @param baseMetadata      the metadata the window's expressions were compiled against,
     *                          which is what an ORDER BY term's name resolves through
     * @param recordTypes       the types of the record those expressions read, by index. It
     *                          is {@code baseMetadata} itself for a streaming compile; a
     *                          cached compile passes the record chain's own type list,
     *                          because the chain metadata leaves a hole where every window
     *                          output sits and so cannot be asked how many indexes it spans
     */
    public static @Nullable WindowMapSpec of(
            @NotNull WindowContext context,
            @NotNull ObjList<ExpressionNode> orderBy,
            @NotNull IntList orderByDirections,
            boolean orderDismissed,
            @NotNull WindowFunction function,
            @NotNull RecordMetadata baseMetadata,
            @NotNull ColumnTypes recordTypes
    ) {
        if (context.isEmpty()) {
            return null;
        }
        final VirtualRecord partitionByRecord = context.getPartitionByRecord();
        final ColumnTypes contextKeyTypes = context.getPartitionByKeyTypes();
        if (partitionByRecord == null || contextKeyTypes == null) {
            return null;
        }
        final ObjList<? extends Function> partitionByFunctions = partitionByRecord.getFunctions();
        final int partitionCount = partitionByFunctions == null ? 0 : partitionByFunctions.size();
        if (partitionCount == 0 || contextKeyTypes.getColumnCount() != partitionCount) {
            return null;
        }
        final IntList partitionColumnIndexes = new IntList(partitionCount);
        final IntList keyColumnTypes = new IntList(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            final int columnIndex = WindowAccumulatorDescriptor.directColumnIndex(
                    partitionByFunctions.getQuick(i),
                    recordTypes
            );
            if (columnIndex < 0) {
                return null;
            }
            partitionColumnIndexes.add(columnIndex);
            keyColumnTypes.add(contextKeyTypes.getColumnType(i));
        }
        final int orderCount = orderBy.size();
        final IntList orderColumnIndexes = new IntList(orderCount);
        final IntList orderDirections = new IntList(orderCount);
        for (int i = 0; i < orderCount; i++) {
            final ExpressionNode node = orderBy.getQuick(i);
            if (node == null || node.token == null) {
                return null;
            }
            final int columnIndex = SqlUtil.getColumnIndexQuiet(baseMetadata, node.token);
            if (columnIndex < 0) {
                return null;
            }
            orderColumnIndexes.add(columnIndex);
            orderDirections.add(orderByDirections.getQuick(i));
        }
        return new WindowMapSpec(
                partitionColumnIndexes,
                keyColumnTypes,
                orderColumnIndexes,
                orderDirections,
                orderDismissed,
                context.getOrderByScanDirection(),
                context.getFramingMode(),
                context.getRowsLo(),
                // The folded high bound, which is the one the factories dispatch on and the
                // one the frame actually ends at.
                context.getRowsHi(),
                context.getExclusionKind(),
                function.getPassCount(),
                function.getPass1ScanDirection(),
                context.getTimestampIndex(),
                context.getTimestampType()
        );
    }

    public int getExclusionKind() {
        return exclusionKind;
    }

    public int getFramingMode() {
        return framingMode;
    }

    /**
     * Returns the number of columns in the group's map key, which is the number of
     * PARTITION BY terms.
     */
    public int getKeyColumnCount() {
        return keyColumnTypes.size();
    }

    /**
     * Returns the type key column {@code index} is written as, which is the partition
     * term's compiled type except where the compile resolves a SYMBOL through its string.
     */
    public int getKeyColumnType(int index) {
        return keyColumnTypes.getQuick(index);
    }

    public int getOrderColumnCount() {
        return orderColumnIndexes.size();
    }

    public int getOrderColumnIndex(int index) {
        return orderColumnIndexes.getQuick(index);
    }

    public int getOrderDirection(int index) {
        return orderDirections.getQuick(index);
    }

    public WindowFunction.Pass1ScanDirection getPass1ScanDirection() {
        return pass1ScanDirection;
    }

    public int getPassCount() {
        return passCount;
    }

    public int getPartitionColumnCount() {
        return partitionColumnIndexes.size();
    }

    /**
     * Returns PARTITION BY term {@code index}'s column in the base metadata. Every term is
     * a direct column, or {@link #of} would have declined the whole spec.
     */
    public int getPartitionColumnIndex(int index) {
        return partitionColumnIndexes.getQuick(index);
    }

    public long getRowsHi() {
        return rowsHi;
    }

    public long getRowsLo() {
        return rowsLo;
    }

    /**
     * The direction the base cursor is scanned in when the window's order was dismissed
     * against it, or {@code RecordCursorFactory.SCAN_DIRECTION_OTHER} when it was not.
     */
    public int getScanDirection() {
        return scanDirection;
    }

    public int getTimestampIndex() {
        return timestampIndex;
    }

    public int getTimestampType() {
        return timestampType;
    }

    /**
     * Whether the compiler proved the base cursor already produces this window's order, so
     * the functions read it without a sort in between.
     */
    public boolean isOrderDismissed() {
        return orderDismissed;
    }

    /**
     * Whether {@code other} describes the same physical window traversal, and so may share
     * one partition map with this one.
     */
    public boolean isSameSpec(@NotNull WindowMapSpec other) {
        return this == other
                || (framingMode == other.framingMode
                && rowsLo == other.rowsLo
                && rowsHi == other.rowsHi
                && exclusionKind == other.exclusionKind
                && orderDismissed == other.orderDismissed
                && scanDirection == other.scanDirection
                && passCount == other.passCount
                && pass1ScanDirection == other.pass1ScanDirection
                && timestampIndex == other.timestampIndex
                && timestampType == other.timestampType
                && partitionColumnIndexes.equals(other.partitionColumnIndexes)
                && keyColumnTypes.equals(other.keyColumnTypes)
                && orderColumnIndexes.equals(other.orderColumnIndexes)
                && orderDirections.equals(other.orderDirections));
    }

    @Override
    public String toString() {
        return "WindowMapSpec{partitionColumns=" + partitionColumnIndexes
                + ", keyColumnTypes=" + keyColumnTypes
                + ", orderColumns=" + orderColumnIndexes
                + ", orderDirections=" + orderDirections
                + ", orderDismissed=" + orderDismissed
                + ", scanDirection=" + scanDirection
                + ", framingMode=" + framingMode
                + ", rowsLo=" + rowsLo
                + ", rowsHi=" + rowsHi
                + ", exclusionKind=" + exclusionKind
                + ", passCount=" + passCount
                + ", pass1ScanDirection=" + pass1ScanDirection
                + '}';
    }
}
