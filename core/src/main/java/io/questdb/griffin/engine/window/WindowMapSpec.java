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
import io.questdb.std.str.StringSink;
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
 *     <li><b>the canonical identity of the partition terms, in order, and the map key column
 *     types.</b> The terms are the key's semantics and the types are its physical layout; the
 *     two are separate because a live-view compile writes a SYMBOL key through its resolved
 *     string, so one term list can produce two key layouts and those cannot share a map. A
 *     term is a resolved column index where it is a direct column reference and a
 *     {@link WindowKeyExpressionIdentity rendered expression} where it is not;</li>
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
 *     <li><b>a PARTITION BY term {@link WindowKeyExpressionIdentity} cannot name</b> - a bind
 *     variable, a subquery, a random call, an unresolvable column. An expression term that it
 *     can name joins on that identity rather than on being a column;</li>
 *     <li><b>an ORDER BY term that is not a direct base column.</b> The identity above would
 *     name one, and what has not been worked out is the rest: an ordered group is a cached
 *     factory's sort bucket, and whether two expression orders that render alike are sorted
 *     alike is that factory's question rather than this one.</li>
 * </ul>
 *
 * <h2>An expression key is a borrowed projection</h2>
 * A spec whose terms are all direct columns says everything the group's key projection needs -
 * the columns, and the record they are read off. An expression-keyed one does not, so it also
 * carries the <b>compiled</b> terms of the function it was snapshotted for, non-owning, for
 * {@link WindowMapState} to evaluate the key through. They stay that function's to free, and
 * they are alive and initialized for exactly as long as the group is: both are the factory's,
 * and every window function's {@code init} runs before the first row of any traversal.
 */
public final class WindowMapSpec {
    private final int exclusionKind;
    private final int framingMode;
    private final IntList keyColumnTypes;
    private final IntList orderColumnIndexes;
    private final IntList orderDirections;
    private final boolean orderDismissed;
    private final ObjList<? extends Function> partitionByFunctions;
    private final IntList partitionColumnIndexes;
    private final String partitionKeyIdentity;
    private final WindowFunction.Pass1ScanDirection pass1ScanDirection;
    private final int passCount;
    private final long rowsHi;
    private final long rowsLo;
    private final int scanDirection;
    private final int timestampIndex;
    private final int timestampType;
    private int specHash;

    private WindowMapSpec(
            IntList partitionColumnIndexes,
            String partitionKeyIdentity,
            ObjList<? extends Function> partitionByFunctions,
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
        this.partitionKeyIdentity = partitionKeyIdentity;
        this.partitionByFunctions = partitionByFunctions;
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
     * @param partitionBy       the window's PARTITION BY terms, as parsed, in the order the
     *                          context's compiled functions were built from them. They are read
     *                          here and not retained: the identity rendered off them is
     *                          {@link WindowKeyExpressionIdentity}'s string, so a pooled node
     *                          the compiler recycles after this statement is never referenced
     *                          again
     * @param orderBy           the window's ORDER BY terms, as written
     * @param orderByDirections one direction per term
     * @param orderDismissed    whether the compiler proved the base cursor already
     *                          produces this order, so no sort stands between the two
     * @param function          the compiled window function, read for its pass structure
     * @param baseMetadata      the metadata the window's expressions were compiled against,
     *                          which is what a term's name resolves through - an ORDER BY one,
     *                          and a column inside an expression PARTITION BY term
     * @param recordTypes       the types of the record those expressions read, by index. It
     *                          is {@code baseMetadata} itself for a streaming compile; a
     *                          cached compile passes the record chain's own type list,
     *                          because the chain metadata leaves a hole where every window
     *                          output sits and so cannot be asked how many indexes it spans
     */
    public static @Nullable WindowMapSpec of(
            @NotNull WindowContext context,
            @NotNull ObjList<ExpressionNode> partitionBy,
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
        if (partitionCount == 0
                || contextKeyTypes.getColumnCount() != partitionCount
                || partitionBy.size() != partitionCount) {
            return null;
        }
        final IntList partitionColumnIndexes = new IntList(partitionCount);
        final IntList keyColumnTypes = new IntList(partitionCount);
        final StringSink identity = new StringSink();
        boolean expressionKey = false;
        for (int i = 0; i < partitionCount; i++) {
            final Function term = partitionByFunctions.getQuick(i);
            final int columnIndex = WindowAccumulatorDescriptor.directColumnIndex(term, recordTypes);
            if (i > 0) {
                identity.putAscii(WindowKeyExpressionIdentity.TERM_SEPARATOR);
            }
            // A direct column term renders through the same identity an expression one does, so
            // a group's key is one string however its terms were written - but off the compiled
            // function rather than off the tree. Both routes answer the same thing for such a
            // term, and reading it off the function is what makes this build's admissions a
            // superset of the last one's: a key that bound before cannot stop binding because
            // its name resolves some way this rendering does not.
            //
            // What the index beside the identity adds is the term's own answer to "which column
            // is this" - the key projection reads it, and so does the guard that lets a count
            // over the partition key join a row count - and an expression term answers -1 to
            // both.
            if (columnIndex >= 0) {
                WindowKeyExpressionIdentity.renderColumn(columnIndex, term.getType(), identity);
            } else {
                if (!WindowKeyExpressionIdentity.render(partitionBy.getQuick(i), term, baseMetadata, identity)) {
                    return null;
                }
                expressionKey = true;
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
                identity.toString(),
                // Kept only where the key cannot be written off the record's own columns. The
                // reference is the compiling function's and is never freed here.
                expressionKey ? partitionByFunctions : null,
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

    /**
     * The compiled PARTITION BY terms a group has to evaluate to write its key, or null when
     * the key is direct columns and the record carries it already.
     * <p>
     * <b>Non-owning.</b> They belong to the window function this spec was snapshotted for,
     * which frees them, initializes them and outlives every group built on this spec.
     */
    public @Nullable ObjList<? extends Function> getPartitionByFunctions() {
        return partitionByFunctions;
    }

    public int getPartitionColumnCount() {
        return partitionColumnIndexes.size();
    }

    /**
     * Returns PARTITION BY term {@code index}'s column in the base metadata, or {@code -1}
     * when the term is an expression rather than a direct column of that metadata's own type.
     */
    public int getPartitionColumnIndex(int index) {
        return partitionColumnIndexes.getQuick(index);
    }

    /**
     * The canonical identity of the whole PARTITION BY list - one
     * {@link WindowKeyExpressionIdentity rendered term} per column, separated by
     * {@link WindowKeyExpressionIdentity#TERM_SEPARATOR}. It is the whole of the key half of
     * {@link #isSameSpec}.
     */
    public String getPartitionKeyIdentity() {
        return partitionKeyIdentity;
    }

    /**
     * Whether any PARTITION BY term is an expression rather than a direct column, which is
     * what decides how a group writes its key - see {@link #getPartitionByFunctions()}.
     */
    public boolean hasExpressionPartitionKey() {
        return partitionByFunctions != null;
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

    /**
     * A hash over exactly the fields {@link #isSameSpec} compares, so two specs that group
     * together cannot land in different buckets. The two have to be changed together: a
     * field added to one and not the other is the way a bucketed lookup starts splitting a
     * group in half.
     * <p>
     * Every field it reads is final and no composite it reads is mutated after the
     * constructor, so the value is computed once and kept in one field, {@code String}'s
     * way rather than with a separate computed flag. A spec whose fields genuinely hash to
     * zero recomputes on each call, which costs fourteen operations and can never be
     * wrong; the two-field form would instead let another thread see the flag set before
     * the value it guards.
     */
    public int getSpecHash() {
        int h = specHash;
        if (h == 0) {
            h = framingMode;
            h = h * 31 + Long.hashCode(rowsLo);
            h = h * 31 + Long.hashCode(rowsHi);
            h = h * 31 + exclusionKind;
            h = h * 31 + (orderDismissed ? 1 : 0);
            h = h * 31 + scanDirection;
            h = h * 31 + passCount;
            // Compared with == in isSameSpec, which for an enum is ordinal equality.
            h = h * 31 + (pass1ScanDirection == null ? 0 : pass1ScanDirection.ordinal());
            h = h * 31 + timestampIndex;
            h = h * 31 + timestampType;
            h = h * 31 + partitionKeyIdentity.hashCode();
            h = h * 31 + keyColumnTypes.hashCode();
            h = h * 31 + orderColumnIndexes.hashCode();
            h = h * 31 + orderDirections.hashCode();
            specHash = h;
        }
        return h;
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
                // The identity subsumes the column indexes beside it: a direct column term
                // renders as the index it resolved to and the type it reads it as.
                && partitionKeyIdentity.equals(other.partitionKeyIdentity)
                && keyColumnTypes.equals(other.keyColumnTypes)
                && orderColumnIndexes.equals(other.orderColumnIndexes)
                && orderDirections.equals(other.orderDirections));
    }

    @Override
    public String toString() {
        return "WindowMapSpec{partitionKey=" + partitionKeyIdentity
                + ", partitionColumns=" + partitionColumnIndexes
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
