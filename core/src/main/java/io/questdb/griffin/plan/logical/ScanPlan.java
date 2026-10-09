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

package io.questdb.griffin.plan.logical;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableToken;
import io.questdb.std.IntIntHashMap;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectFactory;

import java.util.Objects;

public final class ScanPlan extends LogicalPlan {
    public static final ObjectFactory<ScanPlan> FACTORY = ScanPlan::new;
    public static final int HINT_FORCE_USE_COVERING = 4;
    public static final int HINT_NO_COVERING = 2;
    public static final int HINT_NO_INDEX = 1;
    public static final int HINT_NO_SYMBOL_PATTERN_INDEX = 8;
    public static final int HINT_PRE_TOUCH = 16;
    private final IntList authorizedColumnIndexes = new IntList();
    private final IntList authorizedColumns = new IntList();
    private final ObjList<BoundExpression> excludedKeys = new ObjList<>();
    private final IntList indexedColumnIds = new IntList();
    private final ObjList<BoundExpression> indexKeys = new ObjList<>();
    private final LongList joinIntervals = new LongList();
    private final IntList referencedColumnIndexes = new IntList();
    private final ObjList<CharSequence> referencedColumnNames = new ObjList<>();
    private final IntList referencedColumnPositions = new IntList();
    private final SortKeys requestedOrder = new SortKeys();
    private final IntList sourceColumnIndexes = new IntList();
    private AccessPath accessPath;
    private int depth;
    private int hints;
    private int indexColumnId = -1;
    private SortDirection indexDirection = SortDirection.ASCENDING;
    private IndexOrder indexOrder = IndexOrder.NONE;
    private IndexRead indexRead = IndexRead.NONE;
    private boolean isRandomAccess = true;
    private boolean isRowOrderRequired = true;
    private boolean isSinglePartition;
    private boolean isTimestampDropped;
    private boolean isUpdate;
    private boolean isWalClientUpdate;
    private WindowJoinStep joinIntervalStep;
    private FunctionExpression keyPattern;
    private CursorExpression keySubquery;
    private BoundExpression limitHi;
    private BoundExpression limitLo;
    private long metadataVersion = -1;
    private int nativeTimestampColumnId = -1;
    private int nativeTimestampType = ColumnType.UNDEFINED;
    private BoundExpression residual;
    private FilterPlan.Algorithm residualAlgorithm;
    private SortDirection scanDirection = SortDirection.ASCENDING;
    private TableToken tableToken;
    private String viewName;
    private int viewPosition = -1;
    private FunctionExpression within;

    @Override
    public void clear() {
        super.clear();
        authorizedColumnIndexes.clear();
        authorizedColumns.clear();
        indexedColumnIds.clear();
        referencedColumnIndexes.clear();
        referencedColumnNames.clear();
        referencedColumnPositions.clear();
        requestedOrder.clear();
        sourceColumnIndexes.clear();
        limitHi = null;
        limitLo = null;
        metadataVersion = -1;
        nativeTimestampColumnId = -1;
        nativeTimestampType = ColumnType.UNDEFINED;
        tableToken = null;
        viewName = null;
        viewPosition = -1;
        isUpdate = false;
        isWalClientUpdate = false;
        isRandomAccess = true;
        isRowOrderRequired = true;
        scanDirection = SortDirection.ASCENDING;
        hints = 0;
        clearAccessPath();
    }

    /**
     * Forgets the access path, before access path planning decides it again.
     */
    public void clearAccessPath() {
        accessPath = null;
        depth = 0;
        excludedKeys.clear();
        indexKeys.clear();
        joinIntervals.clear();
        indexColumnId = -1;
        indexDirection = SortDirection.ASCENDING;
        indexOrder = IndexOrder.NONE;
        indexRead = IndexRead.NONE;
        isSinglePartition = false;
        isTimestampDropped = false;
        joinIntervalStep = null;
        keyPattern = null;
        keySubquery = null;
        residual = null;
        residualAlgorithm = null;
        within = null;
    }

    /**
     * Adds to {@code sink} the names of the table columns the query references or the scan reads, which selecting from
     * the table requires the permission on: the referenced ones in the order the query text first references them,
     * then the ones only the scan reads, in table order.
     */
    public void collectAuthorizedColumnNames(ObjList<CharSequence> sink) {
        for (int i = 0, n = orderAuthorizedColumns(); i < n; i += 3) {
            final int origin = authorizedColumns.getQuick(i + 2);
            sink.add(origin < 0 ? getOutput().getColumnName(-origin - 1) : referencedColumnNames.getQuick(origin));
        }
    }

    /**
     * How the scan reads the table, which access path planning decides; null until it has.
     */
    public AccessPath getAccessPath() {
        return accessPath;
    }

    /**
     * The table indexes of the columns {@link #collectAuthorizedColumnNames} names, in its order.
     */
    public IntList getAuthorizedColumnIndexes() {
        authorizedColumnIndexes.clear();
        for (int i = 0, n = orderAuthorizedColumns(); i < n; i += 3) {
            authorizedColumnIndexes.add(authorizedColumns.getQuick(i + 1));
        }
        return authorizedColumnIndexes;
    }

    /**
     * The row count the filter over a covering index or symbol pattern access path may stop after: the plain LIMIT
     * count of the consumers when they request no order or ascending designated timestamp order, which both access
     * paths keep, otherwise null.
     */
    public BoundExpression getCoveredFilterLimit() {
        if (limitHi != null) {
            return null;
        }
        return requestedOrder.isEmpty() || requestedOrder.size() == 1
                && requestedOrder.getColumnIds().getQuick(0) == getOutput().getTimestampColumnId()
                && requestedOrder.getDirections().getQuick(0) == SortDirection.ASCENDING ? limitLo : null;
    }

    /**
     * The generation depth of the query level the scan belongs to: 0 for the statement, one more for each sub-query
     * around it. It caps how deep interval extraction speculates on sub-query bounds.
     */
    public int getDepth() {
        return depth;
    }

    /**
     * The key values the index access path excludes: the symbol values it reads are all the others.
     */
    public ObjList<BoundExpression> getExcludedKeys() {
        return excludedKeys;
    }

    /**
     * The row count a filter over the scan may stop after: the plain LIMIT count of its consumers when the scan
     * delivers the order they request, otherwise null.
     */
    public BoundExpression getFilterLimit() {
        if (limitHi != null) {
            return null;
        }
        return requestedOrder.isEmpty() || requestedOrder.size() == 1
                && requestedOrder.getColumnIds().getQuick(0) == nativeTimestampColumnId
                && requestedOrder.getDirections().getQuick(0) == scanDirection ? limitLo : null;
    }

    public int getHints() {
        return hints;
    }

    /**
     * The id of the column whose index the access path reads, -1 for none.
     */
    public int getIndexColumnId() {
        return indexColumnId;
    }

    /**
     * The direction the access path reads the row ids of each key from the index in.
     */
    public SortDirection getIndexDirection() {
        return indexDirection;
    }

    public IntList getIndexedColumnIds() {
        return indexedColumnIds;
    }

    /**
     * The key values whose rows the index access path reads.
     */
    public ObjList<BoundExpression> getIndexKeys() {
        return indexKeys;
    }

    /**
     * The requested order the index access path emits its rows in.
     */
    public IndexOrder getIndexOrder() {
        return indexOrder;
    }

    /**
     * How the access path reads the index of {@link #getIndexColumnId}.
     */
    public IndexRead getIndexRead() {
        return indexRead;
    }

    /**
     * The static intervals of the window join master the scan narrows its intervals to, see
     * {@link #getJoinIntervalStep}.
     */
    public LongList getJoinIntervals() {
        return joinIntervals;
    }

    /**
     * The window join step whose window widens the {@link #getJoinIntervals master intervals} the scan of its slave
     * reads, or null.
     */
    public WindowJoinStep getJoinIntervalStep() {
        return joinIntervalStep;
    }

    /**
     * The conjunct whose matching symbol values the pattern access path reads from the index.
     */
    public FunctionExpression getKeyPattern() {
        return keyPattern;
    }

    /**
     * The sub-query whose values the index access path reads the rows of.
     */
    public CursorExpression getKeySubquery() {
        return keySubquery;
    }

    /**
     * The upper bound of the LIMIT that bounds the rows the scan's consumers read, or null.
     */
    public BoundExpression getLimitHi() {
        return limitHi;
    }

    /**
     * The LIMIT that bounds the rows the scan's consumers read, or null when they may read every row; a filter
     * over the scan may stop once it has produced that many rows.
     */
    public BoundExpression getLimitLo() {
        return limitLo;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    public int getNativeTimestampColumnId() {
        return nativeTimestampColumnId;
    }

    public int getNativeTimestampType() {
        return nativeTimestampType;
    }

    public IntList getReferencedColumnIndexes() {
        return referencedColumnIndexes;
    }

    public ObjList<CharSequence> getReferencedColumnNames() {
        return referencedColumnNames;
    }

    public IntList getReferencedColumnPositions() {
        return referencedColumnPositions;
    }

    /**
     * The order the consumers of the scan would like its rows in; an index access path may deliver it.
     */
    public SortKeys getRequestedOrder() {
        return requestedOrder;
    }

    /**
     * The predicate the access path filters the rows it reads with, null for none. The pattern access path filters
     * with it whole, its key pattern included.
     */
    public BoundExpression getResidual() {
        return residual;
    }

    /**
     * How the generator executes the filter over the residual, which order planning records where the generator
     * builds one whose execution it chooses: over page frames, over a covering index and within a pattern scan.
     */
    public FilterPlan.Algorithm getResidualAlgorithm() {
        return residualAlgorithm;
    }

    /**
     * The direction the scan reads the table in, by designated timestamp.
     */
    public SortDirection getScanDirection() {
        return scanDirection;
    }

    public IntList getSourceColumnIndexes() {
        return sourceColumnIndexes;
    }

    public TableToken getTableToken() {
        return tableToken;
    }

    public String getViewName() {
        return viewName;
    }

    public int getViewPosition() {
        return viewPosition;
    }

    /**
     * The within() conjunct whose GeoHash prefixes the indexed LATEST BY access path matches, or null.
     */
    public FunctionExpression getWithin() {
        return within;
    }

    /**
     * Whether a covering index scan keeps an index scan as its backup, for a key that may be NULL.
     */
    public boolean hasCoveringBackup() {
        return !hasHint(HINT_FORCE_USE_COVERING) && hasNullableKey();
    }

    public boolean hasHint(int hint) {
        return (hints & hint) != 0;
    }

    /**
     * Whether an index key may be NULL: a NULL literal, or a bind variable whose value execution supplies.
     */
    public boolean hasNullableKey() {
        for (int i = 0, n = indexKeys.size(); i < n; i++) {
            final BoundExpression key = indexKeys.getQuick(i);
            if (key instanceof BindVariableExpression || key instanceof ConstantExpression constant && switch (ColumnType.tagOf(constant.getDataType())) {
                case ColumnType.CHAR -> constant.getLongValue() == 0;
                case ColumnType.STRING -> constant.getStrValue() == null;
                default -> true;
            }) {
                return true;
            }
        }
        return false;
    }

    @Override
    public LogicalPlan inputAt(int index) {
        throw new IndexOutOfBoundsException("scan has no input: " + index);
    }

    @Override
    public int inputCount() {
        return 0;
    }

    public boolean isRandomAccess() {
        return isRandomAccess;
    }

    /**
     * Whether the access path emits the rows in the order the consumers request.
     */
    public boolean isRequestedOrderDelivered() {
        return accessPath == AccessPath.SORTED_SYMBOL_INDEX || indexOrder != IndexOrder.NONE;
    }

    /**
     * False when every consumer of the scan re-sorts or discards its row order, so an index access path may
     * emit the rows key by key.
     */
    public boolean isRowOrderRequired() {
        return isRowOrderRequired;
    }

    /**
     * Whether the intervals the scan reads lie in a single partition, which lets an index access path emit rows in
     * key order.
     */
    public boolean isSinglePartition() {
        return isSinglePartition;
    }

    /**
     * Whether the access path emits rows out of designated timestamp order, so its metadata declares no timestamp.
     */
    public boolean isTimestampDropped() {
        return isTimestampDropped;
    }

    /**
     * True when the scan reads the target table of an UPDATE, which the binder opened for write.
     */
    public boolean isUpdate() {
        return isUpdate;
    }

    /**
     * True when the scan reads the target of a client-side UPDATE of a WAL table: the client validates the UPDATE
     * against the sequencer metadata and only the WAL apply job reads the rows.
     */
    public boolean isWalClientUpdate() {
        return isWalClientUpdate;
    }

    /**
     * Records the columns of the scan the query references, with the text position of their first reference; the
     * binder calls it once the query is bound, before any pass prunes them. A scan that reads a table through a view
     * records none: the view's own text references its columns, and the view's permission covers them.
     */
    public void markReferencedColumns(IntIntHashMap referencePositions) {
        final OutputSchema output = getOutput();
        for (int i = 0, n = output.getColumnCount(); i < n; i++) {
            final int index = referencePositions.keyIndex(output.getColumnId(i));
            if (index < 0) {
                referencedColumnIndexes.add(sourceColumnIndexes.getQuick(i));
                referencedColumnNames.add(output.getColumnName(i));
                referencedColumnPositions.add(referencePositions.valueAt(index));
            }
        }
    }

    /**
     * Marks the scan as the target of a client-side UPDATE of a WAL table, see {@link #isWalClientUpdate}; the binder
     * decides it from the execution context it binds the UPDATE under.
     */
    public void markWalClientUpdate() {
        isWalClientUpdate = true;
    }

    public ScanPlan of(TableToken tableToken, long metadataVersion, int position) {
        return of(tableToken, metadataVersion, position, false);
    }

    public ScanPlan of(TableToken tableToken, long metadataVersion, int position, boolean isUpdate) {
        this.tableToken = Objects.requireNonNull(tableToken);
        this.metadataVersion = metadataVersion;
        this.isUpdate = isUpdate;
        setPosition(position);
        return this;
    }

    @Override
    public void replaceInput(int index, LogicalPlan input) {
        throw new IndexOutOfBoundsException("scan has no input: " + index);
    }

    public void setAccessPath(AccessPath accessPath, BoundExpression residual) {
        this.accessPath = accessPath;
        this.residual = residual;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public void setHints(int hints) {
        this.hints = hints;
    }

    public void setIndex(int columnId, IndexRead read) {
        indexColumnId = columnId;
        indexRead = read;
    }

    public void setIndexOrder(IndexOrder order, SortDirection direction) {
        indexOrder = order;
        indexDirection = direction;
    }

    public void setJoinIntervalStep(WindowJoinStep step) {
        joinIntervalStep = step;
    }

    public void setKeyPattern(FunctionExpression pattern) {
        keyPattern = pattern;
    }

    public void setKeySubquery(CursorExpression subquery) {
        keySubquery = subquery;
    }

    public void setLimit(BoundExpression lo, BoundExpression hi) {
        limitLo = lo;
        limitHi = hi;
    }

    public void setNativeTimestamp(int columnId, int columnType) {
        nativeTimestampColumnId = columnId;
        nativeTimestampType = columnType;
    }

    public void setRandomAccess(boolean isRandomAccess) {
        this.isRandomAccess = isRandomAccess;
    }

    public void setResidualAlgorithm(FilterPlan.Algorithm residualAlgorithm) {
        this.residualAlgorithm = residualAlgorithm;
    }

    public void setRowOrderRequired(boolean isRowOrderRequired) {
        this.isRowOrderRequired = isRowOrderRequired;
    }

    public void setScanDirection(SortDirection scanDirection) {
        this.scanDirection = scanDirection;
    }

    public void setSinglePartition(boolean isSinglePartition) {
        this.isSinglePartition = isSinglePartition;
    }

    public void setTimestampDropped(boolean isTimestampDropped) {
        this.isTimestampDropped = isTimestampDropped;
    }

    public void setView(String viewName, int viewPosition) {
        this.viewName = viewName;
        this.viewPosition = viewPosition;
    }

    public void setWithin(FunctionExpression within) {
        this.within = within;
    }

    @Override
    public void visitReads(PlanExpressionVisitor visitor) {
        limitLo = PlanReads.expression(limitLo, visitor);
        limitHi = PlanReads.expression(limitHi, visitor);
        indexColumnId = PlanReads.columnId(indexColumnId, -1, visitor);
        residual = PlanReads.expression(residual, visitor);
        PlanReads.expressions(indexKeys, visitor);
        PlanReads.expressions(excludedKeys, visitor);
        keySubquery = (CursorExpression) PlanReads.expression(keySubquery, visitor);
        keyPattern = (FunctionExpression) PlanReads.expression(keyPattern, visitor);
        within = (FunctionExpression) PlanReads.expression(within, visitor);
    }

    /**
     * Fills {@link #authorizedColumns} with one (first reference position, table index, origin) triple per column
     * the scan authorizes, ordered by position then table index, and returns its size. An origin indexes the
     * referenced columns, or is {@code -1 - i} for output column {@code i}.
     */
    private int orderAuthorizedColumns() {
        authorizedColumns.clear();
        for (int i = 0, n = referencedColumnIndexes.size(); i < n; i++) {
            authorizedColumns.add(referencedColumnIndexes.getQuick(i));
            authorizedColumns.add(referencedColumnPositions.getQuick(i));
            authorizedColumns.add(i);
        }
        for (int i = 0, n = sourceColumnIndexes.size(); i < n; i++) {
            authorizedColumns.add(sourceColumnIndexes.getQuick(i));
            authorizedColumns.add(Integer.MAX_VALUE);
            authorizedColumns.add(-i - 1);
        }
        authorizedColumns.sortGroups(3);
        int size = 0;
        for (int i = 0, n = authorizedColumns.size(); i < n; i += 3) {
            final int columnIndex = authorizedColumns.getQuick(i);
            if (size == 0 || authorizedColumns.getQuick(size - 2) != columnIndex) {
                final int position = authorizedColumns.getQuick(i + 1);
                final int origin = authorizedColumns.getQuick(i + 2);
                authorizedColumns.setQuick(size, position);
                authorizedColumns.setQuick(size + 1, columnIndex);
                authorizedColumns.setQuick(size + 2, origin);
                size += 3;
            }
        }
        authorizedColumns.setPos(size);
        authorizedColumns.sortGroups(3);
        return size;
    }

    /**
     * How the scan reads the table, see {@link #getAccessPath}.
     */
    public enum AccessPath {
        /**
         * No row: the intervals or the key values the predicate implies contradict each other, or the residual of an
         * index access path is false.
         */
        EMPTY,
        /**
         * Every partition, or the intervals the predicate implies, page frame by page frame.
         */
        PAGE_FRAMES,
        /**
         * The rows of a single partition in the requested key order, key by key from the index.
         */
        SORTED_SYMBOL_INDEX,
        /**
         * The rows of the index keys, one value or a list, from the index.
         */
        SYMBOL_INDEX,
        /**
         * The rows of every symbol value but the excluded keys, from the index.
         */
        EXCLUDED_SYMBOL_INDEX,
        /**
         * The rows of the symbol values the key sub-query returns, from the index.
         */
        SYMBOL_SUBQUERY,
        /**
         * The rows of the symbol values the key pattern matches, from the index or the covering index, or page frame
         * by page frame, whichever the execution estimates is cheaper.
         */
        SYMBOL_PATTERN,
        /**
         * The distinct values of the key column, from its posting index.
         */
        POSTING_DISTINCT,
        /**
         * The latest row of each value the key sub-query returns.
         */
        LATEST_BY_SUBQUERY,
        /**
         * The latest row of the single index key.
         */
        LATEST_BY_VALUE,
        /**
         * The latest row of each index key, or of every value but the excluded keys.
         */
        LATEST_BY_VALUES,
        /**
         * The latest row of each value of the single key column, from its index.
         */
        LATEST_BY_ALL_INDEXED,
        /**
         * The latest row of each value of the single key column, whose symbol table is static.
         */
        LATEST_BY_STATIC_SYMBOL,
        /**
         * The latest row of each combination of the symbol key columns.
         */
        LATEST_BY_SYMBOLS,
        /**
         * The latest row of each combination of the key columns.
         */
        LATEST_BY_ALL,
        /**
         * The empty stub a client-side UPDATE of a WAL table reads.
         */
        UPDATE_STUB
    }

    /**
     * The requested order an index access path emits its rows in.
     */
    public enum IndexOrder {
        /**
         * Not the requested order.
         */
        NONE,
        /**
         * Ordered by the index key, then by designated timestamp in the index direction.
         */
        KEY,
        /**
         * Ordered by designated timestamp.
         */
        TIMESTAMP
    }

    /**
     * How an access path reads the index of its key column.
     */
    public enum IndexRead {
        /**
         * Not at all.
         */
        NONE,
        /**
         * The row ids of each key, then the rows.
         */
        INDEX,
        /**
         * The column values the covering index includes, without the rows.
         */
        COVERING
    }
}
