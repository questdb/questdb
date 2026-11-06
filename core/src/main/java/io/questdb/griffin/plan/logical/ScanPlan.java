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

/**
 * A read of a table. Access path planning decides how the scan reads it, see {@link #getAccessPath}, and records the
 * access path facts with it: the index and its keys, the residual and how a filter evaluates it, the intervals, the
 * order the access path emits and the row counts operator planning depends on. Under
 * {@link io.questdb.ParanoiaState#PLAN_PARANOIA_MODE} a read of an access path fact fails until the access path is
 * decided, as {@link #clearAccessPath} forgets it.
 */
public final class ScanPlan extends LogicalPlan {
    public static final ObjectFactory<ScanPlan> FACTORY = ScanPlan::new;
    public static final int HINT_FORCE_USE_COVERING = 4;
    public static final int HINT_NO_COVERING = 2;
    public static final int HINT_NO_INDEX = 1;
    public static final int HINT_NO_SYMBOL_PATTERN_INDEX = 8;
    public static final int HINT_PRE_TOUCH = 16;
    private static final int NO_ORIGIN = Integer.MIN_VALUE;
    private final IntList authorizedColumnIndexes = new IntList();
    private final LongList authorizedColumns = new LongList();
    private final IntList authorizedOrigins = new IntList();
    private final ObjList<BoundExpression> excludedKeys = new ObjList<>();
    private final IntList indexedColumnIds = new IntList();
    private final ObjList<BoundExpression> indexKeys = new ObjList<>();
    private final LongList joinIntervals = new LongList();
    private final IntList referencedColumnIndexes = new IntList();
    private final ObjList<CharSequence> referencedColumnNames = new ObjList<>();
    private final IntList referencedColumnPositions = new IntList();
    private final SortKeys requestedOrder = new SortKeys();
    private final IntList sourceColumnIndexes = new IntList();
    private final LongList staticIntervals = new LongList();
    private AccessPath accessPath;
    private int depth;
    private String expiryPolicy;
    private int hints;
    private int indexColumnId = -1;
    private SortDirection indexDirection = SortDirection.ASCENDING;
    private IndexOrder indexOrder = IndexOrder.NONE;
    private IndexRead indexRead = IndexRead.NONE;
    private IntervalScan intervalScan = IntervalScan.NONE;
    private boolean isRandomAccess = true;
    private boolean isSequencedUpdate;
    private boolean isRewound;
    private boolean isTimestampDropped;
    private boolean isUpdate;
    private WindowJoinStep joinIntervalStep;
    private FunctionExpression keyPattern;
    private CursorExpression keySubquery;
    private BoundExpression limitHi;
    private BoundExpression limitLo;
    private long maxRowCount = Long.MAX_VALUE;
    private long metadataVersion = -1;
    private long minRowCount;
    private int nativeTimestampColumnId = -1;
    private int nativeTimestampType = ColumnType.UNDEFINED;
    private BoundExpression residual;
    private FilterPlan.Algorithm residualAlgorithm;
    private RowOrder rowOrder = RowOrder.REQUIRED;
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
        authorizedOrigins.clear();
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
        expiryPolicy = null;
        viewPosition = -1;
        isUpdate = false;
        isSequencedUpdate = false;
        isRandomAccess = true;
        isRewound = false;
        rowOrder = RowOrder.REQUIRED;
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
        intervalScan = IntervalScan.NONE;
        staticIntervals.clear();
        isTimestampDropped = false;
        joinIntervalStep = null;
        keyPattern = null;
        keySubquery = null;
        maxRowCount = Long.MAX_VALUE;
        minRowCount = 0;
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
        orderAuthorizedColumns();
        for (int i = 0, n = authorizedColumns.size(); i < n; i++) {
            final int origin = authorizedOrigins.getQuick((int) authorizedColumns.getQuick(i));
            sink.add(origin < 0 ? getOutput().getColumnName(-origin - 1) : referencedColumnNames.getQuick(origin));
        }
    }

    /**
     * How the scan reads the table, which access path planning decides; null until it has.
     */
    public AccessPath getAccessPath() {
        checkPhysicalRead("ScanPlan.accessPath");
        return accessPath;
    }

    /**
     * The table indexes of the columns {@link #collectAuthorizedColumnNames} names, in its order.
     */
    public IntList getAuthorizedColumnIndexes() {
        authorizedColumnIndexes.clear();
        orderAuthorizedColumns();
        for (int i = 0, n = authorizedColumns.size(); i < n; i++) {
            authorizedColumnIndexes.add((int) authorizedColumns.getQuick(i));
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
        checkAccessPath("ScanPlan.depth");
        return depth;
    }

    /**
     * The key values the index access path excludes: the symbol values it reads are all the others.
     */
    public ObjList<BoundExpression> getExcludedKeys() {
        checkAccessPath("ScanPlan.excludedKeys");
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
        return requestedOrder.isEmpty() || isNativeTimestampOrderRequested()
                && requestedOrder.getDirections().getQuick(0) == scanDirection ? limitLo : null;
    }

    /**
     * The stored EXPIRE ROWS policy that the read of this table applies; null for any other read.
     */
    public String getExpiryPolicy() {
        return expiryPolicy;
    }

    public int getHints() {
        return hints;
    }

    /**
     * The id of the column whose index the access path reads, -1 for none.
     */
    public int getIndexColumnId() {
        checkAccessPath("ScanPlan.indexColumnId");
        return indexColumnId;
    }

    /**
     * The direction the access path reads the row ids of each key from the index in.
     */
    public SortDirection getIndexDirection() {
        checkAccessPath("ScanPlan.indexDirection");
        return indexDirection;
    }

    public IntList getIndexedColumnIds() {
        return indexedColumnIds;
    }

    /**
     * The key values whose rows the index access path reads.
     */
    public ObjList<BoundExpression> getIndexKeys() {
        checkAccessPath("ScanPlan.indexKeys");
        return indexKeys;
    }

    /**
     * The requested order the index access path emits its rows in.
     */
    public IndexOrder getIndexOrder() {
        checkAccessPath("ScanPlan.indexOrder");
        return indexOrder;
    }

    /**
     * How the access path reads the index of {@link #getIndexColumnId}.
     */
    public IndexRead getIndexRead() {
        checkAccessPath("ScanPlan.indexRead");
        return indexRead;
    }

    /**
     * How the scan reads the partitions of the table through intervals, which access path planning decides.
     */
    public IntervalScan getIntervalScan() {
        checkAccessPath("ScanPlan.intervalScan");
        return intervalScan;
    }

    /**
     * The static intervals of the window join master the scan narrows its intervals to, see
     * {@link #getJoinIntervalStep}.
     */
    public LongList getJoinIntervals() {
        checkAccessPath("ScanPlan.joinIntervals");
        return joinIntervals;
    }

    /**
     * The window join step whose window widens the {@link #getJoinIntervals master intervals} the scan of its slave
     * reads, or null.
     */
    public WindowJoinStep getJoinIntervalStep() {
        checkAccessPath("ScanPlan.joinIntervalStep");
        return joinIntervalStep;
    }

    /**
     * The conjunct whose matching symbol values the pattern access path reads from the index.
     */
    public FunctionExpression getKeyPattern() {
        checkAccessPath("ScanPlan.keyPattern");
        return keyPattern;
    }

    /**
     * The sub-query whose values the index access path reads the rows of.
     */
    public CursorExpression getKeySubquery() {
        checkAccessPath("ScanPlan.keySubquery");
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

    /**
     * The most rows the table may hold for the choices operator planning made from its row count to hold, see
     * {@link #keepRowCountWithin}; {@link Long#MAX_VALUE} when no choice depends on it.
     */
    public long getMaxRowCount() {
        checkAccessPath("ScanPlan.maxRowCount");
        return maxRowCount;
    }

    public long getMetadataVersion() {
        return metadataVersion;
    }

    /**
     * The fewest rows the table may hold for the choices operator planning made from its row count to hold, see
     * {@link #keepRowCountWithin}; 0 when no choice depends on it.
     */
    public long getMinRowCount() {
        checkAccessPath("ScanPlan.minRowCount");
        return minRowCount;
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
        checkAccessPath("ScanPlan.residual");
        return residual;
    }

    /**
     * How the generator applies the residual, which operator planning records where the generator builds a filter over
     * it whose execution it chooses: over page frames, where it may also fold or gate the residual, see
     * {@link FilterPlan.Algorithm}, over a covering index and within a pattern scan.
     */
    public FilterPlan.Algorithm getResidualAlgorithm() {
        checkAccessPath("ScanPlan.residualAlgorithm");
        return residualAlgorithm;
    }

    /**
     * What the consumers of the scan require of the order of its rows.
     */
    public RowOrder getRowOrder() {
        return rowOrder;
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

    /**
     * The intervals of a {@link IntervalScan#STATIC static} interval scan as sorted [lo, hi] pairs at the precision
     * of the designated timestamp, empty for any other scan.
     */
    public LongList getStaticIntervals() {
        checkAccessPath("ScanPlan.staticIntervals");
        return staticIntervals;
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
        checkAccessPath("ScanPlan.within");
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
        checkAccessPath("ScanPlan.indexKeys");
        for (int i = 0, n = indexKeys.size(); i < n; i++) {
            final BoundExpression key = indexKeys.getQuick(i);
            if (key instanceof BindVariableExpression || key instanceof ConstantExpression constant && switch (ColumnType.tagOf(constant.getDataType())) {
                case ColumnType.CHAR -> constant.getLongValue() == 0;
                case ColumnType.STRING -> constant.getStrValue() == null;
                case ColumnType.VARCHAR -> constant.getVarcharValue() == null;
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

    /**
     * Whether a covering index scan has no backup for a key that may be NULL, because the {@code force_use_covering}
     * hint suppresses it.
     */
    public boolean isCoveringBackupSuppressed() {
        return hasHint(HINT_FORCE_USE_COVERING) && hasNullableKey();
    }

    /**
     * Whether the consumers request the order of the native designated timestamp alone, in either direction.
     */
    public boolean isNativeTimestampOrderRequested() {
        return requestedOrder.size() == 1 && requestedOrder.getColumnIds().getQuick(0) == nativeTimestampColumnId;
    }

    public boolean isRandomAccess() {
        return isRandomAccess;
    }

    /**
     * Whether the parallel filter over the residual, as operator planning records it, pre-touches the column memory
     * of the rows it keeps, which the {@code enable_pre_touch} hint asks of the scan.
     */
    public boolean isResidualPreTouched() {
        checkAccessPath("ScanPlan.residualAlgorithm");
        return residualAlgorithm == FilterPlan.Algorithm.PARALLEL && hasHint(HINT_PRE_TOUCH);
    }

    /**
     * Whether the access path emits the rows in the order the consumers request.
     */
    public boolean isRequestedOrderDelivered() {
        checkAccessPath("ScanPlan.indexOrder");
        return accessPath == AccessPath.SORTED_SYMBOL_INDEX || indexOrder.isRequested();
    }

    /**
     * Whether the generator reads the partitions of the table through intervals: the ones the predicate implies, the
     * ones the execution context overrides or the ones a window join narrows the scan to.
     */
    public boolean isIntervalScan() {
        checkAccessPath("ScanPlan.isIntervalScan");
        return intervalScan != IntervalScan.NONE;
    }

    /**
     * True when the scan belongs to the client-side compilation of an UPDATE of a WAL table, see
     * {@link io.questdb.griffin.bind.UpdateTarget#isSequenced()}, and is not the scan of a sub-query whose rows
     * binding consumes, which the client runs: the client only validates the statement, so the scan reads no rows and
     * takes no reader, and only the WAL apply job reads the rows. The target binds the sequencer metadata; a
     * sub-query's scan binds the table it reads as any scan does.
     */
    public boolean isSequencedUpdate() {
        return isSequencedUpdate;
    }

    /**
     * Whether the intervals the scan reads lie in a single partition, which lets an index access path emit rows in
     * key order.
     */
    /**
     * True when a nested loop reads the rows of the scan again for each row of its other input.
     */
    public boolean isRewound() {
        return isRewound;
    }

    /**
     * Whether the access path emits rows out of designated timestamp order, so its metadata declares no timestamp.
     */
    public boolean isTimestampDropped() {
        checkAccessPath("ScanPlan.isTimestampDropped");
        return isTimestampDropped;
    }

    /**
     * True when the scan reads the target table of an UPDATE, which the binder opened for write.
     */
    public boolean isUpdate() {
        return isUpdate;
    }

    /**
     * Records that a choice of operator planning holds only while the table holds {@code min} to {@code max} rows: a
     * cursor open of the scan that finds another row count asks for the statement to be compiled again.
     */
    public void keepRowCountWithin(long min, long max) {
        minRowCount = Math.max(minRowCount, min);
        maxRowCount = Math.min(maxRowCount, max);
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
     * Marks the scan as part of the client-side compilation of an UPDATE of a WAL table, see
     * {@link #isSequencedUpdate}; the binder marks every scan it binds for such a statement but the scans of a
     * sub-query whose rows binding consumes.
     */
    public void markSequencedUpdate() {
        isSequencedUpdate = true;
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

    public void setExcludedKeys(ObjList<BoundExpression> keys) {
        excludedKeys.clear();
        excludedKeys.addAll(keys);
    }

    public void setExpiryPolicy(String expiryPolicy) {
        this.expiryPolicy = expiryPolicy;
    }

    public void setHints(int hints) {
        this.hints = hints;
    }

    public void setIndex(int columnId, IndexRead read) {
        indexColumnId = columnId;
        indexRead = read;
    }

    public void setIndexKeys(ObjList<BoundExpression> keys) {
        indexKeys.clear();
        indexKeys.addAll(keys);
    }

    public void setIndexOrder(IndexOrder order, SortDirection direction) {
        indexOrder = order;
        indexDirection = direction;
    }

    /**
     * Narrows the intervals the scan of a window join slave reads to the static {@code intervals} of the join master,
     * widened by the window of {@code step}.
     */
    public void setJoinIntervals(WindowJoinStep step, LongList intervals) {
        joinIntervalStep = step;
        joinIntervals.clear();
        joinIntervals.add(intervals);
    }

    public void setKeyPattern(FunctionExpression pattern) {
        keyPattern = pattern;
    }

    public void setKeySubquery(CursorExpression subquery) {
        keySubquery = subquery;
    }

    public void setLimit(BoundExpression lo, BoundExpression hi) {
        checkRewritable("ScanPlan.setLimit");
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

    public void setRewound(boolean isRewound) {
        checkRewritable("ScanPlan.setRewound");
        this.isRewound = isRewound;
    }

    public void setRowOrder(RowOrder rowOrder) {
        checkRewritable("ScanPlan.setRowOrder");
        this.rowOrder = rowOrder;
    }

    public void setScanDirection(SortDirection scanDirection) {
        this.scanDirection = scanDirection;
    }

    /**
     * Records how the scan reads the partitions through intervals, with the intervals as sorted [lo, hi] pairs when
     * they are {@link IntervalScan#STATIC static}.
     */
    public void setIntervalScan(IntervalScan intervalScan, LongList staticIntervals) {
        this.intervalScan = intervalScan;
        this.staticIntervals.clear();
        if (intervalScan == IntervalScan.STATIC) {
            this.staticIntervals.add(staticIntervals);
        }
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
    public void visitFacts(PlanFactRecorder visitor) {
        super.visitFacts(visitor);
        visitor.columnIds("indexedColumnIds", indexedColumnIds);
        visitor.facts("referencedColumnIndexes", referencedColumnIndexes);
        visitor.facts("referencedColumnNames", referencedColumnNames);
        visitor.facts("referencedColumnPositions", referencedColumnPositions);
        visitor.columnIds("requestedOrder", requestedOrder.getColumnIds());
        visitor.facts("requestedOrder", requestedOrder.getDirections());
        visitor.facts("sourceColumnIndexes", sourceColumnIndexes);
        visitor.fact("accessPath", accessPath);
        visitor.fact("depth", depth);
        visitor.fact("hints", hints);
        visitor.fact("indexDirection", indexDirection);
        visitor.fact("indexOrder", indexOrder);
        visitor.fact("indexRead", indexRead);
        visitor.fact("intervalScan", intervalScan);
        visitor.fact("isRandomAccess", isRandomAccess);
        visitor.fact("isRewound", isRewound);
        visitor.fact("isSequencedUpdate", isSequencedUpdate);
        visitor.fact("isTimestampDropped", isTimestampDropped);
        visitor.fact("isUpdate", isUpdate);
        visitor.facts("joinIntervals", joinIntervals);
        visitor.reference("joinIntervalStep", joinIntervalStep);
        visitor.fact("maxRowCount", maxRowCount);
        visitor.fact("metadataVersion", metadataVersion);
        visitor.fact("minRowCount", minRowCount);
        visitor.columnId("nativeTimestampColumnId", nativeTimestampColumnId);
        visitor.fact("nativeTimestampType", nativeTimestampType);
        visitor.fact("residualAlgorithm", residualAlgorithm);
        visitor.fact("rowOrder", rowOrder);
        visitor.fact("scanDirection", scanDirection);
        visitor.facts("staticIntervals", staticIntervals);
        visitor.fact("tableToken", tableToken);
        visitor.fact("expiryPolicy", expiryPolicy);
        visitor.fact("viewName", viewName);
        visitor.fact("viewPosition", viewPosition);
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
     * Fails, under paranoia, a read of the access path fact {@code fact} before access path planning decides the
     * access path, see {@link #getAccessPath}.
     */
    private void checkAccessPath(String fact) {
        PlanFacts.checkDecided(accessPath != null, fact);
    }

    /**
     * Fills {@link #authorizedColumns} with one {@code position << 32 | table index} key per column the scan
     * authorizes, ordered by its first reference position then table index, and {@link #authorizedOrigins}, by table
     * index, with the origin of each: an index into the referenced columns, or {@code -1 - i} for output column
     * {@code i}, which takes the position {@link Integer#MAX_VALUE}.
     */
    private void orderAuthorizedColumns() {
        int columnCount = 0;
        for (int i = 0, n = referencedColumnIndexes.size(); i < n; i++) {
            columnCount = Math.max(columnCount, referencedColumnIndexes.getQuick(i) + 1);
        }
        for (int i = 0, n = sourceColumnIndexes.size(); i < n; i++) {
            columnCount = Math.max(columnCount, sourceColumnIndexes.getQuick(i) + 1);
        }
        authorizedOrigins.setAll(columnCount, NO_ORIGIN);
        for (int i = 0, n = referencedColumnIndexes.size(); i < n; i++) {
            final int columnIndex = referencedColumnIndexes.getQuick(i);
            final int origin = authorizedOrigins.getQuick(columnIndex);
            if (origin == NO_ORIGIN || referencedColumnPositions.getQuick(i) < referencedColumnPositions.getQuick(origin)) {
                authorizedOrigins.setQuick(columnIndex, i);
            }
        }
        for (int i = 0, n = sourceColumnIndexes.size(); i < n; i++) {
            final int columnIndex = sourceColumnIndexes.getQuick(i);
            if (authorizedOrigins.getQuick(columnIndex) < 0) {
                authorizedOrigins.setQuick(columnIndex, -i - 1);
            }
        }
        authorizedColumns.clear();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            final int origin = authorizedOrigins.getQuick(columnIndex);
            if (origin != NO_ORIGIN) {
                final int position = origin < 0 ? Integer.MAX_VALUE : referencedColumnPositions.getQuick(origin);
                authorizedColumns.add((long) position << 32 | columnIndex);
            }
        }
        authorizedColumns.sort();
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

    /**
     * How a scan reads the partitions of its table through intervals: the ones the predicate implies, the ones the
     * execution context overrides or the ones a window join narrows the scan to.
     */
    public enum IntervalScan {
        /**
         * Through no intervals: the scan reads every partition.
         */
        NONE,
        /**
         * Through the intervals planning computed, see {@link #getStaticIntervals}.
         */
        STATIC,
        /**
         * Through intervals that runtime values bound, which the generator extracts with their functions.
         */
        RUNTIME
    }

    /**
     * What the consumers of a scan require of the order of its rows.
     */
    public enum RowOrder {
        /**
         * The order the scan reads its rows in.
         */
        REQUIRED,
        /**
         * Nothing, as every consumer re-sorts or discards the row order.
         */
        ANY,
        /**
         * Nothing, as an aggregate without keys folds every row into a single group.
         */
        ANY_SINGLE_GROUP
    }
}
