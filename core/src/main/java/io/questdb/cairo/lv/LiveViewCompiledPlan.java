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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.griffin.engine.functions.columns.ColumnFunction;
import io.questdb.griffin.engine.table.PageFrameRecordCursorFactory;
import io.questdb.griffin.engine.table.SelectedRecordCursorFactory;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowLightRecordCursorFactory;
import io.questdb.griffin.engine.window.CachedWindowRecordCursorFactory;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The decomposition of a live view's compiled SELECT into the nodes the incremental
 * refresh path knows how to drive. One walk of the factory tree, shared by the CREATE
 * validator ({@code CairoEngine.validateLiveViewFactory}) and the refresh job, so the
 * two can never disagree on which shape was accepted.
 *
 * <h2>The accepted shape</h2>
 * <pre>
 * [VirtualRecordCursorFactory]           output projection
 * WindowRecordCursorFactory
 *   [VirtualRecordCursorFactory]         input projection
 *     [SelectedRecordCursorFactory]      input mapping
 *       [filter factory]                 residual WHERE
 *         PageFrameRecordCursorFactory   base scan
 * </pre>
 * Every bracketed node is optional; the planner emits them in exactly this order, and
 * the walk rejects any other node outright rather than guessing. Each optional node is
 * a pure per-row transform - a scalar projection or a column re-mapping - so it neither
 * drops nor reorders rows, which is what lets the refresh path rebuild the same chain
 * over WAL segment rows and get the same answer as a full scan.
 *
 * <h2>Two metadata shapes, not one</h2>
 * Before the projections existed, the window factory's output was the view's schema and
 * its input was the base scan's, so one metadata answered every question. It no longer
 * does, and mixing them up is a silent misread rather than a failure:
 * <ul>
 *     <li>{@link #getOutputMetadata()} is the view's own schema - what the LV table is
 *     created with, what the copier and the in-memory tier are shaped by.</li>
 *     <li>{@link #getWindowInputMetadata()} is what the window functions, their
 *     PARTITION BY keys and the ANCHOR expression resolve against.</li>
 *     <li>{@link #getBaseScanMetadata()} is the base columns the WAL segment cursor
 *     reads, and the filter's own resolution shape.</li>
 * </ul>
 * With no projection in the tree all three collapse onto the window factory's, which is
 * why the pre-projection code could use it for all of them.
 *
 * <h2>Why the filter is not found through {@code getFilter()}</h2>
 * {@link SelectedRecordCursorFactory#getFilter()} <b>delegates to its base</b>, so a
 * probe of "does this node report a filter" answers yes for a mapping node sitting above
 * a filtered scan and hands back the scan's own filter. The walk is therefore driven by
 * node type, and only a node that is neither a projection, a mapping nor the leaf is
 * treated as the filter.
 */
public final class LiveViewCompiledPlan {
    private final RecordCursorFactory filterFactory;
    // The three optional nodes are kept for traceOutputColumnToBaseScan, which walks their
    // functions and cross index. Driving the refresh chain goes through the cursors below
    // instead, so nothing outside this class needs the factories themselves.
    private final SelectedRecordCursorFactory inputMapping;
    private final MappingRecordCursor inputMappingCursor;
    private final VirtualRecordCursorFactory inputProjection;
    private final ProjectingRecordCursor inputProjectionCursor;
    private final VirtualRecordCursorFactory outputProjection;
    private final ProjectingRecordCursor outputProjectionCursor;
    private final PageFrameRecordCursorFactory pageFrameFactory;
    private final RecordCursorFactory root;
    private final WindowRecordCursorFactory windowFactory;

    private LiveViewCompiledPlan(
            RecordCursorFactory root,
            VirtualRecordCursorFactory outputProjection,
            WindowRecordCursorFactory windowFactory,
            VirtualRecordCursorFactory inputProjection,
            SelectedRecordCursorFactory inputMapping,
            RecordCursorFactory filterFactory,
            PageFrameRecordCursorFactory pageFrameFactory
    ) {
        this.root = root;
        this.outputProjection = outputProjection;
        this.windowFactory = windowFactory;
        this.inputProjection = inputProjection;
        this.inputMapping = inputMapping;
        this.filterFactory = filterFactory;
        this.pageFrameFactory = pageFrameFactory;
        // The adapters borrow the compiled factories' functions and cross index, so they
        // are bound to this plan's lifetime and cannot be shared across views. A view
        // with no projection allocates none of them, which is every view that existed
        // before the projections were admitted.
        this.outputProjectionCursor = projectingCursor(outputProjection);
        this.inputProjectionCursor = projectingCursor(inputProjection);
        this.inputMappingCursor = inputMapping != null
                ? new MappingRecordCursor(inputMapping.getColumnCrossIndex())
                : null;
    }

    /**
     * Decomposes {@code factory} against the accepted shape, throwing the reject that
     * names what actually stands in the way.
     *
     * @param factory  the compiled SELECT, still wrapped in its {@link QueryProgress}
     * @param position the CREATE statement position every reject is anchored at
     */
    public static LiveViewCompiledPlan of(RecordCursorFactory factory, int position) throws SqlException {
        // SqlCompiler wraps every compiled query in a QueryProgress factory for registry
        // tracking; unwrap it (and any other transparent wrapper that exposes
        // getBaseFactory()) so we reason about the actual query shape.
        RecordCursorFactory root = factory;
        while (root instanceof QueryProgress) {
            root = root.getBaseFactory();
        }

        RecordCursorFactory node = root;
        VirtualRecordCursorFactory outputProjection = null;
        if (node instanceof VirtualRecordCursorFactory v && containsWindowFactory(v.getBaseFactory())) {
            // A projection over the window's output: `px - avg(px) OVER (...)`. Admitted
            // only when a window factory really is underneath - a projection over a
            // window-free scan is a view with no window function at all, and the reject
            // below has to say so rather than blame the projection.
            outputProjection = v;
            node = v.getBaseFactory();
        }

        rejectCachedWindow(node, position);
        if (!(node instanceof WindowRecordCursorFactory windowFactory)) {
            if (containsWindowFactory(node)) {
                throw SqlException.$(position, "live view select must be a plain windowed scan of the base table; ")
                        .put(describe(node)).put(" is not supported yet");
            }
            throw SqlException.$(position, "live view select must contain at least one window function");
        }

        node = windowFactory.getBaseFactory();
        VirtualRecordCursorFactory inputProjection = null;
        if (node instanceof VirtualRecordCursorFactory v) {
            inputProjection = v;
            node = v.getBaseFactory();
        }
        SelectedRecordCursorFactory inputMapping = null;
        if (node instanceof SelectedRecordCursorFactory s) {
            inputMapping = s;
            node = s.getBaseFactory();
        }
        RecordCursorFactory filterFactory = null;
        if (!(node instanceof PageFrameRecordCursorFactory) && node != null && node.getFilter() != null) {
            filterFactory = node;
            node = node.getBaseFactory();
            // unreachable in practice: a filter factory always wraps a base cursor
            // factory; a filter with no base would be a planner invariant break. Kept
            // as a defensive backstop.
            if (node == null) {
                throw SqlException.$(position, "live view select has a malformed filter factory");
            }
        }
        if (!(node instanceof PageFrameRecordCursorFactory pageFrameFactory) || node.getBaseFactory() != null) {
            throw SqlException.$(position, "live view select must be a simple scan of a single WAL base table; " +
                    "joins, subqueries, GROUP BY, ORDER BY and LIMIT are not supported yet");
        }
        return new LiveViewCompiledPlan(
                root,
                outputProjection,
                windowFactory,
                inputProjection,
                inputMapping,
                filterFactory,
                pageFrameFactory
        );
    }

    /**
     * The base columns the WAL segment cursor reads, and the shape the residual filter
     * and the timestamp-bound cursors resolve against.
     */
    public RecordMetadata getBaseScanMetadata() {
        return pageFrameFactory.getMetadata();
    }

    /**
     * The residual WHERE the refresh path applies row by row, or {@code null} when the
     * view has no filter.
     */
    public @Nullable Function getFilter() {
        return filterFactory != null ? filterFactory.getFilter() : null;
    }

    /**
     * The view's own schema: what the LV table is created with, and what the copier and
     * the in-memory tier are shaped by.
     */
    public RecordMetadata getOutputMetadata() {
        return root.getMetadata();
    }

    public PageFrameRecordCursorFactory getPageFrameFactory() {
        return pageFrameFactory;
    }

    public @NotNull WindowRecordCursorFactory getWindowFactory() {
        return windowFactory;
    }

    /**
     * What the window functions, their PARTITION BY keys and the ANCHOR expression
     * resolve against.
     */
    public RecordMetadata getWindowInputMetadata() {
        return windowFactory.getBaseFactory().getMetadata();
    }

    /**
     * Resolves an output column back to the base-scan column it passes through, or
     * {@code -1} when it is computed rather than passed through. Every node between the
     * two renames or re-indexes columns without renaming the data, so the trace is exact
     * rather than a name match - which an alias would defeat, silently and in the
     * direction that turns SYMBOL caching back on for a base that asked for NOCACHE.
     *
     * @param outputColumnIndex a column index into {@link #getOutputMetadata()}
     * @return the column index into {@link #getBaseScanMetadata()}, or {@code -1}
     */
    public int traceOutputColumnToBaseScan(int outputColumnIndex) {
        int index = outputColumnIndex;
        if (outputProjection != null) {
            index = traceThroughProjection(outputProjection, index);
            if (index < 0) {
                return -1;
            }
        }
        // The window factory is a projection in its own right: output column i is
        // functions[i], a window function for a computed column and a plain column
        // reference for one the SELECT carries through.
        index = unwrapColumnIndex(windowFactory.getFunctions(), index);
        if (index < 0) {
            return -1;
        }
        if (inputProjection != null) {
            index = traceThroughProjection(inputProjection, index);
            if (index < 0) {
                return -1;
            }
        }
        if (inputMapping != null) {
            final IntList crossIndex = inputMapping.getColumnCrossIndex();
            if (index >= crossIndex.size()) {
                return -1;
            }
            index = crossIndex.getQuick(index);
        }
        return index >= 0 && index < getBaseScanMetadata().getColumnCount() ? index : -1;
    }

    /**
     * Resolves a window-input column back to the base-scan column it passes through, or
     * {@code -1} when it is computed rather than passed through.
     * <p>
     * This is the tail of {@link #traceOutputColumnToBaseScan(int)}, entered at the
     * window's input rather than at the view's output, and it answers a different
     * question: a PARTITION BY key resolves against
     * {@link #getWindowInputMetadata() the window's input}, so tracing it to a base column
     * is what decides whether that key is a base column an index can name or an expression
     * only a full scan can produce.
     *
     * @param windowInputColumnIndex a column index into {@link #getWindowInputMetadata()}
     * @return the column index into {@link #getBaseScanMetadata()}, or {@code -1}
     */
    public int traceWindowInputColumnToBaseScan(int windowInputColumnIndex) {
        int index = windowInputColumnIndex;
        if (index < 0) {
            return -1;
        }
        if (inputProjection != null) {
            index = traceThroughProjection(inputProjection, index);
            if (index < 0) {
                return -1;
            }
        }
        if (inputMapping != null) {
            final IntList crossIndex = inputMapping.getColumnCrossIndex();
            if (index >= crossIndex.size()) {
                return -1;
            }
            index = crossIndex.getQuick(index);
        }
        return index >= 0 && index < getBaseScanMetadata().getColumnCount() ? index : -1;
    }

    /**
     * Rebuilds the compiled nodes between the base scan and the window over {@code source},
     * so the window sees rows in the shape its functions were compiled against. Returns
     * {@code source} unchanged when the window reads the scan directly.
     * <p>
     * Belongs above the residual filter and the boundary-freezing cursor, which resolve
     * against the base scan's shape, and below the anchor dispatch, which resolves against
     * the window's input shape.
     */
    public RecordCursor wrapWindowInput(RecordCursor source, SqlExecutionContext executionContext) throws SqlException {
        RecordCursor cursor = source;
        if (inputMappingCursor != null) {
            inputMappingCursor.of(cursor);
            cursor = inputMappingCursor;
        }
        if (inputProjectionCursor != null) {
            inputProjectionCursor.of(cursor, executionContext);
            cursor = inputProjectionCursor;
        }
        return cursor;
    }

    /**
     * Applies the projection over the window's output, turning window-shaped rows into the
     * view's own schema. Returns {@code windowCursor} unchanged when the window's output
     * already is the view's schema.
     * <p>
     * The returned cursor does not own {@code windowCursor}: closing it leaves the window
     * cursor open, because the refresh path's incremental cursor carries accumulator state
     * across cycles and closes on its own terms.
     */
    public RecordCursor wrapWindowOutput(RecordCursor windowCursor, SqlExecutionContext executionContext) throws SqlException {
        if (outputProjectionCursor == null) {
            return windowCursor;
        }
        outputProjectionCursor.of(windowCursor, executionContext);
        return outputProjectionCursor;
    }

    private static boolean containsWindowFactory(RecordCursorFactory factory) {
        for (RecordCursorFactory f = factory; f != null; f = f.getBaseFactory()) {
            if (f instanceof WindowRecordCursorFactory
                    || f instanceof CachedWindowRecordCursorFactory
                    || f instanceof CachedWindowLightRecordCursorFactory) {
                return true;
            }
        }
        return false;
    }

    /**
     * Names {@code node} in the terms the SQL author used, so the reject points at the
     * clause to remove rather than at a factory class name.
     */
    private static CharSequence describe(RecordCursorFactory node) {
        if (node == null) {
            return "this query shape";
        }
        final String name = node.getClass().getSimpleName();
        if (name.contains("Sort")) {
            return "ORDER BY over a window function";
        }
        if (name.contains("Limit")) {
            return "LIMIT over a window function";
        }
        if (name.contains("Distinct")) {
            return "DISTINCT over a window function";
        }
        if (name.contains("Union") || name.contains("Except") || name.contains("Intersect")) {
            return "a set operation over a window function";
        }
        if (name.contains("GroupBy")) {
            return "GROUP BY over a window function";
        }
        if (name.contains("Join")) {
            return "a join over a window function";
        }
        return "this query shape";
    }

    private static ProjectingRecordCursor projectingCursor(VirtualRecordCursorFactory projection) {
        if (projection == null) {
            return null;
        }
        return new ProjectingRecordCursor(
                projection.getFunctions(),
                projection.getPriorityMetadata().getVirtualColumnReservedSlots()
        );
    }

    /**
     * The planner picks a cached factory whenever any window function needs multi-pass
     * evaluation (e.g. lead, percentile). The LIGHT variant is chosen for
     * encoded-sort-eligible, fixed-width outputs; the regular one otherwise. Both mean
     * caching the incremental refresh cannot drive.
     * <p>
     * Checked at the node the window is expected at rather than at the tree's root: with
     * an output projection on top, the cached factory sits one level down and a
     * root-only check would walk straight past it into the generic shape reject.
     */
    private static void rejectCachedWindow(RecordCursorFactory node, int position) throws SqlException {
        if (node instanceof CachedWindowRecordCursorFactory || node instanceof CachedWindowLightRecordCursorFactory) {
            throw SqlException.$(position, "live view select may only use window functions that support incremental refresh; " +
                    "this query requires caching or multi-pass evaluation");
        }
    }

    /**
     * Resolves a column index through a {@link VirtualRecordCursorFactory}, whose
     * functions address their base through the priority metadata's reserved slots rather
     * than directly - the offset that lets a projection reference a column it produced
     * itself.
     */
    private static int traceThroughProjection(VirtualRecordCursorFactory projection, int index) {
        final int virtualIndex = unwrapColumnIndex(projection.getFunctions(), index);
        if (virtualIndex < 0) {
            return -1;
        }
        return projection.getPriorityMetadata().getBaseColumnIndex(virtualIndex);
    }

    private static int unwrapColumnIndex(ObjList<Function> functions, int index) {
        if (functions == null || index < 0 || index >= functions.size()) {
            return -1;
        }
        final ColumnFunction columnFunction = ColumnFunction.unwrap(functions.getQuick(index));
        return columnFunction != null ? columnFunction.getColumnIndex() : -1;
    }
}
