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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.window.LiveViewCheckpointFunctionCompiler;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * Whether a live view's SQL admits the two halves of a deferred, coalesced repair of its
 * closed anchor segments, and when it does not, which gate stands in the way.
 *
 * <h2>What this decides, and what it does not</h2>
 * Nothing. Both answers are diagnostics: the refresh path takes the same route it always
 * did, and {@code live_views()} reports these so a view whose latency cliff is still
 * invisible can be told apart from one that would never take the cheaper route anyway.
 * The gates are the ones a deferred repair would have to hold, written down once so the
 * measurement pass and any later executor read the same predicate.
 *
 * <h2>The two gates</h2>
 * <ul>
 *     <li><b>Deferral.</b> A correction landing in a closed anchor segment may be recorded
 *     and repaired later, off the refresh's critical path, only when every stateful
 *     function the view carries is reset at the segment boundary. Then a row in a closed
 *     segment cannot influence the current segment's output, and leaving it unrepaired for
 *     one pass interval leaves the runtime correct. A bounded ROWS or RANGE function
 *     declared beside the anchored one keeps sliding across that boundary and denies it.</li>
 *     <li><b>Keyed scan.</b> Inside one such segment, only the keys the late rows carry
 *     have changed output, so the replay should follow those keys' rows rather than every
 *     row of the segment. That needs a base scan an index can localize:
 *     {@code PageFrameRecordCursorFactory} substitutes an index-backed row cursor into a
 *     plain full scan for one indexed SYMBOL value, and nothing else. A key this gate
 *     rejects is not a denial - the segment falls back to a whole-segment replay, which
 *     costs the same write and only a larger read.</li>
 * </ul>
 *
 * <h2>What is deliberately absent</h2>
 * Everything a static read of the SQL cannot answer: whether the segment is closed,
 * whether the change set is insert-only, whether the correction sits above the view's own
 * floor. Those are per-repair questions and belong to
 * {@link LiveViewCheckpointRepairPlan}, which answers them against one correction rather
 * than against a definition.
 */
public final class LiveViewBackfillEnvelope {
    /**
     * The view's SQL admits this gate.
     */
    public static final int GATE_AVAILABLE = 0;
    /**
     * A bounded ROWS or RANGE function is declared beside the anchored one. Its frame
     * slides across the anchor boundary, so a row in a closed segment still changes the
     * current segment's output and cannot be deferred.
     */
    public static final int GATE_BOUNDED_FRAME = 3;
    /**
     * The view partitions on several columns. The index names one column's values.
     */
    public static final int GATE_COMPOUND_KEY = 4;
    /**
     * The partition key is computed rather than carried through from the base scan, so no
     * base column's index names its values.
     * <p>
     * A backstop today rather than a case an operator meets: CREATE rejects a PARTITION BY
     * that is not a direct base column reference on an anchored view, so every anchored
     * view's key already traces to a base column. Reporting the trace's empty answer under
     * a wrong gate name would be worse than reporting it under one nothing reaches yet.
     */
    public static final int GATE_EXPRESSION_KEY = 5;
    /**
     * One of the view's window functions sits outside the three dependency plans, so no
     * repair of any shape is localized - see
     * {@link LiveViewCheckpointFunctionCompiler#isDependencyComplete}.
     */
    public static final int GATE_INCOMPLETE_DEPENDENCY = 1;
    /**
     * The partition key is a base SYMBOL column with no index.
     */
    public static final int GATE_KEY_NOT_INDEXED = 7;
    /**
     * The partition key traces to a base column of another type. Only SYMBOL carries the
     * posting index that names one value's rows.
     */
    public static final int GATE_KEY_NOT_SYMBOL = 6;
    /**
     * The view's own output does not carry the partition key unchanged, so a keyed replay
     * could not stamp the rows it re-emits with the key they belong to.
     */
    public static final int GATE_KEY_NOT_PROJECTED = 8;
    /**
     * The view's window functions do not all partition on the same identity, so one
     * key domain does not describe what a keyed replay would have to rebuild.
     */
    public static final int GATE_MIXED_PARTITION_KEYS = 10;
    /**
     * The view carries no anchored window, so it has no closed segment to defer.
     */
    public static final int GATE_NO_ANCHOR_PLAN = 2;
    /**
     * The compiled base scan is not the plain full scan the index-backed row cursor can be
     * substituted into - it already resolves through an index, or through a partition frame
     * cursor that is not a full one.
     */
    public static final int GATE_SCAN_NOT_LOCALIZABLE = 9;
    /**
     * The view has not compiled its SELECT yet, so neither gate has an answer.
     */
    public static final int GATE_UNKNOWN = -1;

    private LiveViewBackfillEnvelope() {
    }

    /**
     * Whether a correction landing in a closed anchor segment could be recorded and
     * repaired off the refresh's critical path, read off the compiled factory alone.
     * <p>
     * The dependency question is asked through
     * {@link LiveViewCheckpointFunctionCompiler#isDependencyComplete} rather than restated,
     * so this cannot drift from the compiler. Note that a stateless function is covered by
     * the RANGE arm at the zero width its empty extent proves, so a view carrying one still
     * needs a RANGE plan - and still defers, because a frame of zero width does not cross
     * the anchor boundary.
     */
    public static int deferralGate(
            @Nullable ObjList<WindowFunction> windowFunctions,
            boolean hasRangePlan,
            boolean hasRowsPlan,
            boolean hasAnchorPlan
    ) {
        if (windowFunctions == null) {
            return GATE_INCOMPLETE_DEPENDENCY;
        }
        if (!LiveViewCheckpointFunctionCompiler.isDependencyComplete(
                windowFunctions, hasRangePlan, hasRowsPlan, hasAnchorPlan)) {
            return GATE_INCOMPLETE_DEPENDENCY;
        }
        if (!hasAnchorPlan) {
            return GATE_NO_ANCHOR_PLAN;
        }
        for (int i = 0, n = windowFunctions.size(); i < n; i++) {
            final LiveViewCheckpointDependency dependency = windowFunctions.getQuick(i).checkpointDependency();
            // isDependencyComplete already refused a null contract, so a dependency here is
            // the compiler's own description of the function's frame.
            if (dependency.isFiniteRows() || dependency.isFiniteRange()) {
                return GATE_BOUNDED_FRAME;
            }
        }
        return GATE_AVAILABLE;
    }

    /**
     * Renders a gate as the token {@code live_views()} reports, or {@code null} for
     * {@link #GATE_UNKNOWN} - the view that has not compiled its SELECT yet, which reads
     * as NULL rather than as a denial.
     */
    public static String gateName(int gate) {
        return switch (gate) {
            case GATE_AVAILABLE -> "available";
            case GATE_INCOMPLETE_DEPENDENCY -> "incomplete dependency";
            case GATE_NO_ANCHOR_PLAN -> "no anchor plan";
            case GATE_BOUNDED_FRAME -> "bounded frame";
            case GATE_COMPOUND_KEY -> "compound key";
            case GATE_EXPRESSION_KEY -> "expression key";
            case GATE_KEY_NOT_SYMBOL -> "key not symbol";
            case GATE_KEY_NOT_INDEXED -> "key not indexed";
            case GATE_KEY_NOT_PROJECTED -> "key not projected";
            case GATE_SCAN_NOT_LOCALIZABLE -> "scan not localizable";
            case GATE_MIXED_PARTITION_KEYS -> "mixed partition keys";
            default -> null;
        };
    }

    /**
     * Whether a replay of one closed segment could follow the affected keys' rows rather
     * than every row of the segment, read off the compiled factory alone.
     * <p>
     * {@code partitionColumnNames} are the anchored window's PARTITION BY columns, which
     * resolve against {@link LiveViewCompiledPlan#getWindowInputMetadata()}; null is the
     * unanchored view, which has no segment to replay.
     */
    public static int keyedScanGate(
            @Nullable LiveViewCompiledPlan plan,
            @Nullable ObjList<String> partitionColumnNames,
            @Nullable ObjList<WindowFunction> windowFunctions
    ) {
        if (plan == null || partitionColumnNames == null) {
            return GATE_NO_ANCHOR_PLAN;
        }
        if (partitionColumnNames.size() != 1) {
            return GATE_COMPOUND_KEY;
        }
        if (!hasOnePartitionIdentity(windowFunctions)) {
            return GATE_MIXED_PARTITION_KEYS;
        }
        final int windowKeyIndex = plan.getWindowInputMetadata()
                .getColumnIndexQuiet(partitionColumnNames.getQuick(0));
        final int baseKeyIndex = plan.traceWindowInputColumnToBaseScan(windowKeyIndex);
        if (baseKeyIndex < 0) {
            return GATE_EXPRESSION_KEY;
        }
        final RecordMetadata baseMetadata = plan.getBaseScanMetadata();
        if (!ColumnType.isSymbol(baseMetadata.getColumnType(baseKeyIndex))) {
            return GATE_KEY_NOT_SYMBOL;
        }
        if (!baseMetadata.isColumnIndexed(baseKeyIndex)) {
            return GATE_KEY_NOT_INDEXED;
        }
        // The same predicate the indexed timestamp-range cursor is gated on, minus the
        // column test above: the substitution needs a full partition frame cursor whose
        // row cursor is an entity and does not already resolve through an index.
        if (!plan.getPageFrameFactory().isBackwardTimestampRangeSupported()) {
            return GATE_SCAN_NOT_LOCALIZABLE;
        }
        // The key has to survive into the view's own schema, because a keyed replay
        // re-emits only its keys' rows and the output must still name which key each row
        // belongs to. An alias does not defeat this - the trace follows column indexes.
        final RecordMetadata outputMetadata = plan.getOutputMetadata();
        for (int i = 0, n = outputMetadata.getColumnCount(); i < n; i++) {
            if (plan.traceOutputColumnToBaseScan(i) == baseKeyIndex) {
                return GATE_AVAILABLE;
            }
        }
        return GATE_KEY_NOT_PROJECTED;
    }

    /**
     * Whether every window function the factory carries partitions on the same identity.
     * The compiler encodes that identity as a signature precisely so two functions can be
     * compared without re-deriving either one's key layout.
     */
    private static boolean hasOnePartitionIdentity(@Nullable ObjList<WindowFunction> windowFunctions) {
        if (windowFunctions == null || windowFunctions.size() == 0) {
            return false;
        }
        String signature = null;
        for (int i = 0, n = windowFunctions.size(); i < n; i++) {
            final LiveViewCheckpointDependency dependency = windowFunctions.getQuick(i).checkpointDependency();
            if (dependency == null) {
                return false;
            }
            final String partitionSignature = dependency.getPartitionSignature();
            if (signature == null) {
                signature = partitionSignature;
            } else if (!signature.equals(partitionSignature)) {
                return false;
            }
        }
        return true;
    }
}
