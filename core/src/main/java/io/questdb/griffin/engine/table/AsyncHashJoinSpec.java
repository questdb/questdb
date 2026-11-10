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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

/**
 * A parallel hash join as the planner compiled it: the left input, which the join streams, its
 * stages, one per join, and the joined rows' metadata, which lists the left input's columns and
 * then every stage's build columns, as the serial joins' metadata does.
 * <p>
 * The spec also records how the left input as compiled answered the two questions a parent asks a
 * join about its order, {@code followedOrderByAdvice()} and {@code getScanDirection()}: the join
 * answers them as the serial join does, by asking its left input, and the left input it keeps is
 * the frame scan the planner peeled from what the query compiled.
 * <p>
 * An INNER first join may swap its inputs at execution, as the serial hash join does: when no parent
 * has fixed the join's order and its left input is the smaller one, it hashes its left input and
 * streams its right one, the first stage's build input. The spec of such a join, a swap candidate,
 * holds the joined rows' column map in that orientation too, and records whether a parent fixed the
 * order, which rules the swap out; see {@link #markOrderDetermined(boolean)}.
 * <p>
 * A later join of the query whose keys read the left input's columns alone, a star join, joins the
 * rows of the joins before it, which no page frame holds: the planner absorbs it into the same
 * operator instead, as a stage of its own; see {@link #addStage(HashJoinStageSpec, RecordMetadata)}.
 * The spec goes on from one factory to the next as the planner absorbs the joins, with the stages
 * and their steppers, and with the answers recorded so far, so a parent's question asked of an
 * earlier factory still holds for the last one.
 * <p>
 * The spec owns the inputs, the stages and the metadata, every join's: a stage's filters compiled
 * against the metadata of its own join and print with it.
 */
public final class AsyncHashJoinSpec implements QuietCloseable {
    /**
     * The joins that one operator runs at most; a later join past them wraps the operator as the
     * master of an ordinary join.
     */
    public static final int MAX_STAGE_COUNT = 32;
    // For each column of the joined rows, its column in the record that holds it: a frame column of
    // the left input, or a payload column of a stage.
    private final IntList columnIndexes = new IntList();
    // The same, once the first join swapped its inputs: a payload column of the first stage for a
    // column of the left input, and a frame column of the right input for a column of the first
    // stage; empty when the join never swaps.
    private final IntList columnIndexesB = new IntList();
    // For each column of the joined rows, the record that holds it: 0 for the left input's frames,
    // k + 1 for stage k's payload.
    private final IntList columnSources = new IntList();
    // The same, once the first join swapped its inputs: 1 for the left input, whose rows the first
    // stage holds, and 0 for the first stage's columns, which the right input's frames hold; empty
    // when the join never swaps.
    private final IntList columnSourcesB = new IntList();
    private final boolean isLeftOrderByAdviceFollowed;
    private final boolean isSwapCandidate;
    private final HashJoinInput leftInput;
    private final int leftScanDirection;
    // The joined rows' metadata of each join before the last, which that join's stage borrows.
    private final ObjList<RecordMetadata> previousMetadata = new ObjList<>();
    private final ObjList<HashJoinStageSpec> stages = new ObjList<>();
    // True once a parent asked the join about its order and the answer constrained it.
    private boolean isOrderDetermined;
    private RecordMetadata metadata;

    /**
     * Takes ownership of the left input, the stage and the metadata.
     *
     * @param leftInput                   the input the join streams
     * @param stage                       the first join
     * @param metadata                    the joined rows' metadata
     * @param isLeftOrderByAdviceFollowed the compiled left input's {@code followedOrderByAdvice()}
     * @param leftScanDirection           the compiled left input's {@code getScanDirection()}
     */
    public AsyncHashJoinSpec(
            HashJoinInput leftInput,
            HashJoinStageSpec stage,
            RecordMetadata metadata,
            boolean isLeftOrderByAdviceFollowed,
            int leftScanDirection
    ) {
        this.leftInput = leftInput;
        this.metadata = metadata;
        this.isLeftOrderByAdviceFollowed = isLeftOrderByAdviceFollowed;
        this.leftScanDirection = leftScanDirection;
        // A stage that holds its keys in the swapped orientation is the first join of a candidate.
        this.isSwapCandidate = stage.getSwapKeyLayout() != null;
        stage.setInputSwapPossible(isSwapCandidate);
        stages.add(stage);
        final IntList leftColumnMap = leftInput.getColumnMap();
        for (int c = 0, n = leftColumnMap.size(); c < n; c++) {
            columnSources.add(0);
            columnIndexes.add(leftColumnMap.getQuick(c));
        }
        // A stage's payload reader reads its payload columns by their payload index.
        final IntList payloadColumns = stage.getPayloadColumns();
        for (int c = 0, n = payloadColumns.size(); c < n; c++) {
            columnSources.add(1);
            columnIndexes.add(c);
        }
        if (isSwapCandidate) {
            // Swapped, the first stage hashes the left input, whose every column is a payload column
            // in the left input's order, and the probe reads the right input's frames, whose columns
            // the stage's payload columns name.
            for (int c = 0, n = leftColumnMap.size(); c < n; c++) {
                columnSourcesB.add(1);
                columnIndexesB.add(c);
            }
            for (int c = 0, n = payloadColumns.size(); c < n; c++) {
                columnSourcesB.add(0);
                columnIndexesB.add(payloadColumns.getQuick(c));
            }
        }
        assert columnSources.size() == metadata.getColumnCount();
    }

    /**
     * Appends the stage of a later join, which the operator then runs after the stages before it, a
     * star join whose keys read the left input's columns alone, and takes over the stage and the
     * metadata of the rows that join produces: the metadata so far, followed by the stage's build
     * columns. The metadata so far stays with the spec, for the earlier stages, which borrow it.
     * Only a spec whose first join never swaps its inputs, and that holds fewer than
     * {@link #MAX_STAGE_COUNT} stages, takes another.
     *
     * @param stage    the later join
     * @param metadata the joined rows' metadata of the later join
     */
    public void addStage(HashJoinStageSpec stage, RecordMetadata metadata) {
        assert !isSwapCandidate : "a join that may swap its inputs takes no later join";
        assert stages.size() < MAX_STAGE_COUNT;
        assert stage.getSwapKeyLayout() == null;
        assert metadata.getColumnCount() == columnSources.size() + stage.getPayloadColumns().size();
        previousMetadata.add(this.metadata);
        this.metadata = metadata;
        stages.add(stage);
        // The stage's payload reader reads its payload columns by their payload index.
        final int source = stages.size();
        for (int c = 0, n = stage.getPayloadColumns().size(); c < n; c++) {
            columnSources.add(source);
            columnIndexes.add(c);
        }
    }

    @Override
    public void close() {
        Throwable failure = Misc.freeObjListBestEffort(null, stages);
        failure = Misc.freeBestEffort(failure, leftInput);
        failure = Misc.freeIfCloseableBestEffort(failure, metadata);
        metadata = null;
        for (int i = 0, n = previousMetadata.size(); i < n; i++) {
            failure = Misc.freeIfCloseableBestEffort(failure, previousMetadata.getQuick(i));
        }
        previousMetadata.clear();
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * For each column of the joined rows, its column in the record that holds it, which
     * {@link #getColumnSource(int)} names: a frame column of the left input, or a payload column of
     * a stage, by its payload index.
     */
    public int getColumnIndex(int column) {
        return columnIndexes.getQuick(column);
    }

    /**
     * For each column of the joined rows, the record that holds it: 0 for the left input's frames,
     * {@code k + 1} for stage {@code k}'s payload.
     */
    public int getColumnSource(int column) {
        return columnSources.getQuick(column);
    }

    public HashJoinInput getLeftInput() {
        return leftInput;
    }

    /**
     * The compiled left input's {@code getScanDirection()}, which the join returns as its own.
     */
    public int getLeftScanDirection() {
        return leftScanDirection;
    }

    public RecordMetadata getMetadata() {
        return metadata;
    }

    public HashJoinStageSpec getStage(int stage) {
        return stages.getQuick(stage);
    }

    public int getStageCount() {
        return stages.size();
    }

    /**
     * True when a stage runs a post-join filter that the planner absorbed into the join.
     */
    public boolean hasPostFilter() {
        for (int i = 0, n = stages.size(); i < n; i++) {
            if (stages.getQuick(i).hasPostFilter()) {
                return true;
            }
        }
        return false;
    }

    /**
     * True while the first join may swap its inputs at execution: it is a candidate, see
     * {@link #isSwapCandidate()}, and no parent has fixed its order, see
     * {@link #markOrderDetermined(boolean)}. Every execution then compares the inputs' sizes.
     */
    public boolean isInputSwapPossible() {
        return isSwapCandidate && !isOrderDetermined;
    }

    /**
     * The compiled left input's {@code followedOrderByAdvice()}, which the join returns as its own.
     */
    public boolean isLeftOrderByAdviceFollowed() {
        return isLeftOrderByAdviceFollowed;
    }

    /**
     * True once a parent asked the join, through any of the factories that held the spec, about its
     * order, and the answer constrained it; see {@link #markOrderDetermined(boolean)}.
     */
    @TestOnly
    public boolean isOrderDetermined() {
        return isOrderDetermined;
    }

    /**
     * True when the first join may swap its inputs, as the serial hash join swaps them: an INNER join
     * whose left input offers random access, over two unfiltered table scans that report their
     * sizes, possibly under projections, whose keys the planner compiled in both orientations.
     */
    public boolean isSwapCandidate() {
        return isSwapCandidate;
    }

    /**
     * Records the answer to a question a parent asked the join about its order, as the serial hash
     * join records it: an answer that constrains the order, an advice the left input followed or a
     * scan direction other than {@code SCAN_DIRECTION_OTHER}, rules the swap out for good, since the
     * parent depends on the left input's order from then on. Only a parent asks; the join, its cursor
     * and the planner read the recorded answers instead.
     */
    public void markOrderDetermined(boolean isDetermined) {
        this.isOrderDetermined |= isDetermined;
        // The first stage's plan names the swap while it is possible.
        stages.getQuick(0).setInputSwapPossible(isInputSwapPossible());
    }

    /**
     * Lets go of the joined rows' metadata of the last join without freeing it, for a planner whose
     * absorption of that join failed after {@link #addStage(HashJoinStageSpec, RecordMetadata)}: the
     * planner frees that metadata itself, and then the spec, with the rest.
     */
    public void releaseMetadata() {
        metadata = null;
    }

    /**
     * A flat record over the joined rows, whose sources the caller binds: the probe scan's frame
     * record and each stage's payload reader; see {@link MultiHashJoinRecord}. A candidate's record
     * holds the column map of the swapped orientation too, in which the probe scan is the right input
     * and the first stage's payload is the left input.
     */
    public MultiHashJoinRecord newJoinedRecord() {
        final ObjList<ColumnTypes> payloadTypes = new ObjList<>();
        for (int i = 0, n = stages.size(); i < n; i++) {
            payloadTypes.add(stages.getQuick(i).getBuildInput().getColumnTypes());
        }
        if (!isSwapCandidate) {
            return new MultiHashJoinRecord(columnSources, columnIndexes, payloadTypes, null, null, null);
        }
        final ObjList<ColumnTypes> payloadTypesB = new ObjList<>();
        payloadTypesB.add(leftInput.getColumnTypes());
        return new MultiHashJoinRecord(columnSources, columnIndexes, payloadTypes, columnSourcesB, columnIndexesB, payloadTypesB);
    }

    /**
     * True when the left input or a stage's build input reads data from outside the database.
     */
    public boolean usesExternalDataSource() {
        if (leftInput.usesExternalDataSource()) {
            return true;
        }
        for (int i = 0, n = stages.size(); i < n; i++) {
            if (stages.getQuick(i).getBuildInput().usesExternalDataSource()) {
                return true;
            }
        }
        return false;
    }
}
