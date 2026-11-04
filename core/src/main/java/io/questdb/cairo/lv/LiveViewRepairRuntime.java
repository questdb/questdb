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

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.Misc;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A second compiled runtime of the view's own SELECT, owned by the refresh worker and
 * used by nothing but a converging out-of-order repair.
 * <p>
 * A repair that stops at a finite convergence boundary {@code H} replays {@code [L, H)}
 * and proves, by stopping there, that the state above {@code H} it found was already
 * correct. The replay still has to fold every row it reads into <i>some</i> window
 * state, and before this class existed there was only one - the primary runtime the
 * forward drain stands in. So the repair took the primary's whole state aside into
 * {@link LiveViewCheckpointScratchOverlay}, wiped it, replayed over it and put it back.
 * That copy is as large as the window state itself: at the target scale of 20M keys per
 * anchor day it is the single largest allocation on the repair path, and every deep
 * correction pays it twice - once to copy out, once to copy back.
 * <p>
 * An isolated runtime removes the exchange rather than making it cheaper. The replay
 * folds its rows into <i>this</i> runtime's functions and anchor map, which hold only
 * the keys of the range being repaired, and the primary's are never read, never written
 * and never wiped. What the repair then owes the primary is not its state - which never
 * moved - but the one number the repair did change underneath it: the generation its
 * timeline splice published, which {@link LiveViewCheckpointSealCarryover} re-stamps.
 * <p>
 * <b>Same SQL, same shape, separate state.</b> The runtime compiles the view's own
 * {@code viewSql} through the same path the primary takes, so its decomposition, its
 * window functions, its projections and its anchor plan are the ones the primary would
 * have produced. Only the accumulated state differs. It is built on the first repair
 * that can use it and freed with the primary in
 * {@code LiveViewInstance.freeCompiledArtifacts()}, so a base-schema recompile cannot
 * leave a repair replaying through a shape the view no longer has.
 * <p>
 * <b>It holds one repair's keys between repairs.</b> {@link #reset} rewinds the
 * accumulators through the same {@code toTop()} contract the primary's wipe uses, which
 * empties the maps without handing their capacity back. That is deliberate: a pass
 * repairs segment after segment, and re-allocating a map per segment costs more than the
 * high-water mark of one segment's keys - which is bounded by the segment, and so below
 * the primary's own domain.
 */
public final class LiveViewRepairRuntime implements QuietCloseable {
    private final Function anchorFunction;
    private final LiveViewWindow anchorWindow;
    private final RecordCursorFactory factory;
    private final LiveViewCompiledPlan plan;

    public LiveViewRepairRuntime(
            @NotNull RecordCursorFactory factory,
            @NotNull LiveViewCompiledPlan plan,
            @Nullable LiveViewWindow anchorWindow,
            @Nullable Function anchorFunction
    ) {
        this.factory = factory;
        this.plan = plan;
        this.anchorWindow = anchorWindow;
        this.anchorFunction = anchorFunction;
    }

    @Override
    public void close() {
        // The plan borrows the factory's functions and cross index, so it dies with it
        // and needs no free of its own - the same ownership the primary's
        // freeCompiledArtifacts states.
        Misc.free(anchorWindow);
        Misc.free(anchorFunction);
        Misc.free(factory);
    }

    /**
     * @return this runtime's own anchor window, or null for an unanchored view. The
     * replay's per-row {@code resetPartition} dispatches through it, and its map holds
     * the last-seen anchor value of every key the replay touched
     */
    public @Nullable LiveViewWindow getAnchorWindow() {
        return anchorWindow;
    }

    /**
     * @return the decomposition the replay drives: this runtime's base scan, filter,
     * projections and window factory. Distinct from the primary's at every node, so a
     * cursor opened here cannot advance a memoizer or a function the forward drain reads
     */
    public @NotNull LiveViewCompiledPlan getPlan() {
        return plan;
    }

    public @NotNull WindowRecordCursorFactory getWindowFactory() {
        return plan.getWindowFactory();
    }

    /**
     * Rewinds every accumulator and the anchor map to identity, so the next repair
     * starts from the empty state its own {@code [L, H)} replay rebuilds. Runs before a
     * repair rather than after one - a repair that unwinds leaves whatever it had
     * folded, and only the next repair's reset is guaranteed to run - and again once a
     * repair completes, so a runtime sitting idle holds no keys.
     */
    public void reset() {
        LiveViewRefreshJob.clearWindowState(getWindowFactory(), anchorWindow);
    }
}
