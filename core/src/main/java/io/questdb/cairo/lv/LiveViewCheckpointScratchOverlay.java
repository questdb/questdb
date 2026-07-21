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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.NotNull;

/**
 * In-RAM copy of the compiled factory's window-function state, taken before a
 * localized out-of-order repair replays over it and put back afterwards. This is
 * the scratch overlay of design section 12.4: "construct scratch state without
 * mutating the published runtime".
 * <p>
 * A repair with a finite convergence boundary {@code H} replays only
 * {@code [L, H)}, so the state the replay ends on describes {@code H - 1} rather
 * than the runtime frontier. That state is not the runtime's - the frontier state
 * the repair found was already correct, which is precisely what a finite {@code H}
 * proves (see
 * {@link LiveViewCheckpointRepairPlan#isRuntimeStatePreserved()}). The replay still
 * has to run through the live {@link WindowFunction} instances, because the
 * compiled cursor stack owns them and there is only one of it, so the overlay
 * takes the published state out of the way first and restores it when the replay
 * is done.
 * <p>
 * The copy goes through the same freeze/restore contract a checkpoint uses
 * ({@link LiveViewFunctionSnapshot}), so a function needs no separate clone path
 * and the overlay inherits its bounded, length-framed decoding. Only functions
 * that report {@link WindowFunction#supportsCheckpointState()} are carried;
 * {@link #capture} and {@link #restore} walk the same list through the same
 * filter, and the frame count is reconciled on the way back so a mismatched pair
 * fails loudly instead of restoring one function's bytes into another.
 * <p>
 * One instance per refresh worker, reused across repairs. The buffer is released
 * as soon as the state is handed back: it is as large as the whole window state,
 * and a repair that converges below the runtime frontier is rare enough that
 * holding that much native memory for the worker's life costs more than the
 * allocation it saves.
 */
public final class LiveViewCheckpointScratchOverlay implements QuietCloseable {
    private static final long PAGE_SIZE = 64 * 1024;
    private boolean captured;
    // (offset, length) of each captured function's payload within mem, in
    // window-function order and skipping the functions that carry no checkpoint
    // state.
    private final LongList frames = new LongList();
    private MemoryCARW mem;

    /**
     * Copies every checkpoint-capable function's state out of the compiled factory.
     * Discards whatever the overlay held before, so a repair that failed to restore
     * cannot leak stale state into the next one.
     */
    public void capture(@NotNull ObjList<WindowFunction> functions) {
        clear();
        mem = Vm.getCARWInstance(PAGE_SIZE, Integer.MAX_VALUE, MemoryTag.NATIVE_LIVE_VIEW_IN_MEM);
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction f = functions.getQuick(i);
            if (!f.supportsCheckpointState()) {
                continue;
            }
            final long payloadStart = mem.getAppendOffset();
            LiveViewFunctionSnapshot.write(mem, f);
            frames.add(payloadStart);
            frames.add(mem.getAppendOffset() - payloadStart);
        }
        captured = true;
    }

    /** Drops the captured state and releases the buffer holding it. */
    public void clear() {
        frames.clear();
        captured = false;
        mem = Misc.free(mem);
    }

    @Override
    public void close() {
        clear();
    }

    public boolean isCaptured() {
        return captured;
    }

    /**
     * Puts the captured state back into the same functions it came from. The
     * function objects are the compiled factory's, so their format versions and key
     * shapes are the ones the capture wrote with; a disagreement here is a bug
     * rather than corruption, and {@link LiveViewFunctionSnapshot} throws on it.
     */
    public void restore(@NotNull ObjList<WindowFunction> functions) {
        if (!captured) {
            throw CairoException.critical(0).put("live view repair overlay holds no captured state");
        }
        int frame = 0;
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction f = functions.getQuick(i);
            if (!f.supportsCheckpointState()) {
                continue;
            }
            if (frame + 2 > frames.size()) {
                throw CairoException.critical(0)
                        .put("live view repair overlay is short of captured functions [captured=")
                        .put(frames.size() / 2)
                        .put(']');
            }
            LiveViewFunctionSnapshot.restore(
                    mem,
                    frames.getQuick(frame),
                    frames.getQuick(frame + 1),
                    f,
                    f.checkpointStateFormatVersion()
            );
            frame += 2;
        }
        if (frame != frames.size()) {
            throw CairoException.critical(0)
                    .put("live view repair overlay holds unclaimed function state [captured=")
                    .put(frames.size() / 2)
                    .put(", restored=")
                    .put(frame / 2)
                    .put(']');
        }
        clear();
    }
}
