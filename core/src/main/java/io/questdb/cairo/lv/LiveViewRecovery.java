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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;

/**
 * Static helpers for live-view restart recovery. Concerned strictly with
 * seed-checkpoint file-system housekeeping inside a live view's
 * {@code _checkpoints/} directory; the actual deserialisation lives in
 * {@link LiveViewCheckpointReader} and the refresh worker's first-cycle hook.
 * The versioned checkpoint timeline owns its own bounded reconciliation
 * ({@link LiveViewCheckpointLifecycle}) and needs nothing from here.
 * <p>
 * No forward-scan reconstruction of {@code lvConsumedSeqTxn} from the LV WAL
 * is required: {@code CairoEngine.advanceLiveViewConsumedSeqTxn} persists the
 * new floor into {@code _lv.s} before publishing it in-memory, so the durable
 * value never sits ahead of the LV WAL state. A persist failure leaves the
 * floor at the previous durable value; the next successful apply re-publishes
 * it. The worst case is a temporary leak of base WAL segments that {@code
 * WalPurgeJob} retains longer than necessary, bounded by the apply-to-persist
 * window. If that leak becomes material under {@code cairo.commit.mode=async},
 * the forward-scan recovery from the LV WAL is the proper fix.
 */
public final class LiveViewRecovery {

    private static final Log LOG = LogFactory.getLog(LiveViewRecovery.class);

    private LiveViewRecovery() {
    }

    /**
     * Sweeps a live view's {@code _checkpoints/} directory at startup for
     * rolling seed checkpoints ({@code <key>.scp}), a namespace disjoint
     * from the versioned checkpoint timeline's own files.
     * <p>
     * Always unlinks {@code *.scp.tmp} orphans. When {@code isSeeding} the
     * view is mid-sweep: retain the highest {@code .scp} (the resume source),
     * retire older ones, and return its key. When not seeding the view has
     * either completed or never seeded: retire every {@code .scp} (leftovers
     * from a crash before the post-completion unlink) and return
     * {@link Numbers#LONG_NULL}.
     *
     * @param ff          files-facade
     * @param sweepPath   reusable {@link Path}, re-based on entry
     * @param liveViewDir absolute path to the LV directory (no
     *                    {@code _checkpoints/} suffix)
     * @param isSeeding   whether the view loaded in SEEDING state
     * @param nameSink    reusable sink for filename decoding; cleared on entry
     * @return the highest surviving {@code .scp} key when seeding, else
     * {@link Numbers#LONG_NULL}
     */
    public static long sweepSeedCheckpoints(
            @NotNull FilesFacade ff,
            @NotNull Path sweepPath,
            @NotNull Path liveViewDir,
            boolean isSeeding,
            @NotNull StringSink nameSink
    ) {
        sweepPath.of(liveViewDir).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
        if (!ff.exists(sweepPath.$())) {
            return Numbers.LONG_NULL;
        }
        long highest = Numbers.LONG_NULL;
        final long findPtr = ff.findFirst(sweepPath.$());
        if (findPtr == 0) {
            return Numbers.LONG_NULL;
        }
        try {
            do {
                final long namePtr = ff.findName(findPtr);
                if (namePtr == 0) {
                    continue;
                }
                nameSink.clear();
                if (!Utf8s.utf8ToUtf16Z(namePtr, nameSink)) {
                    continue;
                }
                if (Chars.equals(nameSink, ".") || Chars.equals(nameSink, "..")) {
                    continue;
                }
                if (Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_SCP_TMP_FILE_EXT)) {
                    unlinkInDir(ff, sweepPath, liveViewDir, nameSink);
                    continue;
                }
                if (!Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_SCP_FILE_EXT)) {
                    // Foreign noise - not our namespace.
                    continue;
                }
                final long key = parseKeyBeforeExt(nameSink, LiveViewCheckpointWriter.CP_SCP_FILE_EXT.length());
                if (key == Numbers.LONG_NULL) {
                    continue;
                }
                if (!isSeeding) {
                    // Completed (or never-seeded) view: no .scp should
                    // survive. Retire leftovers from a pre-unlink crash.
                    unlinkInDir(ff, sweepPath, liveViewDir, nameSink);
                    continue;
                }
                if (highest == Numbers.LONG_NULL || key > highest) {
                    highest = key;
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
        }
        if (!isSeeding || highest == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        // Second pass: retire .scp files older than the survivor.
        sweepPath.of(liveViewDir).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
        final long findPtr2 = ff.findFirst(sweepPath.$());
        if (findPtr2 == 0) {
            return highest;
        }
        try {
            do {
                final long namePtr = ff.findName(findPtr2);
                if (namePtr == 0) {
                    continue;
                }
                nameSink.clear();
                if (!Utf8s.utf8ToUtf16Z(namePtr, nameSink)) {
                    continue;
                }
                if (!Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_SCP_FILE_EXT)
                        || Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_SCP_TMP_FILE_EXT)) {
                    continue;
                }
                final long key = parseKeyBeforeExt(nameSink, LiveViewCheckpointWriter.CP_SCP_FILE_EXT.length());
                if (key == Numbers.LONG_NULL || key == highest) {
                    continue;
                }
                unlinkInDir(ff, sweepPath, liveViewDir, nameSink);
            } while (ff.findNext(findPtr2) > 0);
        } finally {
            ff.findClose(findPtr2);
        }
        return highest;
    }

    private static long parseKeyBeforeExt(StringSink name, int extLen) {
        final int len = name.length();
        final int digitsLen = len - extLen;
        if (digitsLen <= 0) {
            return Numbers.LONG_NULL;
        }
        try {
            return Numbers.parseLong(name, 0, digitsLen);
        } catch (NumericException e) {
            return Numbers.LONG_NULL;
        }
    }

    private static void unlinkInDir(FilesFacade ff, Path sweepPath, Path liveViewDir, CharSequence fileName) {
        sweepPath.of(liveViewDir).concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME).slash().put(fileName);
        ff.removeQuiet(sweepPath.$());
    }
}
