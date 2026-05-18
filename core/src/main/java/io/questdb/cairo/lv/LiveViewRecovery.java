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
 * file-system housekeeping inside a live view's {@code _checkpoints/}
 * directory; the actual deserialisation lives in {@link LiveViewCheckpointReader}
 * and the refresh worker's first-cycle hook.
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

    private LiveViewRecovery() {
    }

    /**
     * Sweeps a live view's {@code _checkpoints/} directory at startup and
     * returns the highest surviving {@code <lvSeqTxn>.cp} filename's parsed
     * {@code lvSeqTxn}, or {@link Numbers#LONG_NULL} when nothing is left.
     * <p>
     * Cleans:
     * <ul>
     *     <li>Any {@code *.cp.tmp} orphans (crashed between
     *     {@code MemoryMARW} commit and the rename to {@code .cp}).</li>
     *     <li>Any {@code .cp} whose embedded {@code lvSeqTxn} is strictly
     *     greater than {@code appliedWatermark} - these are orphans of a
     *     crash that lost the {@code _txn} advance, e.g. under
     *     {@code cairo.commit.mode=async}.</li>
     *     <li>Any {@code .cp} older than the highest surviving one (older
     *     unlink survivor of a crash between the rename and the prior-cp
     *     unlink).</li>
     *     <li>Any filename that does not match the {@code <16-digit>.cp}
     *     pattern - foreign noise.</li>
     * </ul>
     * <p>
     * Failure to unlink any single file is logged through
     * {@link FilesFacade#removeQuiet} (best-effort); the sweep continues so
     * a transient FS error does not block startup. The first post-restart
     * refresh cycle re-runs the sweep on each LV by virtue of the same
     * "highest .cp wins" rule, so stragglers self-clean.
     *
     * @param ff               files-facade
     * @param sweepPath        reusable {@link Path} pointed at the LV's
     *                         directory before the call; the method mutates it
     *                         to address {@code _checkpoints/} and individual
     *                         files but always re-bases on entry, so the
     *                         caller can hand any Path in
     * @param liveViewDir      absolute path to the LV directory (without the
     *                         {@code _checkpoints/} suffix)
     * @param appliedWatermark base seqTxn position from {@code _lv.s}; any
     *                         {@code .cp} ahead of this is an orphan and gets
     *                         unlinked
     * @param nameSink         reusable sink for filename decoding; cleared
     *                         on entry
     * @return the highest surviving {@code <lvSeqTxn>.cp}'s {@code lvSeqTxn},
     * or {@link Numbers#LONG_NULL} when no head survives
     */
    public static long sweepCheckpoints(
            @NotNull FilesFacade ff,
            @NotNull Path sweepPath,
            @NotNull Path liveViewDir,
            long appliedWatermark,
            @NotNull StringSink nameSink
    ) {
        sweepPath.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
        if (!ff.exists(sweepPath.$())) {
            return Numbers.LONG_NULL;
        }
        long highest = Numbers.LONG_NULL;
        final long findPtr = ff.findFirst(sweepPath.$());
        if (findPtr == 0) {
            return Numbers.LONG_NULL;
        }
        try {
            // First pass: unlink .cp.tmp orphans + .cp ahead of applied_watermark
            // + anything that does not look like our naming convention. Track
            // the highest surviving lvSeqTxn so the second pass can retire
            // older survivors.
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
                if (Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_TMP_FILE_EXT)) {
                    unlinkInDir(ff, sweepPath, liveViewDir, nameSink);
                    continue;
                }
                if (!Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_FILE_EXT)) {
                    // Foreign noise. Leave it alone - a future operator audit
                    // can investigate. Removing files we did not put there is
                    // not our place.
                    continue;
                }
                final long lvSeqTxn = parseLvSeqTxn(nameSink);
                if (lvSeqTxn == Numbers.LONG_NULL) {
                    // Malformed filename - leave alone for the same reason.
                    continue;
                }
                if (appliedWatermark != Numbers.LONG_NULL && lvSeqTxn > appliedWatermark) {
                    unlinkInDir(ff, sweepPath, liveViewDir, nameSink);
                    continue;
                }
                if (highest == Numbers.LONG_NULL || lvSeqTxn > highest) {
                    highest = lvSeqTxn;
                }
            } while (ff.findNext(findPtr) > 0);
        } finally {
            ff.findClose(findPtr);
        }
        if (highest == Numbers.LONG_NULL) {
            return Numbers.LONG_NULL;
        }
        // Second pass: retire .cp files older than the survivor.
        sweepPath.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME);
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
                if (!Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_FILE_EXT)
                        || Chars.endsWith(nameSink, LiveViewCheckpointWriter.CP_TMP_FILE_EXT)) {
                    continue;
                }
                final long lvSeqTxn = parseLvSeqTxn(nameSink);
                if (lvSeqTxn == Numbers.LONG_NULL || lvSeqTxn == highest) {
                    continue;
                }
                unlinkInDir(ff, sweepPath, liveViewDir, nameSink);
            } while (ff.findNext(findPtr2) > 0);
        } finally {
            ff.findClose(findPtr2);
        }
        return highest;
    }

    private static long parseLvSeqTxn(StringSink name) {
        final int len = name.length();
        final int extLen = LiveViewCheckpointWriter.CP_FILE_EXT.length();
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
        sweepPath.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash().put(fileName);
        ff.removeQuiet(sweepPath.$());
    }
}
