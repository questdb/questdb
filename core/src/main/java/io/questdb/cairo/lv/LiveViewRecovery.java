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

import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.BlockFileReader;
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
import org.jetbrains.annotations.Nullable;

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

    private static final Log LOG = LogFactory.getLog(LiveViewRecovery.class);

    private LiveViewRecovery() {
    }

    /**
     * Reads and structurally validates a live view's
     * {@code _checkpoints/_ring} manifest into {@code candidateOut}, leaving it
     * cleared - and so {@link LiveViewCheckpointRingCandidate#isStructurallyValid()
     * invalid} - when there is nothing to trust later.
     * <p>
     * Makes <em>no</em> trust decision: that compares the manifest's
     * {@code coveredBaseSeqTxn} against the reconciled applied floor, which does
     * not exist until {@code reconcileAppliedFloorAfterRestart} runs on the
     * refresh worker. Everything this method knows about {@code _lv.s} is the
     * raw and legitimately stale value.
     * <p>
     * Validation is structural and cheap by design (design section 7.2): the
     * codec's own invariants, plus {@link FilesFacade#exists} per listed entry.
     * It opens no {@code .cp} file. CRCing each one would cost the full
     * retention byte budget per view on the startup thread, to validate state
     * only an O3 needs; a listed checkpoint that turns out corrupt is evicted
     * lazily at use time, without disturbing its neighbours. The
     * {@code exists()} check is not mere hygiene: the add path unlinks pruned
     * checkpoints even when their publication failed, so a manifest naming a
     * missing file is a reachable state that must fall back rather than promise
     * an anchor that is gone.
     * <p>
     * Every failure - absent, corrupt, version-skewed, or naming a missing
     * checkpoint - is non-fatal and costs a boundary rebuild at most. Ring state
     * is derived; it never invalidates the view.
     *
     * @param ff              files-facade
     * @param ringPath        reusable {@link Path}, re-based on entry
     * @param liveViewDir     absolute path to the LV directory, without the
     *                        {@code _checkpoints/} suffix
     * @param liveViewToken   the LV, for log lines and codec rejection messages
     * @param blockFileReader reusable block-file reader
     * @param manifestReader  reusable parse scratch; cleared by the codec on
     *                        entry, and copied out of before it is reused for
     *                        the next view
     * @param candidateOut    populated on success, cleared otherwise
     */
    public static void readRingCandidate(
            @NotNull FilesFacade ff,
            @NotNull Path ringPath,
            @NotNull Path liveViewDir,
            @NotNull TableToken liveViewToken,
            @NotNull BlockFileReader blockFileReader,
            @NotNull LiveViewCheckpointRingManifestReader manifestReader,
            @NotNull LiveViewCheckpointRingCandidate candidateOut
    ) {
        candidateOut.clear();
        LiveViewCheckpointRingManifest.ringManifestPath(ringPath, liveViewDir);
        if (!ff.exists(ringPath.$())) {
            // Legacy or never published. Not a fault: highest-.cp-only recovery
            // is the fallback the whole design keeps permanently.
            return;
        }
        try {
            blockFileReader.of(ringPath.$());
            manifestReader.of(blockFileReader, liveViewToken);
        } catch (Throwable th) {
            // Two shapes land here and both mean the same thing. The codec
            // rejects a truncated / version-skewed / invariant-violating
            // payload with LV_CHECKPOINT_RING_MANIFEST_INVALID, while the block
            // file layer throws its own critical exception on a checksum
            // mismatch or torn region - it selects a region by version parity
            // and has no automatic fallback to the prior one. Do not filter on
            // the errno.
            LOG.error().$("could not read live view checkpoint ring manifest, falling back to highest checkpoint [view=")
                    .$(liveViewToken)
                    .$(", error=").$(th).I$();
            candidateOut.clear();
            return;
        }
        for (int i = 0, n = manifestReader.getEntryCount(); i < n; i++) {
            final long lvSeqTxn = manifestReader.getEntryLvSeqTxn(i);
            ringPath.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash();
            LiveViewCheckpointWriter.appendCpFileName(ringPath, lvSeqTxn);
            if (!ff.exists(ringPath.$())) {
                // Reject the manifest whole rather than entry by entry: a
                // partial ring is a claim nothing on disk backs, and the
                // membership is what makes the surviving entries meaningful.
                LOG.error().$("live view checkpoint ring manifest references a missing checkpoint, falling back to highest checkpoint [view=")
                        .$(liveViewToken)
                        .$(", lvSeqTxn=").$(lvSeqTxn)
                        .$(", entryIndex=").$(i)
                        .$(", entryCount=").$(n).I$();
                candidateOut.clear();
                return;
            }
        }
        candidateOut.of(manifestReader);
        LOG.info().$("live view checkpoint ring manifest read [view=")
                .$(liveViewToken)
                .$(", generation=").$(candidateOut.getGeneration())
                .$(", coveredBaseSeqTxn=").$(candidateOut.getCoveredBaseSeqTxn())
                .$(", entries=").$(candidateOut.getEntryCount()).I$();
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
     * {@code ringCandidate}, when structurally valid, is an <b>allow-list that
     * exempts its members from both retirement rules</b>. Neither rule can tell
     * a live ring entry from garbage on its own: {@code _lv.s} is a stale lower
     * bound by design - {@code persistState} cannot persist-then-publish, and
     * {@code reconcileAppliedFloorAfterRestart} clamps the floor back up on the
     * refresh worker - so the orphan gate would delete the very entry the
     * reconciled floor is about to validate; and every entry below the head is
     * older than the highest survivor by construction, so the second pass would
     * delete the rest of the ring. Both would leave the restart with the one
     * anchor it already had, which is the gap the manifest exists to close.
     * <p>
     * <b>Exemption keeps the file; it never promotes it to the head.</b> A
     * listed {@code .cp} above {@code appliedWatermark} survives but does not
     * count towards the returned head, because the head is the <em>fallback</em>
     * - the anchor used precisely when the manifest is not trusted. Only the
     * trust decision separates a stale-{@code _lv.s} false positive from a
     * genuine orphan whose commit never landed, and it runs on the refresh
     * worker; a genuine orphan restored as the head would walk the applied
     * watermark up over base commits the LV table never materialised. So the
     * head keeps today's conservative raw-watermark gate, and the manifest
     * carries the entries the reconciled floor can vouch for.
     * <p>
     * The manifest itself needs no special case: it is not named {@code *.cp},
     * so both passes already leave it alone.
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
     *                         unlinked unless {@code ringCandidate} lists it
     * @param nameSink         reusable sink for filename decoding; cleared
     *                         on entry
     * @param ringCandidate    the {@code _ring} manifest {@link #readRingCandidate}
     *                         produced, or null to sweep without an allow-list
     *                         (no manifest, or one that did not validate) -
     *                         which is exactly the legacy behaviour
     * @return the highest surviving {@code <lvSeqTxn>.cp}'s {@code lvSeqTxn},
     * or {@link Numbers#LONG_NULL} when no head survives
     */
    public static long sweepCheckpoints(
            @NotNull FilesFacade ff,
            @NotNull Path sweepPath,
            @NotNull Path liveViewDir,
            long appliedWatermark,
            @NotNull StringSink nameSink,
            @Nullable LiveViewCheckpointRingCandidate ringCandidate
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
                // A real applied watermark is a non-negative base seqTxn; both "uninitialized"
                // sentinels - LiveViewStateReader's -1 (what CairoEngine's startup sweep passes)
                // and Numbers.LONG_NULL - are negative and mean "no watermark, keep every .cp".
                // Guarding on >= 0 recognizes both, so an uninitialized state cannot make the
                // sweep evict live checkpoints as false orphans.
                if (appliedWatermark >= 0 && lvSeqTxn > appliedWatermark) {
                    if (!isListed(ringCandidate, lvSeqTxn)) {
                        unlinkInDir(ff, sweepPath, liveViewDir, nameSink);
                    }
                    // A listed .cp above the raw watermark survives - _lv.s
                    // trails the view's real durable position, so this may be a
                    // sealed entry the reconciled floor will vouch for - but it
                    // does not become the head: only the trust decision tells
                    // that apart from an orphan whose commit never landed.
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
                if (isListed(ringCandidate, lvSeqTxn)) {
                    // The ring's older entries are below the head by
                    // construction; retiring them here would empty the manifest
                    // of everything but its newest entry before the refresh
                    // worker ever sees it.
                    continue;
                }
                unlinkInDir(ff, sweepPath, liveViewDir, nameSink);
            } while (ff.findNext(findPtr2) > 0);
        } finally {
            ff.findClose(findPtr2);
        }
        return highest;
    }

    /**
     * Sweeps a live view's {@code _checkpoints/} directory at startup for
     * rolling seed checkpoints ({@code <key>.scp}), a namespace disjoint
     * from the steady {@code .cp} files {@link #sweepCheckpoints} handles.
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
                    // Steady .cp or foreign noise - not our namespace.
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

    private static boolean isListed(@Nullable LiveViewCheckpointRingCandidate ringCandidate, long lvSeqTxn) {
        return ringCandidate != null && ringCandidate.isListed(lvSeqTxn);
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

    private static long parseLvSeqTxn(StringSink name) {
        return parseKeyBeforeExt(name, LiveViewCheckpointWriter.CP_FILE_EXT.length());
    }

    private static void unlinkInDir(FilesFacade ff, Path sweepPath, Path liveViewDir, CharSequence fileName) {
        sweepPath.of(liveViewDir).concat(LiveViewCheckpointWriter.CHECKPOINT_DIR_NAME).slash().put(fileName);
        ff.removeQuiet(sweepPath.$());
    }
}
