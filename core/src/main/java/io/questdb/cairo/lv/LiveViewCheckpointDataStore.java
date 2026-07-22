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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.LongHashSet;
import io.questdb.std.LongIntHashMap;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.HashMap;

/**
 * Owns the immutable data-segment lifecycle. Compaction candidates protect all
 * source and target segments until redirected metadata is published or the
 * candidate is abandoned. Purge combines candidate ownership with the A/B slot
 * floor and live generation pins before unlinking a zero-reference segment.
 *
 * <p>Repack validates metadata bounds and copies encoded bytes without decoding
 * or calculating a data checksum. Repeated references to one physical page are
 * written once and retain sharing in the returned references. The caller writes
 * those references into candidate roots and publishes them through the metadata
 * store; this component remains independent of the Phase-3 checkpoint API.</p>
 */
public class LiveViewCheckpointDataStore implements Closeable {

    private static final Log LOG = LogFactory.getLog(LiveViewCheckpointDataStore.class);
    private final LongIntHashMap candidateOwnershipCounts = new LongIntHashMap();
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final FilesFacade ff;
    private final LiveViewCheckpointMetaStore metaStore;
    private final LiveViewCheckpointSegmentDirectoryReader segmentDirectory;
    private final PurgeSweep sweep = new PurgeSweep();
    private boolean isOpen;

    public LiveViewCheckpointDataStore(
            @NotNull CairoConfiguration configuration,
            @NotNull LiveViewCheckpointMetaStore metaStore
    ) {
        this.configuration = configuration;
        this.ff = configuration.getFilesFacade();
        this.metaStore = metaStore;
        this.segmentDirectory = new LiveViewCheckpointSegmentDirectoryReader(configuration);
    }

    public Candidate beginCandidate() {
        ensureOpen();
        return new Candidate(this);
    }

    @Override
    public synchronized void close() {
        assert candidateOwnershipCounts.size() == 0
                : "live view checkpoint compaction candidates leaked: " + candidateOwnershipCounts.size();
        candidateOwnershipCounts.clear();
        Misc.free(segmentDirectory);
        Misc.free(checkpointsDir);
        isOpen = false;
    }

    public void of(@Transient @NotNull Path checkpointsDir) {
        if (isOpen) {
            throw CairoException.critical(0).put("live view checkpoint data store already open");
        }
        this.checkpointsDir.of(checkpointsDir);
        isOpen = true;
    }

    /**
     * Processes the current checksummed zero-reference queue. Failed unlinks
     * remain queued and are retried on the next call.
     */
    public synchronized PurgeResult purge() {
        ensureOpen();
        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            segmentDirectory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            sweep.of(
                    metaStore.getOldestValidSuperblockGeneration(),
                    metaStore.getMinPinnedGeneration()
            );
            // Walking the catalogue costs the segment count, not the timeline
            // length: the tree carries one entry per segment however many logical
            // checkpoints reference it.
            segmentDirectory.iterateAll(sweep);
            return new PurgeResult(
                    sweep.purgedSegments,
                    sweep.failedSegments,
                    sweep.purgedBytes,
                    sweep.liveSegments,
                    sweep.obsoleteBytes
            );
        }
    }

    private synchronized void abortCandidate(@NotNull Candidate candidate) {
        for (int i = 0, n = candidate.targetSegmentIds.size(); i < n; i++) {
            final long segmentId = candidate.targetSegmentIds.get(i);
            try (Path path = new Path()) {
                LiveViewCheckpointLayout.dataSegmentTmpPath(path, checkpointsDir, segmentId);
                ff.removeQuiet(path.$());
                LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
                ff.removeQuiet(path.$());
            }
        }
        releaseCandidate(candidate);
    }

    private static long checkedAdd(long a, long b) {
        if (b > Long.MAX_VALUE - a) {
            throw CairoException.critical(0).put("live view checkpoint purged byte count overflow");
        }
        return a + b;
    }

    private static void copyRef(
            @NotNull LiveViewCheckpointStatePageRef from,
            @NotNull LiveViewCheckpointStatePageRef to
    ) {
        to.of(
                from.getSegmentId(),
                from.getOffset(),
                from.getStoredLength(),
                from.getDecodedLength(),
                from.getPageKind(),
                from.getCodec(),
                from.getRowCount(),
                from.getFlags()
        );
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0).put("live view checkpoint data store is not open");
        }
    }

    private boolean isCandidateOwned(long segmentId) {
        return candidateOwnershipCounts.get(segmentId) > 0;
    }

    private void own(@NotNull Candidate candidate, long segmentId) {
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint candidate segment id must be non-negative, was ")
                    .put(segmentId);
        }
        if (!candidate.ownedSegmentIds.add(segmentId)) {
            return;
        }
        final int count = candidateOwnershipCounts.get(segmentId);
        if (count == Integer.MAX_VALUE) {
            candidate.ownedSegmentIds.remove(segmentId);
            throw CairoException.critical(0)
                    .put("live view checkpoint candidate ownership count overflow, segmentId=")
                    .put(segmentId);
        }
        candidateOwnershipCounts.put(segmentId, count < 0 ? 1 : count + 1);
    }

    private synchronized long repack(
            @NotNull Candidate candidate,
            long targetSegmentId,
            @NotNull ObjList<LiveViewCheckpointStatePageRef> sourceRefs,
            @NotNull ObjList<LiveViewCheckpointStatePageRef> targetRefs
    ) {
        ensureOpen();
        if (sourceRefs.size() == 0) {
            throw CairoException.critical(0).put("cannot repack an empty live view checkpoint page set");
        }
        if (candidate.targetSegmentIds.contains(targetSegmentId)) {
            throw CairoException.critical(0)
                    .put("duplicate live view checkpoint compaction target, segmentId=")
                    .put(targetSegmentId);
        }

        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            segmentDirectory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());
            if (targetSegmentId <= segmentDirectory.lastSegmentId()) {
                throw CairoException.critical(0)
                        .put("live view checkpoint compaction target id must be monotonic, segmentId=")
                        .put(targetSegmentId);
            }
            candidate.redirects.clear();
            candidate.stagedRefs.clear();
            try (LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration)) {
                writer.of(checkpointsDir, targetSegmentId);
                candidate.targetSegmentIds.add(targetSegmentId);
                own(candidate, targetSegmentId);
                final HashMap<Long, LiveViewCheckpointDataSegmentReader> readers = new HashMap<>();
                try {
                    for (int i = 0, n = sourceRefs.size(); i < n; i++) {
                        final LiveViewCheckpointStatePageRef sourceRef = sourceRefs.getQuick(i);
                        if (sourceRef == null || sourceRef.isNull()) {
                            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                    .put("live view checkpoint compaction source reference is null");
                        }
                        final long sourceSegmentId = sourceRef.getSegmentId();
                        if (sourceSegmentId == targetSegmentId) {
                            throw CairoException.critical(0)
                                    .put("live view checkpoint compaction target aliases a source segment, segmentId=")
                                    .put(targetSegmentId);
                        }
                        if (segmentDirectory.getReferenceCount(sourceSegmentId) <= 0) {
                            throw CairoException.critical(0)
                                    .put("cannot repack obsolete live view checkpoint segment, segmentId=")
                                    .put(sourceSegmentId);
                        }
                        own(candidate, sourceSegmentId);
                        final PhysicalPageKey key = new PhysicalPageKey(sourceRef);
                        final Redirect old = candidate.redirects.get(key);
                        if (old != null) {
                            if (!sameMetadata(old.sourceRef, sourceRef)) {
                                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                        .put("live view checkpoint shared data page metadata mismatch")
                                        .put(" [segmentId=").put(sourceSegmentId)
                                        .put(", offset=").put(sourceRef.getOffset()).put(']');
                            }
                            final LiveViewCheckpointStatePageRef copy = new LiveViewCheckpointStatePageRef();
                            copyRef(old.targetRef, copy);
                            candidate.stagedRefs.add(copy);
                            continue;
                        }

                        LiveViewCheckpointDataSegmentReader reader = readers.get(sourceSegmentId);
                        if (reader == null) {
                            reader = new LiveViewCheckpointDataSegmentReader(configuration);
                            reader.of(checkpointsDir, sourceSegmentId, segmentDirectory.getFileLength(sourceSegmentId));
                            readers.put(sourceSegmentId, reader);
                        }
                        reader.openPage(
                                sourceRef,
                                sourceRef.getPageKind(),
                                sourceRef.getCodec(),
                                sourceRef.getFlags(),
                                sourceRef.getRowCount(),
                                sourceRef.getDecodedLength()
                        );
                        final LiveViewCheckpointStatePageRef targetRef = new LiveViewCheckpointStatePageRef();
                        final MemoryA mem = writer.beginPage();
                        mem.putBlockOfBytes(reader.getPageAddress(), reader.getPageStoredLength());
                        writer.endPage(
                                targetRef,
                                sourceRef.getDecodedLength(),
                                sourceRef.getPageKind(),
                                sourceRef.getCodec(),
                                sourceRef.getRowCount(),
                                sourceRef.getFlags()
                        );
                        final LiveViewCheckpointStatePageRef sourceCopy = new LiveViewCheckpointStatePageRef();
                        copyRef(sourceRef, sourceCopy);
                        candidate.redirects.put(key, new Redirect(sourceCopy, targetRef));
                        final LiveViewCheckpointStatePageRef targetCopy = new LiveViewCheckpointStatePageRef();
                        copyRef(targetRef, targetCopy);
                        candidate.stagedRefs.add(targetCopy);
                    }
                    final long fileLength = writer.commit();
                    targetRefs.clear();
                    for (int i = 0, n = candidate.stagedRefs.size(); i < n; i++) {
                        final LiveViewCheckpointStatePageRef copy = new LiveViewCheckpointStatePageRef();
                        copyRef(candidate.stagedRefs.getQuick(i), copy);
                        targetRefs.add(copy);
                    }
                    return fileLength;
                } finally {
                    for (LiveViewCheckpointDataSegmentReader reader : readers.values()) {
                        Misc.free(reader);
                    }
                }
            }
        }
    }

    private synchronized void releaseCandidate(@NotNull Candidate candidate) {
        for (int i = 0, n = candidate.ownedSegmentIds.size(); i < n; i++) {
            final long segmentId = candidate.ownedSegmentIds.get(i);
            final int count = candidateOwnershipCounts.get(segmentId);
            assert count > 0;
            if (count == 1) {
                candidateOwnershipCounts.remove(segmentId);
            } else {
                candidateOwnershipCounts.put(segmentId, count - 1);
            }
        }
        candidate.ownedSegmentIds.clear();
        candidate.targetSegmentIds.clear();
    }

    private static boolean sameMetadata(
            @NotNull LiveViewCheckpointStatePageRef left,
            @NotNull LiveViewCheckpointStatePageRef right
    ) {
        return left.getSegmentId() == right.getSegmentId()
                && left.getOffset() == right.getOffset()
                && left.getStoredLength() == right.getStoredLength()
                && left.getDecodedLength() == right.getDecodedLength()
                && left.getPageKind() == right.getPageKind()
                && left.getCodec() == right.getCodec()
                && left.getRowCount() == right.getRowCount()
                && left.getFlags() == right.getFlags();
    }

    public static final class Candidate implements Closeable {

        private final LongHashSet ownedSegmentIds = new LongHashSet();
        private final HashMap<PhysicalPageKey, Redirect> redirects = new HashMap<>();
        private final ObjList<LiveViewCheckpointStatePageRef> stagedRefs = new ObjList<>();
        private final LongHashSet targetSegmentIds = new LongHashSet();
        private boolean failed;
        private LiveViewCheckpointDataStore owner;
        private boolean published;

        private Candidate(@NotNull LiveViewCheckpointDataStore owner) {
            this.owner = owner;
        }

        @Override
        public void close() {
            final LiveViewCheckpointDataStore o = owner;
            if (o == null) {
                return;
            }
            owner = null;
            if (published) {
                o.releaseCandidate(this);
            } else {
                o.abortCandidate(this);
            }
            redirects.clear();
            stagedRefs.clear();
        }

        /**
         * Called only after the metadata generation commit point.
         */
        public void markPublished() {
            ensureOpen();
            published = true;
        }

        public long repack(
                long targetSegmentId,
                @NotNull ObjList<LiveViewCheckpointStatePageRef> sourceRefs,
                @NotNull ObjList<LiveViewCheckpointStatePageRef> targetRefs
        ) {
            ensureOpen();
            try {
                return owner.repack(this, targetSegmentId, sourceRefs, targetRefs);
            } catch (RuntimeException | Error th) {
                failed = true;
                throw th;
            }
        }

        private void ensureOpen() {
            if (owner == null) {
                throw CairoException.critical(0).put("live view checkpoint compaction candidate is closed");
            }
            if (published) {
                throw CairoException.critical(0).put("live view checkpoint compaction candidate is already published");
            }
            if (failed) {
                throw CairoException.critical(0).put("live view checkpoint compaction candidate has failed");
            }
        }
    }

    public static final class PurgeResult {
        private final int failedSegmentCount;
        private final int liveSegmentCount;
        private final long obsoleteBytes;
        private final long purgedBytes;
        private final int purgedSegmentCount;

        private PurgeResult(
                int purgedSegmentCount,
                int failedSegmentCount,
                long purgedBytes,
                int liveSegmentCount,
                long obsoleteBytes
        ) {
            this.purgedSegmentCount = purgedSegmentCount;
            this.failedSegmentCount = failedSegmentCount;
            this.purgedBytes = purgedBytes;
            this.liveSegmentCount = liveSegmentCount;
            this.obsoleteBytes = obsoleteBytes;
        }

        public int getFailedSegmentCount() {
            return failedSegmentCount;
        }

        /**
         * @return data segments a current logical root still names, counted over
         * the same ordered walk the sweep makes
         */
        public int getLiveSegmentCount() {
            return liveSegmentCount;
        }

        /**
         * @return bytes held by retired segments this sweep could not unlink -
         * still protected by the fallback slot or a reader pin, or failed to
         * remove. The collection lag in bytes
         */
        public long getObsoleteBytes() {
            return obsoleteBytes;
        }

        public long getPurgedBytes() {
            return purgedBytes;
        }

        public int getPurgedSegmentCount() {
            return purgedSegmentCount;
        }
    }

    private static final class PhysicalPageKey {
        private final long offset;
        private final long segmentId;
        private final int storedLength;

        private PhysicalPageKey(@NotNull LiveViewCheckpointStatePageRef ref) {
            segmentId = ref.getSegmentId();
            offset = ref.getOffset();
            storedLength = ref.getStoredLength();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof PhysicalPageKey)) {
                return false;
            }
            final PhysicalPageKey that = (PhysicalPageKey) obj;
            return segmentId == that.segmentId && offset == that.offset && storedLength == that.storedLength;
        }

        @Override
        public int hashCode() {
            long hash = segmentId * 31 + offset;
            hash = hash * 31 + storedLength;
            return (int) (hash ^ (hash >>> 32));
        }
    }

    private static final class Redirect {
        private final LiveViewCheckpointStatePageRef sourceRef;
        private final LiveViewCheckpointStatePageRef targetRef;

        private Redirect(
                @NotNull LiveViewCheckpointStatePageRef sourceRef,
                @NotNull LiveViewCheckpointStatePageRef targetRef
        ) {
            this.sourceRef = sourceRef;
            this.targetRef = targetRef;
        }
    }

    /**
     * Reusable ordered sweep over the pinned generation's catalogue. A segment is
     * unlinked only when it is unreferenced, unowned by a compaction candidate,
     * unreachable from both valid superblock slots, and older than every live
     * reader pin. A failed unlink stays catalogued and is retried on the next
     * call.
     */
    private final class PurgeSweep implements LiveViewCheckpointSegmentDirectoryReader.Visitor {

        private int failedSegments;
        private int liveSegments;
        private long minPinnedGeneration;
        private long obsoleteBytes;
        private long oldestValidSlotGeneration;
        private long purgedBytes;
        private int purgedSegments;

        @Override
        public void onEntry(LiveViewCheckpointSegmentDirectoryEntry entry) {
            final long segmentId = entry.segmentId;
            if (entry.referenceCount != 0) {
                liveSegments++;
            }
            if (entry.referenceCount != 0 || isCandidateOwned(segmentId)) {
                return;
            }
            if (oldestValidSlotGeneration < entry.retireGeneration || minPinnedGeneration <= entry.retireGeneration) {
                // Retired but still protected by a slot or a reader, so its bytes
                // are garbage this sweep may not collect yet.
                obsoleteBytes = checkedAdd(obsoleteBytes, entry.fileLength);
                return;
            }
            try (Path path = new Path()) {
                LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, segmentId);
                if (!ff.exists(path.$())) {
                    return;
                }
                if (ff.removeQuiet(path.$())) {
                    purgedSegments++;
                    purgedBytes = checkedAdd(purgedBytes, entry.fileLength);
                } else {
                    failedSegments++;
                    obsoleteBytes = checkedAdd(obsoleteBytes, entry.fileLength);
                    LOG.error()
                            .$("could not purge live view checkpoint data segment [path=")
                            .$(path)
                            .$(',').$(" errno=").$(ff.errno()).I$();
                }
            }
        }

        private void of(long oldestValidSlotGeneration, long minPinnedGeneration) {
            this.oldestValidSlotGeneration = oldestValidSlotGeneration;
            this.minPinnedGeneration = minPinnedGeneration;
            purgedBytes = 0;
            purgedSegments = 0;
            failedSegments = 0;
            liveSegments = 0;
            obsoleteBytes = 0;
        }
    }
}
