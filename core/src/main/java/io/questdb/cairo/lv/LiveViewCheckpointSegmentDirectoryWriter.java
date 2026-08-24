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
import io.questdb.std.LongHashSet;
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;

/**
 * Copy-on-write publisher for the segment directory B+ tree. One publication is
 * a session: {@link #begin} pins the prior generation's root, the catalogue
 * mutations stage against it, and {@link #publish} writes only the pages those
 * mutations changed into one fresh metadata segment and returns the new tree
 * root. Every untouched subtree is carried forward by its existing page
 * reference, so a seal's directory cost follows the segments it added or
 * re-referenced rather than how many segments are live - which is what keeps
 * publication metadata flat as the timeline grows.
 * <p>
 * Staging is what makes that possible. A repair applies one reference
 * transaction per repaired boundary, and the same segment can be released by one
 * boundary and taken by the next; the session folds those into a single value
 * per segment and touches each affected leaf once. It also keeps the old
 * validation contract: a rejected transaction leaves the staged image, and
 * therefore the reusable directory, exactly as it was.
 * <p>
 * The catalogue holds metadata segments beside data ones, in two flavours. A
 * boundary-metadata segment counts roots and moves through
 * {@link #applyRootReferenceChanges} exactly as a data segment does. A
 * tree-metadata segment counts pages, and {@link #publish} folds in its own
 * accounting: the pages a path copy supersedes are released against the segments
 * holding them, so such a segment whose last reachable page goes retires like a
 * data segment whose last root does. The one thing it cannot do is register the
 * segment it is writing - the entry would have to name a file whose length the
 * tree carrying it does not yet know - so the caller carries that registration to
 * the next publication.
 * <p>
 * An entry leaves the catalogue only through {@link #removeSegment}, which the
 * purge sweep drives once it has unlinked the file the entry names. Reaching
 * zero retires a segment; unlinking its file retires its entry, one publication
 * later.
 * <p>
 * The instance is reusable across publications and is not thread safe.
 */
public class LiveViewCheckpointSegmentDirectoryWriter implements Closeable {

    private static final int CHILD_STRIDE = 4;
    private static final int STAGED_FILE_LENGTH = 1;
    private static final int STAGED_FLAGS = 4;
    private static final long STAGED_FLAG_INSERT = 1;
    private static final long STAGED_FLAG_NONE = 0;
    private static final long STAGED_FLAG_REMOVE = 2;
    private static final int STAGED_KIND = 5;
    private static final int STAGED_REFERENCE_COUNT = 2;
    private static final int STAGED_RETIRE_GENERATION = 3;
    private static final int STAGED_SEGMENT_ID = 0;
    private static final int STAGED_STRIDE = 6;
    private final LongHashSet addedSegmentIds = new LongHashSet();
    /**
     * Debug-only mirror of the segments {@link #applyRec} actually superseded, so
     * an assertion can hold it against what {@link #collectRec} predicted. Only an
     * enabled-assertions build ever fills it.
     */
    private final LongList appliedSegmentIds = new LongList();
    private final Path checkpointsDir = new Path();
    private final int internalCapacity;
    private final int leafCapacity;
    private final LiveViewCheckpointSegmentDirectoryEntry lookupEntry = new LiveViewCheckpointSegmentDirectoryEntry();
    private final LiveViewCheckpointPageRef oldRoot = new LiveViewCheckpointPageRef();
    private final LongList ownReleasedSegmentIds = new LongList();
    private final LiveViewCheckpointPageRef pageRef = new LiveViewCheckpointPageRef();
    // (segmentId, fileLength, retireGeneration, kind) for entries whose
    // staged reference count crossed from positive to zero. The publication
    // owner persists these before committing the superblock.
    private final LongList retirementTransitions = new LongList();
    private final LiveViewCheckpointSegmentDirectoryReader reader;
    private final LongList releaseTally = new LongList();
    private final LongHashSet removedSegmentIds = new LongHashSet();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;
    private final LongList sortedApplied = new LongList();
    private final LongList sortedReleased = new LongList();
    private final LongList staged = new LongList();
    private LiveViewCheckpointSegmentDirectoryNode[] builderPool = new LiveViewCheckpointSegmentDirectoryNode[0];
    private LiveViewCheckpointSegmentDirectoryNode[] collectPool = new LiveViewCheckpointSegmentDirectoryNode[0];
    private boolean isBegun;
    private long lastSegmentBytes;
    private int lastSegmentPageCount;
    private long liveDataSegmentDelta;
    private LiveViewCheckpointSegmentDirectoryNode[] nodePool = new LiveViewCheckpointSegmentDirectoryNode[0];
    private LongList[] outPool = new LongList[0];
    private LiveViewCheckpointSegmentDirectoryNode[] piecePool = new LiveViewCheckpointSegmentDirectoryNode[0];

    public LiveViewCheckpointSegmentDirectoryWriter(@NotNull CairoConfiguration configuration) {
        this(configuration, 64, 64);
    }

    public LiveViewCheckpointSegmentDirectoryWriter(
            @NotNull CairoConfiguration configuration,
            int leafCapacity,
            int internalCapacity
    ) {
        if (leafCapacity < 2 || internalCapacity < 2) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment directory node capacity must be at least 2, leaf=")
                    .put(leafCapacity).put(", internal=").put(internalCapacity);
        }
        this.leafCapacity = leafCapacity;
        this.internalCapacity = internalCapacity;
        this.reader = new LiveViewCheckpointSegmentDirectoryReader(configuration);
        this.segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    /**
     * Registers a newly published data segment with the number of logical roots
     * that reference it in the candidate generation.
     */
    public void addSegment(long segmentId, long fileLength, long referenceCount) {
        addSegment(segmentId, fileLength, referenceCount, LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA);
    }

    /**
     * Registers a newly published segment. {@code referenceCount} counts logical
     * roots for a {@link LiveViewCheckpointSegmentDirectory#SEGMENT_KIND_DATA} or
     * {@link LiveViewCheckpointSegmentDirectory#SEGMENT_KIND_BOUNDARY} segment and
     * reachable pages for a
     * {@link LiveViewCheckpointSegmentDirectory#SEGMENT_KIND_META} one.
     */
    public void addSegment(long segmentId, long fileLength, long referenceCount, long kind) {
        ensureBegun();
        if (segmentId < 0 || fileLength <= 0 || referenceCount <= 0) {
            throw CairoException.critical(0)
                    .put("invalid live view checkpoint segment directory entry")
                    .put(" [segmentId=").put(segmentId)
                    .put(", fileLength=").put(fileLength)
                    .put(", referenceCount=").put(referenceCount)
                    .put(']');
        }
        validateKind(kind);
        int index = stagedIndexOf(segmentId);
        if (index >= 0 || reader.find(segmentId, lookupEntry)) {
            throw CairoException.critical(0)
                    .put("duplicate live view checkpoint segment, segmentId=")
                    .put(segmentId);
        }
        index = -index - 1;
        insertStaged(
                index,
                segmentId,
                fileLength,
                referenceCount,
                LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE,
                STAGED_FLAG_INSERT,
                kind
        );
        if (kind == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA) {
            liveDataSegmentDelta++;
        }
    }

    /**
     * Applies one generation's root replacement. Repeated references to pages in
     * the same segment count once for each root side.
     */
    public void applyRootReferenceChanges(
            @NotNull LongList removedRootSegmentIds,
            @NotNull LongList addedRootSegmentIds,
            long generation
    ) {
        ensureBegun();
        if (generation < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint retire generation must be non-negative, was ")
                    .put(generation);
        }
        removedSegmentIds.clear();
        for (int i = 0, n = removedRootSegmentIds.size(); i < n; i++) {
            final long segmentId = removedRootSegmentIds.getQuick(i);
            validateReferenceSegmentId(segmentId);
            removedSegmentIds.add(segmentId);
        }
        addedSegmentIds.clear();
        for (int i = 0, n = addedRootSegmentIds.size(); i < n; i++) {
            final long segmentId = addedRootSegmentIds.getQuick(i);
            validateReferenceSegmentId(segmentId);
            addedSegmentIds.add(segmentId);
        }

        // Validate the complete transaction before mutating a count. A failed
        // candidate build must leave the reusable directory image untouched.
        for (int i = 0, n = removedSegmentIds.size(); i < n; i++) {
            final long segmentId = removedSegmentIds.get(i);
            final int index = stageExisting(segmentId, false);
            ensureNotRetiring(index, segmentId);
            ensureRootCounted(index, segmentId);
            if (staged.getQuick(index * STAGED_STRIDE + STAGED_REFERENCE_COUNT) <= 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint segment root reference count underflow, segmentId=")
                        .put(segmentId);
            }
        }
        for (int i = 0, n = addedSegmentIds.size(); i < n; i++) {
            final long segmentId = addedSegmentIds.get(i);
            final int index = stageExisting(segmentId, true);
            ensureNotRetiring(index, segmentId);
            ensureRootCounted(index, segmentId);
            long count = staged.getQuick(index * STAGED_STRIDE + STAGED_REFERENCE_COUNT);
            if (removedSegmentIds.contains(segmentId)) {
                count--;
            }
            if (count == Long.MAX_VALUE) {
                throw CairoException.critical(0)
                        .put("live view checkpoint segment root reference count overflow, segmentId=")
                        .put(segmentId);
            }
        }

        for (int i = 0, n = removedSegmentIds.size(); i < n; i++) {
            addReference(removedSegmentIds.get(i), -1, generation);
        }
        for (int i = 0, n = addedSegmentIds.size(); i < n; i++) {
            addReference(addedSegmentIds.get(i), 1, generation);
        }
    }

    /**
     * Applies sorted {@code (segmentId, netDelta)} pairs for one atomic batch of
     * root replacements. Zero deltas must have been removed by the accumulator.
     * The complete batch is validated before any staged reference count changes.
     */
    public void applyRootReferenceDeltas(@NotNull LongList deltas, long generation) {
        ensureBegun();
        if (generation < 0 || (deltas.size() & 1) != 0) {
            throw CairoException.critical(0).put("invalid live view checkpoint root reference delta batch");
        }
        long previousSegmentId = -1;
        for (int i = 0, n = deltas.size(); i < n; i += 2) {
            final long segmentId = deltas.getQuick(i);
            final long delta = deltas.getQuick(i + 1);
            validateReferenceSegmentId(segmentId);
            if (segmentId <= previousSegmentId || delta == 0) {
                throw CairoException.critical(0)
                        .put("invalid live view checkpoint root reference delta order, segmentId=")
                        .put(segmentId);
            }
            final int index = stageExisting(segmentId, delta > 0);
            ensureNotRetiring(index, segmentId);
            ensureRootCounted(index, segmentId);
            final long count = staged.getQuick(index * STAGED_STRIDE + STAGED_REFERENCE_COUNT);
            final long updated;
            try {
                updated = Math.addExact(count, delta);
            } catch (ArithmeticException e) {
                throw CairoException.critical(0)
                        .put("live view checkpoint segment root reference count overflow, segmentId=")
                        .put(segmentId);
            }
            if (updated < 0) {
                throw CairoException.critical(0)
                        .put("live view checkpoint segment root reference count underflow, segmentId=")
                        .put(segmentId);
            }
            previousSegmentId = segmentId;
        }
        for (int i = 0, n = deltas.size(); i < n; i += 2) {
            addReference(deltas.getQuick(i), deltas.getQuick(i + 1), generation);
        }
    }

    /**
     * Starts a publication against the generation rooted at {@code oldRoot}
     * (null for a fresh catalogue), discarding any staged mutations.
     */
    public void begin(@NotNull LiveViewCheckpointPageRef oldRoot) {
        staged.clear();
        retirementTransitions.clear();
        liveDataSegmentDelta = 0;
        releaseTally.clear();
        ownReleasedSegmentIds.clear();
        this.oldRoot.of(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength());
        reader.of(checkpointsDir, oldRoot);
        lastSegmentBytes = 0;
        lastSegmentPageCount = 0;
        isBegun = true;
    }

    @Override
    public void close() {
        Misc.free(reader);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
        staged.clear();
        releaseTally.clear();
        ownReleasedSegmentIds.clear();
        isBegun = false;
    }

    /**
     * Drops every staged mutation, releases the reader's mappings and discards any
     * in-flight segment, while keeping every shell for the next publication. A
     * publication that failed part-way leaves nothing staged behind it.
     */
    public void detach() {
        reader.detach();
        segmentWriter.discard();
        appliedSegmentIds.clear();
        ownReleasedSegmentIds.clear();
        releaseTally.clear();
        retirementTransitions.clear();
        staged.clear();
        liveDataSegmentDelta = 0;
        isBegun = false;
    }

    /**
     * @return byte size of the metadata segment the last publication wrote, or
     * {@code 0} when it staged no change and reused the prior root
     */
    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    public long getLiveDataSegmentDelta() {
        return liveDataSegmentDelta;
    }

    /**
     * @return number of new metadata pages the last publication wrote (its
     * copy-on-write cost); far below the catalogue's node count, which is the
     * point of the tree
     */
    public int getLastSegmentPageCount() {
        return lastSegmentPageCount;
    }

    /**
     * Reference count of {@code segmentId} in the candidate generation, staged
     * mutations included.
     */
    public long getReferenceCount(long segmentId) {
        final int index = stagedIndexOf(segmentId);
        return index >= 0
                ? staged.getQuick(index * STAGED_STRIDE + STAGED_REFERENCE_COUNT)
                : required(segmentId).referenceCount;
    }

    /**
     * Retire generation of {@code segmentId} in the candidate generation, staged
     * mutations included.
     */
    public long getRetireGeneration(long segmentId) {
        final int index = stagedIndexOf(segmentId);
        return index >= 0
                ? staged.getQuick(index * STAGED_STRIDE + STAGED_RETIRE_GENERATION)
                : required(segmentId).retireGeneration;
    }

    /**
     * Returns the zero-reference transitions staged by this publication as
     * {@code (segmentId, fileLength, retireGeneration, kind)} records. A later
     * transition back to a positive count removes the record, so only the
     * publication's net new retirements remain.
     */
    public LongList getRetirementTransitions() {
        return retirementTransitions;
    }

    /**
     * Points the writer at a live view's {@code _checkpoints} directory.
     */
    public void of(@Transient @NotNull Path checkpointsDir) {
        this.checkpointsDir.of(checkpointsDir);
        isBegun = false;
    }

    /**
     * Writes the staged mutations as new pages of metadata segment
     * {@code newSegmentId}, which must be unused, and fills {@code newRootOut}
     * with the new tree root. A publication that staged nothing writes no segment
     * and reuses {@code oldRoot}.
     * <p>
     * The publication also releases its own superseded pages: every catalogue
     * page the path copy replaces is one fewer reachable page of the metadata
     * segment holding it, and a segment whose last page goes retires at
     * {@code generation}. The new segment cannot register itself here - the tree
     * that would hold the entry is the one being written, and its final length is
     * not known until it is - so the caller carries
     * {@code (newSegmentId, getLastSegmentBytes(), getLastSegmentPageCount())}
     * forward and registers it at the next publication.
     */
    public void publish(long newSegmentId, long generation, @NotNull LiveViewCheckpointPageRef newRootOut) {
        ensureBegun();
        releaseOwnPages(generation);
        isBegun = false;
        final int stagedCount = staged.size() / STAGED_STRIDE;
        if (stagedCount == 0) {
            newRootOut.of(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength());
            lastSegmentBytes = 0;
            lastSegmentPageCount = 0;
            return;
        }
        segmentWriter.of(checkpointsDir, newSegmentId);
        lastSegmentPageCount = 0;
        appliedSegmentIds.clear();
        if (oldRoot.isNull()) {
            buildFreshLeaf(stagedCount);
        } else {
            applyRec(oldRoot.getSegmentId(), oldRoot.getOffset(), oldRoot.getLength(), 0, stagedCount, 0);
        }
        // The releases were staged from a separate descent, so the two have to
        // agree page for page or a superseded page keeps its reference forever.
        assert isReleaseSetComplete()
                : "the release pre-pass and the path copy visited different catalogue pages";

        LongList level = outAt(0);
        if (level.size() == 0) {
            // Every catalogued entry retired, so the root emitted no page and the
            // publication wrote none at all. The catalogue goes back to the empty
            // shape begin() already accepts.
            segmentWriter.discard();
            newRootOut.clear();
            lastSegmentBytes = 0;
            lastSegmentPageCount = 0;
            return;
        }

        // Collapse the emitted level into one root, adding a level whenever the
        // pieces a split produced no longer fit one node.
        int depth = 0;
        while (level.size() > CHILD_STRIDE) {
            final LiveViewCheckpointSegmentDirectoryNode parent = builderAt(depth);
            parent.resetInternal();
            for (int i = 0, n = level.size(); i < n; i += CHILD_STRIDE) {
                parent.appendChild(
                        level.getQuick(i),
                        level.getQuick(i + 1),
                        level.getQuick(i + 2),
                        level.getQuick(i + 3)
                );
            }
            final LongList next = outAt(depth + 1);
            next.clear();
            emitNodes(parent, pieceAt(depth), next, internalCapacity);
            level = next;
            depth++;
        }
        newRootOut.of(level.getQuick(1), level.getQuick(2), (int) level.getQuick(3));
        lastSegmentBytes = segmentWriter.commit();
    }

    /**
     * Releases one reachable page per element of {@code metaSegmentIds}, which is
     * a multiset: a caller lists a segment once for every page of its own the
     * publication replaced. A segment whose count reaches zero retires at
     * {@code generation}.
     * <p>
     * A segment the catalogue does not list is skipped rather than refused. The
     * catalogue is what the purge sweep walks, so an uncatalogued file is one it
     * can never unlink and one no reference count decides the fate of: there is
     * nothing for a release to move. That is what lets a caller publish a
     * directory tree without registering the segments carrying it, which the
     * catalogue's own unit tests do.
     */
    public void releaseMetadataPages(@NotNull LongList metaSegmentIds, long generation) {
        ensureBegun();
        if (generation < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint retire generation must be non-negative, was ")
                    .put(generation);
        }
        if (metaSegmentIds.size() == 0) {
            return;
        }
        // Tally the whole batch and validate it before mutating a count, so a
        // rejected release leaves the staged image - and therefore the reusable
        // directory - exactly as it was.
        releaseTally.clear();
        for (int i = 0, n = metaSegmentIds.size(); i < n; i++) {
            final long segmentId = metaSegmentIds.getQuick(i);
            validateReferenceSegmentId(segmentId);
            final int index = stageCatalogued(segmentId);
            if (index < 0) {
                continue;
            }
            ensureNotRetiring(index, segmentId);
            if (staged.getQuick(index * STAGED_STRIDE + STAGED_KIND) != LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META) {
                throw CairoException.critical(0)
                        .put("cannot release a page of a root-counted live view checkpoint segment, segmentId=")
                        .put(segmentId);
            }
            tallyRelease(segmentId);
        }
        for (int i = 0, n = releaseTally.size(); i < n; i += 2) {
            final long segmentId = releaseTally.getQuick(i);
            final long releases = releaseTally.getQuick(i + 1);
            final long count = staged.getQuick(stagedIndexOf(segmentId) * STAGED_STRIDE + STAGED_REFERENCE_COUNT);
            if (count < releases) {
                throw CairoException.critical(0)
                        .put("live view checkpoint metadata segment page count underflow")
                        .put(" [segmentId=").put(segmentId)
                        .put(", count=").put(count)
                        .put(", released=").put(releases)
                        .put(']');
            }
        }
        for (int i = 0, n = releaseTally.size(); i < n; i += 2) {
            addReference(releaseTally.getQuick(i), -releaseTally.getQuick(i + 1), generation);
        }
    }

    /**
     * Retires the catalogue entry of {@code segmentId}, whose file the purge
     * sweep has already unlinked. Nothing else removes an entry, so without this
     * the catalogue holds one per segment ever written and its own tree gains a
     * leaf every {@code leafCapacity} of them - the last term that grows with a
     * view's age rather than with what it currently holds.
     * <p>
     * An entry the catalogue does not hold is skipped rather than refused: the
     * sweep re-proposes every retired entry whose file is gone, so a proposal
     * that a publication has already applied comes back once more and must be a
     * no-op. A still-referenced entry is refused, because its file cannot have
     * been unlinked and removing it would strand one.
     */
    public void removeSegment(long segmentId) {
        ensureBegun();
        validateReferenceSegmentId(segmentId);
        final int index = stageCatalogued(segmentId);
        if (index < 0) {
            return;
        }
        final int base = index * STAGED_STRIDE;
        if (staged.getQuick(base + STAGED_FLAGS) == STAGED_FLAG_INSERT) {
            throw CairoException.critical(0)
                    .put("cannot retire the entry of a live view checkpoint segment this publication registers, segmentId=")
                    .put(segmentId);
        }
        final long referenceCount = staged.getQuick(base + STAGED_REFERENCE_COUNT);
        if (referenceCount != 0) {
            throw CairoException.critical(0)
                    .put("cannot retire the entry of a referenced live view checkpoint segment")
                    .put(" [segmentId=").put(segmentId)
                    .put(", referenceCount=").put(referenceCount)
                    .put(']');
        }
        staged.setQuick(base + STAGED_FLAGS, STAGED_FLAG_REMOVE);
    }

    private static LiveViewCheckpointSegmentDirectoryNode[] growNodes(
            LiveViewCheckpointSegmentDirectoryNode[] src,
            int size
    ) {
        if (src.length >= size) {
            return src;
        }
        final LiveViewCheckpointSegmentDirectoryNode[] dst = new LiveViewCheckpointSegmentDirectoryNode[size];
        System.arraycopy(src, 0, dst, 0, src.length);
        return dst;
    }

    private static void validateKind(long kind) {
        if (kind != LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA
                && kind != LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META
                && kind != LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_BOUNDARY) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment kind unknown, kind=").put(kind);
        }
    }

    private static void validateReferenceSegmentId(long segmentId) {
        if (segmentId < 0) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment reference id must be non-negative, was ")
                    .put(segmentId);
        }
    }

    private void addReference(long segmentId, long delta, long generation) {
        final int base = stagedIndexOf(segmentId) * STAGED_STRIDE;
        final long oldCount = staged.getQuick(base + STAGED_REFERENCE_COUNT);
        final long count = oldCount + delta;
        staged.setQuick(base + STAGED_REFERENCE_COUNT, count);
        staged.setQuick(
                base + STAGED_RETIRE_GENERATION,
                count == 0 ? generation : LiveViewCheckpointSegmentDirectory.RETIRE_GENERATION_NONE
        );
        if (oldCount > 0 && count == 0) {
            recordRetirementTransition(
                    segmentId,
                    staged.getQuick(base + STAGED_FILE_LENGTH),
                    generation,
                    staged.getQuick(base + STAGED_KIND)
            );
            if (staged.getQuick(base + STAGED_KIND) == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA) {
                liveDataSegmentDelta--;
            }
        } else if (oldCount == 0 && count > 0) {
            removeRetirementTransition(segmentId);
            if (staged.getQuick(base + STAGED_KIND) == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_DATA) {
                liveDataSegmentDelta++;
            }
        }
    }

    private void recordRetirementTransition(long segmentId, long fileLength, long generation, long kind) {
        removeRetirementTransition(segmentId);
        retirementTransitions.add(segmentId, fileLength, generation, kind);
    }

    private void removeRetirementTransition(long segmentId) {
        for (int i = 0, n = retirementTransitions.size(); i < n; i += 4) {
            if (retirementTransitions.getQuick(i) == segmentId) {
                for (int r = 0; r < 4; r++) {
                    retirementTransitions.removeIndex(i);
                }
                return;
            }
        }
    }

    /**
     * Rewrites the subtree at {@code (seg, off, len)} against staged records
     * {@code [lo, hi)}, emitting the pages that replace it into
     * {@code outAt(depth)} as {@code (minKey, segmentId, offset, length)}
     * quadruples. A subtree no staged record falls into is never read and keeps
     * its existing page reference.
     */
    private void applyRec(long seg, long off, long len, int lo, int hi, int depth) {
        final LiveViewCheckpointSegmentDirectoryNode node = nodeAt(depth);
        reader.openAndDecode(seg, off, len, node);
        assert recordApplied(seg);
        final LongList out = outAt(depth);
        out.clear();
        if (node.isLeaf()) {
            for (int r = lo; r < hi; r++) {
                final int base = r * STAGED_STRIDE;
                final long segmentId = staged.getQuick(base + STAGED_SEGMENT_ID);
                final int pos = node.findEntry(segmentId);
                if (pos >= 0) {
                    if (staged.getQuick(base + STAGED_FLAGS) == STAGED_FLAG_REMOVE) {
                        node.removeEntryAt(pos);
                    } else {
                        node.replaceEntryPayloadAt(
                                pos,
                                staged.getQuick(base + STAGED_FILE_LENGTH),
                                staged.getQuick(base + STAGED_REFERENCE_COUNT),
                                staged.getQuick(base + STAGED_RETIRE_GENERATION)
                        );
                    }
                } else {
                    if (staged.getQuick(base + STAGED_FLAGS) != STAGED_FLAG_INSERT) {
                        // The record was staged from a lookup that found the
                        // segment, so descending to a leaf without it means the
                        // tree disagrees with itself.
                        throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                                .put("live view checkpoint segment directory lost a catalogued segment, segmentId=")
                                .put(segmentId);
                    }
                    node.insertEntryAt(
                            node.leafInsertPosition(segmentId),
                            segmentId,
                            staged.getQuick(base + STAGED_FILE_LENGTH),
                            staged.getQuick(base + STAGED_REFERENCE_COUNT),
                            staged.getQuick(base + STAGED_RETIRE_GENERATION),
                            staged.getQuick(base + STAGED_KIND)
                    );
                }
            }
            emitNodes(node, pieceAt(depth), out, leafCapacity);
            return;
        }
        final int count = node.count();
        if (count == 0) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint segment directory node is empty");
        }
        final LiveViewCheckpointSegmentDirectoryNode parent = builderAt(depth);
        parent.resetInternal();
        int r = lo;
        for (int ci = 0; ci < count; ci++) {
            final int subLo = r;
            if (ci + 1 < count) {
                final long nextMinSegmentId = node.childMinSegmentId[ci + 1];
                while (r < hi && staged.getQuick(r * STAGED_STRIDE + STAGED_SEGMENT_ID) < nextMinSegmentId) {
                    r++;
                }
            } else {
                r = hi;
            }
            if (subLo == r) {
                parent.appendChild(
                        node.childMinSegmentId[ci],
                        node.childSegmentId[ci],
                        node.childOffset[ci],
                        node.childLength[ci]
                );
                continue;
            }
            applyRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], subLo, r, depth + 1);
            final LongList childOut = outAt(depth + 1);
            for (int i = 0, n = childOut.size(); i < n; i += CHILD_STRIDE) {
                parent.appendChild(
                        childOut.getQuick(i),
                        childOut.getQuick(i + 1),
                        childOut.getQuick(i + 2),
                        childOut.getQuick(i + 3)
                );
            }
        }
        emitNodes(parent, pieceAt(depth), out, internalCapacity);
    }

    private void buildFreshLeaf(int stagedCount) {
        final LiveViewCheckpointSegmentDirectoryNode leaf = nodeAt(0);
        leaf.resetLeaf();
        for (int r = 0; r < stagedCount; r++) {
            final int base = r * STAGED_STRIDE;
            if (staged.getQuick(base + STAGED_FLAGS) != STAGED_FLAG_INSERT) {
                throw CairoException.critical(0)
                        .put("cannot re-reference a segment of an empty live view checkpoint directory, segmentId=")
                        .put(staged.getQuick(base + STAGED_SEGMENT_ID));
            }
            leaf.insertEntryAt(
                    leaf.count(),
                    staged.getQuick(base + STAGED_SEGMENT_ID),
                    staged.getQuick(base + STAGED_FILE_LENGTH),
                    staged.getQuick(base + STAGED_REFERENCE_COUNT),
                    staged.getQuick(base + STAGED_RETIRE_GENERATION),
                    staged.getQuick(base + STAGED_KIND)
            );
        }
        final LongList out = outAt(0);
        out.clear();
        emitNodes(leaf, pieceAt(0), out, leafCapacity);
    }

    private LiveViewCheckpointSegmentDirectoryNode builderAt(int depth) {
        builderPool = growNodes(builderPool, depth + 1);
        LiveViewCheckpointSegmentDirectoryNode node = builderPool[depth];
        if (node == null) {
            node = new LiveViewCheckpointSegmentDirectoryNode();
            builderPool[depth] = node;
        }
        return node;
    }

    private LiveViewCheckpointSegmentDirectoryNode collectAt(int depth) {
        collectPool = growNodes(collectPool, depth + 1);
        LiveViewCheckpointSegmentDirectoryNode node = collectPool[depth];
        if (node == null) {
            node = new LiveViewCheckpointSegmentDirectoryNode();
            collectPool[depth] = node;
        }
        return node;
    }

    /**
     * Walks the same search paths {@link #applyRec} will, without writing
     * anything, appending the segment of every page it visits to
     * {@link #ownReleasedSegmentIds}. {@code applyRec} emits a replacement for
     * every node it decodes and reuses every subtree it does not descend into, so
     * "visited" and "superseded" are the same set.
     */
    private void collectRec(long seg, long off, long len, int lo, int hi, int depth) {
        final LiveViewCheckpointSegmentDirectoryNode node = collectAt(depth);
        reader.openAndDecode(seg, off, len, node);
        ownReleasedSegmentIds.add(seg);
        if (node.isLeaf()) {
            return;
        }
        final int count = node.count();
        if (count == 0) {
            throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                    .put("live view checkpoint segment directory node is empty");
        }
        int r = lo;
        for (int ci = 0; ci < count; ci++) {
            final int subLo = r;
            if (ci + 1 < count) {
                final long nextMinSegmentId = node.childMinSegmentId[ci + 1];
                while (r < hi && staged.getQuick(r * STAGED_STRIDE + STAGED_SEGMENT_ID) < nextMinSegmentId) {
                    r++;
                }
            } else {
                r = hi;
            }
            if (subLo != r) {
                collectRec(node.childSegmentId[ci], node.childOffset[ci], node.childLength[ci], subLo, r, depth + 1);
            }
        }
    }

    /**
     * Writes {@code node} as one page when it fits {@code capacity}, otherwise
     * breaks it into the fewest equal pieces that do, appending one
     * {@code (minKey, ref)} quadruple per written page to {@code out}.
     * <p>
     * A node whose every record retired writes no page and appends nothing, so
     * its parent simply keeps no child reference to it - and a parent that loses
     * every child empties in turn, which is how a retirement prunes a whole
     * branch without a separate rebalancing pass.
     */
    private void emitNodes(
            LiveViewCheckpointSegmentDirectoryNode node,
            LiveViewCheckpointSegmentDirectoryNode piece,
            LongList out,
            int capacity
    ) {
        final int count = node.count();
        if (count == 0) {
            return;
        }
        if (count <= capacity) {
            writePage(node, out);
            return;
        }
        final int parts = (count + capacity - 1) / capacity;
        final int perPart = (count + parts - 1) / parts;
        for (int start = 0; start < count; start += perPart) {
            node.copyRangeInto(piece, start, Math.min(count, start + perPart));
            writePage(piece, out);
        }
    }

    private void ensureBegun() {
        if (!isBegun) {
            throw CairoException.critical(0)
                    .put("live view checkpoint segment directory publication has not begun");
        }
    }

    /**
     * Refuses any reference movement against an entry this publication retires.
     * The sweep unlinked that segment's file before proposing the retirement, so
     * a root or a page that still reaches it means the count the sweep acted on
     * and the closure a root publishes disagree.
     */
    private void ensureNotRetiring(int stagedIndex, long segmentId) {
        if (staged.getQuick(stagedIndex * STAGED_STRIDE + STAGED_FLAGS) == STAGED_FLAG_REMOVE) {
            throw CairoException.critical(0)
                    .put("cannot reference a live view checkpoint segment this publication retires, segmentId=")
                    .put(segmentId);
        }
    }

    /**
     * Refuses a root reference on a tree-metadata segment. Data and
     * boundary-metadata segments both count roots, so both belong here; the three
     * superblock-rooted trees count pages instead and move only through
     * {@link #releaseMetadataPages}, so counting a root against one of them would
     * mix two units in the same field.
     */
    private void ensureRootCounted(int stagedIndex, long segmentId) {
        if (staged.getQuick(stagedIndex * STAGED_STRIDE + STAGED_KIND) == LiveViewCheckpointSegmentDirectory.SEGMENT_KIND_META) {
            throw CairoException.critical(0)
                    .put("cannot take a root reference on a live view checkpoint tree metadata segment, segmentId=")
                    .put(segmentId);
        }
    }

    private void insertStaged(
            int index,
            long segmentId,
            long fileLength,
            long referenceCount,
            long retireGeneration,
            long flags,
            long kind
    ) {
        final int base = index * STAGED_STRIDE;
        staged.insert(base, STAGED_STRIDE);
        staged.setQuick(base + STAGED_SEGMENT_ID, segmentId);
        staged.setQuick(base + STAGED_FILE_LENGTH, fileLength);
        staged.setQuick(base + STAGED_REFERENCE_COUNT, referenceCount);
        staged.setQuick(base + STAGED_RETIRE_GENERATION, retireGeneration);
        staged.setQuick(base + STAGED_FLAGS, flags);
        staged.setQuick(base + STAGED_KIND, kind);
    }

    /**
     * Compares the two id sets order-independently. Both scratch lists are retained
     * because every publication runs this check, and copying the sets into fresh
     * lists would charge a seal for the comparison alone.
     */
    private boolean isReleaseSetComplete() {
        if (appliedSegmentIds.size() != ownReleasedSegmentIds.size()) {
            return false;
        }
        sortedApplied.clear();
        sortedApplied.add(appliedSegmentIds);
        sortedReleased.clear();
        sortedReleased.add(ownReleasedSegmentIds);
        sortedApplied.sort();
        sortedReleased.sort();
        return sortedApplied.equals(sortedReleased);
    }

    private LiveViewCheckpointSegmentDirectoryNode nodeAt(int depth) {
        nodePool = growNodes(nodePool, depth + 1);
        LiveViewCheckpointSegmentDirectoryNode node = nodePool[depth];
        if (node == null) {
            node = new LiveViewCheckpointSegmentDirectoryNode();
            nodePool[depth] = node;
        }
        return node;
    }

    private LongList outAt(int depth) {
        if (depth >= outPool.length) {
            final LongList[] grown = new LongList[depth + 1];
            System.arraycopy(outPool, 0, grown, 0, outPool.length);
            outPool = grown;
        }
        LongList list = outPool[depth];
        if (list == null) {
            list = new LongList();
            outPool[depth] = list;
        }
        return list;
    }

    private LiveViewCheckpointSegmentDirectoryNode pieceAt(int depth) {
        piecePool = growNodes(piecePool, depth + 1);
        LiveViewCheckpointSegmentDirectoryNode node = piecePool[depth];
        if (node == null) {
            node = new LiveViewCheckpointSegmentDirectoryNode();
            piecePool[depth] = node;
        }
        return node;
    }

    private boolean recordApplied(long segmentId) {
        appliedSegmentIds.add(segmentId);
        return true;
    }

    /**
     * Stages the release of every catalogue page this publication is about to
     * supersede.
     * <p>
     * Which pages those are depends on which keys are staged, and staging a
     * release adds the released segment's own id as a key - so the touched-key set
     * is a closure rather than a snapshot. The loop grows the staged set until it
     * stops growing, which terminates because the set only ever gains keys and is
     * bounded by the catalogue. In practice it settles on the second pass: a seal
     * stages ids the id counter has just minted, and the entries a release names
     * sit in the same rightmost leaves the new ids already path-copy.
     */
    private void releaseOwnPages(long generation) {
        if (oldRoot.isNull() || staged.size() == 0) {
            return;
        }
        while (true) {
            ownReleasedSegmentIds.clear();
            collectRec(
                    oldRoot.getSegmentId(),
                    oldRoot.getOffset(),
                    oldRoot.getLength(),
                    0,
                    staged.size() / STAGED_STRIDE,
                    0
            );
            boolean isConverged = true;
            for (int i = 0, n = ownReleasedSegmentIds.size(); i < n; i++) {
                final long segmentId = ownReleasedSegmentIds.getQuick(i);
                if (stagedIndexOf(segmentId) < 0 && stageCatalogued(segmentId) >= 0) {
                    isConverged = false;
                }
            }
            if (isConverged) {
                break;
            }
        }
        releaseMetadataPages(ownReleasedSegmentIds, generation);
    }

    private LiveViewCheckpointSegmentDirectoryEntry required(long segmentId) {
        if (!reader.find(segmentId, lookupEntry)) {
            throw CairoException.critical(0)
                    .put("unknown live view checkpoint segment, segmentId=")
                    .put(segmentId);
        }
        return lookupEntry;
    }

    /**
     * {@link #stageExisting} for a segment the catalogue may not hold, returning
     * {@code -1} rather than raising when it does not.
     */
    private int stageCatalogued(long segmentId) {
        final int index = stagedIndexOf(segmentId);
        if (index >= 0) {
            return index;
        }
        if (!reader.find(segmentId, lookupEntry)) {
            return -1;
        }
        final int insertAt = -index - 1;
        insertStaged(
                insertAt,
                lookupEntry.segmentId,
                lookupEntry.fileLength,
                lookupEntry.referenceCount,
                lookupEntry.retireGeneration,
                STAGED_FLAG_NONE,
                lookupEntry.kind
        );
        return insertAt;
    }

    /**
     * Ensures the already-catalogued {@code segmentId} has a staged record,
     * loading its current values from the pinned tree on first touch. Staging
     * alone changes nothing: the record starts as a copy of what the tree holds.
     */
    private int stageExisting(long segmentId, boolean isAdd) {
        final int index = stageCatalogued(segmentId);
        if (index < 0) {
            throw CairoException.critical(0)
                    .put("cannot ").put(isAdd ? "add" : "remove")
                    .put(" reference to unknown live view checkpoint segment, segmentId=")
                    .put(segmentId);
        }
        return index;
    }

    /**
     * Returns the staged record index for {@code segmentId}, or
     * {@code -insertionPoint - 1}. Records are kept sorted by id so a publication
     * can walk them alongside the tree in one pass.
     */
    private int stagedIndexOf(long segmentId) {
        int lo = 0;
        int hi = staged.size() / STAGED_STRIDE - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final long value = staged.getQuick(mid * STAGED_STRIDE + STAGED_SEGMENT_ID);
            if (value < segmentId) {
                lo = mid + 1;
            } else if (value > segmentId) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -lo - 1;
    }

    /**
     * Folds one page release for {@code segmentId} into the sorted
     * {@code (segmentId, releases)} tally.
     */
    private void tallyRelease(long segmentId) {
        int lo = 0;
        int hi = releaseTally.size() / 2;
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (releaseTally.getQuick(mid * 2) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < releaseTally.size() / 2 && releaseTally.getQuick(lo * 2) == segmentId) {
            releaseTally.setQuick(lo * 2 + 1, releaseTally.getQuick(lo * 2 + 1) + 1);
        } else {
            releaseTally.add(lo * 2, segmentId);
            releaseTally.add(lo * 2 + 1, 1);
        }
    }

    private void writePage(LiveViewCheckpointSegmentDirectoryNode node, LongList out) {
        node.writeTo(segmentWriter, pageRef);
        lastSegmentPageCount++;
        out.add(node.minKey(), pageRef.getSegmentId(), pageRef.getOffset(), pageRef.getLength());
    }
}
