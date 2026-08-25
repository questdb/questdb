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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.std.LongIntHashMap;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

/**
 * Heap-stable owner for one refresh worker's compaction metadata. Physical page
 * keys and reference columns live in tracker-bound native maps for one call;
 * {@link #end()} frees that capacity before the caller releases or reuses the
 * view tracker. Reader Java shells remain pooled, but every mapping closes at
 * call end and the primitive lookup map avoids boxed segment ids.
 */
public final class LiveViewCheckpointCompactionScratch implements Closeable {

    private static final int PAGE_KEY_OFFSET = 14;
    private static final int PAGE_KEY_SEGMENT_ID = 13;
    private static final ArrayColumnTypes PAGE_KEY_TYPES = new ArrayColumnTypes()
            .add(ColumnType.LONG)
            .add(ColumnType.LONG)
            .add(ColumnType.INT);
    private static final int PAGE_SOURCE_CODEC = 2;
    private static final int PAGE_SOURCE_DECODED_LENGTH = 0;
    private static final int PAGE_SOURCE_FLAGS = 4;
    private static final int PAGE_SOURCE_KIND = 1;
    private static final int PAGE_SOURCE_ROW_COUNT = 3;
    private static final int PAGE_TARGET_CODEC = 10;
    private static final int PAGE_TARGET_DECODED_LENGTH = 8;
    private static final int PAGE_TARGET_FLAGS = 12;
    private static final int PAGE_TARGET_KIND = 9;
    private static final int PAGE_TARGET_OFFSET = 6;
    private static final int PAGE_TARGET_ROW_COUNT = 11;
    private static final int PAGE_TARGET_SEGMENT_ID = 5;
    private static final int PAGE_TARGET_STORED_LENGTH = 7;
    private static final ArrayColumnTypes PAGE_VALUE_TYPES = new ArrayColumnTypes()
            .add(ColumnType.INT)
            .add(ColumnType.INT)
            .add(ColumnType.INT)
            .add(ColumnType.INT)
            .add(ColumnType.INT)
            .add(ColumnType.LONG)
            .add(ColumnType.LONG)
            .add(ColumnType.INT)
            .add(ColumnType.INT)
            .add(ColumnType.INT)
            .add(ColumnType.INT)
            .add(ColumnType.INT)
            .add(ColumnType.INT);
    private static final ArrayColumnTypes SEGMENT_KEY_TYPES = new ArrayColumnTypes().add(ColumnType.LONG);
    private static final ArrayColumnTypes SEGMENT_VALUE_TYPES = new ArrayColumnTypes().add(ColumnType.LONG);

    private final LiveViewCheckpointDataStore.Candidate candidate;
    private final LiveViewCheckpointRoot checkpointRoot;
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointDataStore dataStore;
    private final LiveViewCheckpointFunctionDirectory functionDirectory;
    private final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointFunctionRoot functionRoot;
    private final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
    private final Map liveBytesBySegment;
    private final LivePagePartitionVisitor livePagePartitionVisitor = new LivePagePartitionVisitor();
    private final LivePageTimelineVisitor livePageTimelineVisitor = new LivePageTimelineVisitor();
    private final LiveViewCheckpointMetaStore metaStore;
    private final Map pages;
    private final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
    private final LiveViewCheckpointPartitionMapReader partitionReader;
    private final Path pathScratch;
    private final LiveViewCheckpointCompactionPlan plan = new LiveViewCheckpointCompactionPlan(this);
    private final LongIntHashMap readerIndexBySegment = new LongIntHashMap();
    private final ObjList<LiveViewCheckpointDataSegmentReader> readerPool = new ObjList<>();
    private final LiveViewCheckpointCompaction.Result result = new LiveViewCheckpointCompaction.Result();
    private final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
    private final LiveViewCheckpointSegmentDirectoryReader segmentDirectory;
    private final Map selectedSegments;
    private final SelectSegmentVisitor selectSegmentVisitor = new SelectSegmentVisitor();
    private final LiveViewCheckpointTimelineReader timelineReader;
    private MapRecord currentPage;
    private MapRecordCursor pageCursor;
    private int readerCount;
    private int lastLivePageCount;
    private int lastLiveSegmentCount;
    private int lastSelectedSegmentCount;
    private int lastTargetPageCount;
    private int targetPageCount;
    private int testFailAfterReaderOpenCount;
    private int testFailAfterRepackedPageCount;
    private boolean isActive;

    public LiveViewCheckpointCompactionScratch(@NotNull CairoConfiguration configuration) {
        this(configuration, true);
    }

    LiveViewCheckpointCompactionScratch(
            @NotNull CairoConfiguration configuration,
            boolean ownsCandidate
    ) {
        this.configuration = configuration;
        // Only the worker's own scratch drives a compaction pass. The compatibility
        // scratch a repack candidate nests inside itself never opens a catalogue of
        // its own, so it builds none of the driver's stores or readers.
        this.candidate = ownsCandidate ? new LiveViewCheckpointDataStore.Candidate(configuration) : null;
        this.checkpointRoot = ownsCandidate ? new LiveViewCheckpointRoot(configuration) : null;
        this.functionDirectory = ownsCandidate ? new LiveViewCheckpointFunctionDirectory(configuration) : null;
        this.functionRoot = ownsCandidate ? new LiveViewCheckpointFunctionRoot(configuration) : null;
        this.metaStore = ownsCandidate ? new LiveViewCheckpointMetaStore(configuration) : null;
        this.partitionReader = ownsCandidate ? new LiveViewCheckpointPartitionMapReader(configuration) : null;
        this.pathScratch = ownsCandidate ? new Path() : null;
        this.segmentDirectory = ownsCandidate ? new LiveViewCheckpointSegmentDirectoryReader(configuration) : null;
        this.timelineReader = ownsCandidate ? new LiveViewCheckpointTimelineReader(configuration) : null;
        this.dataStore = ownsCandidate ? new LiveViewCheckpointDataStore(configuration, metaStore) : null;
        pages = MapFactory.createOrderedMap(configuration, PAGE_KEY_TYPES, PAGE_VALUE_TYPES, false);
        liveBytesBySegment = MapFactory.createUnorderedMap(
                configuration,
                SEGMENT_KEY_TYPES,
                SEGMENT_VALUE_TYPES,
                false,
                false
        );
        selectedSegments = MapFactory.createUnorderedMap(configuration, SEGMENT_KEY_TYPES, null, false, false);
    }

    /**
     * Binds and opens native capacity for exactly one compaction call.
     */
    public void begin(@Nullable MemoryTracker memoryTracker) {
        if (isActive) {
            throw CairoException.critical(0).put("live view checkpoint compaction scratch already active");
        }
        pages.setMemoryTracker(memoryTracker);
        liveBytesBySegment.setMemoryTracker(memoryTracker);
        selectedSegments.setMemoryTracker(memoryTracker);
        try {
            pages.reopen();
            liveBytesBySegment.reopen();
            selectedSegments.reopen();
            readerIndexBySegment.clear();
            readerCount = 0;
            targetPageCount = 0;
            isActive = true;
        } catch (Throwable th) {
            closeNativeMaps();
            throw th;
        }
    }

    @Override
    public void close() {
        end();
        Misc.freeObjList(readerPool);
        Misc.free(checkpointRoot);
        Misc.free(dataStore);
        Misc.free(functionDirectory);
        Misc.free(functionRoot);
        Misc.free(metaStore);
        Misc.free(partitionReader);
        Misc.free(pathScratch);
        Misc.free(segmentDirectory);
        Misc.free(timelineReader);
        if (candidate != null) {
            candidate.closeReusableResources();
        }
    }

    /**
     * Closes every mapped reader and releases every tracker-bound native map.
     * The Java map/reader shells stay reusable by the writer's next view.
     */
    public void end() {
        if (isActive) {
            lastLivePageCount = (int) pages.size();
            lastLiveSegmentCount = (int) liveBytesBySegment.size();
            lastSelectedSegmentCount = getSelectedSegmentCount();
            lastTargetPageCount = targetPageCount;
        }
        closeReaders();
        closeNativeMaps();
        // The pass's own shells stay; only what they mapped goes, so no compaction
        // holds a mapping into a file the next retire or repair unlinks.
        if (metaStore != null) {
            checkpointRoot.detach();
            dataStore.detach();
            functionDirectory.detach();
            functionRoot.detach();
            metaStore.detach();
            partitionReader.detach();
            segmentDirectory.detach();
            timelineReader.detach();
        }
        currentPage = null;
        pageCursor = null;
        targetPageCount = 0;
        isActive = false;
    }

    LiveViewCheckpointRoot getCheckpointRoot() {
        return checkpointRoot;
    }

    LiveViewCheckpointDataStore getDataStore() {
        return dataStore;
    }

    LiveViewCheckpointFunctionDirectory getFunctionDirectory() {
        return functionDirectory;
    }

    LiveViewCheckpointPageRef getFunctionDirectoryRef() {
        return functionDirectoryRef;
    }

    LiveViewCheckpointFunctionRoot getFunctionRoot() {
        return functionRoot;
    }

    LiveViewCheckpointPageRef getFunctionRootRef() {
        return functionRootRef;
    }

    LiveViewCheckpointMetaStore getMetaStore() {
        return metaStore;
    }

    LiveViewCheckpointPageRef getPartitionMapRoot() {
        return partitionMapRoot;
    }

    LiveViewCheckpointPartitionMapReader getPartitionReader() {
        return partitionReader;
    }

    Path getPathScratch() {
        return pathScratch;
    }

    /**
     * @return the reusable outcome shell of one pass. It stays valid until the
     * same worker's next compaction, which is all the driver's caller reads it for.
     */
    LiveViewCheckpointCompaction.Result getResult() {
        return result;
    }

    LiveViewCheckpointStatePageRef getScalarRef() {
        return scalarRef;
    }

    LiveViewCheckpointSegmentDirectoryReader getSegmentDirectory() {
        return segmentDirectory;
    }

    LiveViewCheckpointTimelineReader getTimelineReader() {
        return timelineReader;
    }

    LiveViewCheckpointDataStore.Candidate acquireCandidate(@NotNull LiveViewCheckpointDataStore owner) {
        if (candidate == null) {
            throw CairoException.critical(0).put("compaction scratch does not own a candidate shell");
        }
        return candidate.of(owner);
    }

    void addLivePage(@NotNull LiveViewCheckpointStatePageRef ref) {
        ensureActive();
        final MapValue page = pageValue(ref, true);
        if (!page.isNew()) {
            if (!sameSourceMetadata(page, ref)) {
                throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                        .put("live view checkpoint shared data page metadata mismatch")
                        .put(" [segmentId=").put(ref.getSegmentId())
                        .put(", offset=").put(ref.getOffset()).put(']');
            }
            return;
        }
        putSourceMetadata(page, ref);

        final MapKey segmentKey = liveBytesBySegment.withKey();
        segmentKey.putLong(ref.getSegmentId());
        final MapValue bytes = segmentKey.createValue();
        bytes.addLong(0, ref.getStoredLength());
    }

    void addSelectedSegment(long segmentId) {
        final MapKey key = selectedSegments.withKey();
        key.putLong(segmentId);
        key.createValue();
    }

    void collectLivePages(
            LiveViewCheckpointTimelineReader timelineReader,
            LiveViewCheckpointPageRef timelineRoot,
            Path checkpointsDir,
            LiveViewCheckpointRoot checkpointRoot,
            LiveViewCheckpointFunctionDirectory functionDirectory,
            LiveViewCheckpointPageRef functionDirectoryRef,
            LiveViewCheckpointFunctionRoot functionRoot,
            LiveViewCheckpointPageRef functionRootRef,
            LiveViewCheckpointStatePageRef scalarRef,
            LiveViewCheckpointPartitionMapReader partitionReader,
            LiveViewCheckpointPageRef partitionMapRoot
    ) {
        livePageTimelineVisitor.of(
                checkpointsDir,
                checkpointRoot,
                functionDirectory,
                functionDirectoryRef,
                functionRoot,
                functionRootRef,
                scalarRef,
                partitionReader,
                partitionMapRoot
        );
        try {
            timelineReader.iterateAll(timelineRoot, livePageTimelineVisitor);
        } finally {
            livePageTimelineVisitor.clearBindings();
        }
    }

    int getSelectedSegmentCount() {
        return (int) selectedSegments.size();
    }

    void finishRepack() {
        closeReaders();
    }

    long getLiveBytes(long segmentId) {
        final MapKey key = liveBytesBySegment.withKey();
        key.putLong(segmentId);
        final MapValue value = key.findValue();
        return value == null ? 0 : value.getLong(0);
    }

    void selectSegments(
            LiveViewCheckpointSegmentDirectoryReader segmentDirectory,
            int maxSourceSegments,
            int maxLiveFractionPercent
    ) {
        selectSegmentVisitor.of(maxSourceSegments, maxLiveFractionPercent);
        try {
            segmentDirectory.iterateAll(selectSegmentVisitor);
        } finally {
            selectSegmentVisitor.clearBindings();
        }
    }

    @TestOnly
    public int getCandidateIdentityForTest() {
        return System.identityHashCode(candidate);
    }

    @TestOnly
    public int getLastLivePageCountForTest() {
        return lastLivePageCount;
    }

    @TestOnly
    public int getLastLiveSegmentCountForTest() {
        return lastLiveSegmentCount;
    }

    @TestOnly
    public int getLastSelectedSegmentCountForTest() {
        return lastSelectedSegmentCount;
    }

    @TestOnly
    public int getLastTargetPageCountForTest() {
        return lastTargetPageCount;
    }

    @TestOnly
    public int getOpenReaderCountForTest() {
        return readerCount;
    }

    @TestOnly
    public int getPlanIdentityForTest() {
        return System.identityHashCode(plan);
    }

    @TestOnly
    public int getReaderShellCountForTest() {
        return readerPool.size();
    }

    @TestOnly
    public int getVisitorShellIdentityForTest(int index) {
        switch (index) {
            case 0:
                return System.identityHashCode(livePageTimelineVisitor);
            case 1:
                return System.identityHashCode(livePagePartitionVisitor);
            default:
                return System.identityHashCode(selectSegmentVisitor);
        }
    }

    @TestOnly
    public boolean isVisitorShellStateClearForTest() {
        return livePageTimelineVisitor.checkpointRoot == null
                && livePageTimelineVisitor.checkpointsDir == null
                && livePageTimelineVisitor.functionDirectory == null
                && livePageTimelineVisitor.functionDirectoryRef == null
                && livePageTimelineVisitor.functionRoot == null
                && livePageTimelineVisitor.functionRootRef == null
                && livePageTimelineVisitor.partitionReader == null
                && livePageTimelineVisitor.partitionMapRoot == null
                && livePageTimelineVisitor.scalarRef == null
                && selectSegmentVisitor.maxLiveFractionPercent == 0
                && selectSegmentVisitor.maxSourceSegments == 0;
    }

    @TestOnly
    public Object getReaderShellForTest(int index) {
        return readerPool.get(index);
    }

    @TestOnly
    public void setTestFailAfterReaderOpenCount(int count) {
        testFailAfterReaderOpenCount = Math.max(0, count);
    }

    @TestOnly
    public void setTestFailAfterRepackedPageCount(int count) {
        testFailAfterRepackedPageCount = Math.max(0, count);
    }

    int getTargetPageCount() {
        return targetPageCount;
    }

    boolean isSelectedSegment(long segmentId) {
        final MapKey key = selectedSegments.withKey();
        key.putLong(segmentId);
        return key.findValue() != null;
    }

    LiveViewCheckpointCompactionPlan ofPlan(long targetSegmentId, long targetSegmentBytes, long generation) {
        return plan.of(targetSegmentId, targetSegmentBytes, generation);
    }

    LiveViewCheckpointDataSegmentReader openReader(
            @Transient @NotNull Path checkpointsDir,
            long segmentId,
            long expectedFileLength
    ) {
        int index = readerIndexBySegment.get(segmentId);
        if (index > -1) {
            return readerPool.getQuick(index);
        }
        index = readerCount;
        final LiveViewCheckpointDataSegmentReader reader;
        if (index < readerPool.size()) {
            reader = readerPool.getQuick(index);
        } else {
            reader = new LiveViewCheckpointDataSegmentReader(configuration);
            readerPool.add(reader);
        }
        try {
            reader.of(checkpointsDir, segmentId, expectedFileLength);
        } catch (Throwable th) {
            Misc.free(reader);
            throw th;
        }
        readerIndexBySegment.put(segmentId, index);
        readerCount++;
        if (testFailAfterReaderOpenCount > 0 && readerCount >= testFailAfterReaderOpenCount) {
            throw CairoException.critical(0)
                    .put("test failure after live view checkpoint compaction reader open");
        }
        return reader;
    }

    @Nullable LiveViewCheckpointStatePageRef redirect(
            @NotNull LiveViewCheckpointStatePageRef source,
            @NotNull LiveViewCheckpointStatePageRef targetFlyweight
    ) {
        final MapValue value = pageValue(source, false);
        if (value == null || value.getLong(PAGE_TARGET_SEGMENT_ID) < 0) {
            return null;
        }
        return targetFlyweight.of(
                value.getLong(PAGE_TARGET_SEGMENT_ID),
                value.getLong(PAGE_TARGET_OFFSET),
                value.getInt(PAGE_TARGET_STORED_LENGTH),
                value.getInt(PAGE_TARGET_DECODED_LENGTH),
                value.getInt(PAGE_TARGET_KIND),
                value.getInt(PAGE_TARGET_CODEC),
                value.getInt(PAGE_TARGET_ROW_COUNT),
                value.getInt(PAGE_TARGET_FLAGS)
        );
    }

    void recordTarget(@NotNull LiveViewCheckpointStatePageRef target) {
        if (currentPage == null) {
            throw CairoException.critical(0).put("live view checkpoint compaction page cursor is not positioned");
        }
        final MapValue value = currentPage.getValue();
        value.putLong(PAGE_TARGET_SEGMENT_ID, target.getSegmentId());
        value.putLong(PAGE_TARGET_OFFSET, target.getOffset());
        value.putInt(PAGE_TARGET_STORED_LENGTH, target.getStoredLength());
        value.putInt(PAGE_TARGET_DECODED_LENGTH, target.getDecodedLength());
        value.putInt(PAGE_TARGET_KIND, target.getPageKind());
        value.putInt(PAGE_TARGET_CODEC, target.getCodec());
        value.putInt(PAGE_TARGET_ROW_COUNT, target.getRowCount());
        value.putInt(PAGE_TARGET_FLAGS, target.getFlags());
        targetPageCount++;
        if (testFailAfterRepackedPageCount > 0 && targetPageCount >= testFailAfterRepackedPageCount) {
            throw CairoException.critical(0)
                    .put("test failure after live view checkpoint compaction page repack");
        }
    }

    void startSelectedPageIteration() {
        pageCursor = pages.getCursor();
        currentPage = null;
    }

    boolean nextSelectedPage(@NotNull LiveViewCheckpointStatePageRef sourceFlyweight) {
        while (pageCursor.hasNext()) {
            final MapRecord record = pageCursor.getRecord();
            final long segmentId = record.getLong(PAGE_KEY_SEGMENT_ID);
            if (!isSelectedSegment(segmentId)) {
                continue;
            }
            currentPage = record;
            sourceFlyweight.of(
                    segmentId,
                    record.getLong(PAGE_KEY_OFFSET),
                    record.getInt(PAGE_KEY_OFFSET + 1),
                    record.getInt(PAGE_SOURCE_DECODED_LENGTH),
                    record.getInt(PAGE_SOURCE_KIND),
                    record.getInt(PAGE_SOURCE_CODEC),
                    record.getInt(PAGE_SOURCE_ROW_COUNT),
                    record.getInt(PAGE_SOURCE_FLAGS)
            );
            return true;
        }
        currentPage = null;
        return false;
    }

    private void closeNativeMaps() {
        pages.close();
        liveBytesBySegment.close();
        selectedSegments.close();
        pages.setMemoryTracker(null);
        liveBytesBySegment.setMemoryTracker(null);
        selectedSegments.setMemoryTracker(null);
    }

    private void closeReaders() {
        for (int i = 0; i < readerCount; i++) {
            Misc.free(readerPool.getQuick(i));
        }
        readerIndexBySegment.clear();
        readerCount = 0;
    }

    private void ensureActive() {
        if (!isActive) {
            throw CairoException.critical(0).put("live view checkpoint compaction scratch is not active");
        }
    }

    private MapValue pageValue(@NotNull LiveViewCheckpointStatePageRef ref, boolean create) {
        final MapKey key = pages.withKey();
        key.putLong(ref.getSegmentId());
        key.putLong(ref.getOffset());
        key.putInt(ref.getStoredLength());
        return create ? key.createValue() : key.findValue();
    }

    private static void putSourceMetadata(MapValue value, LiveViewCheckpointStatePageRef ref) {
        value.putInt(PAGE_SOURCE_DECODED_LENGTH, ref.getDecodedLength());
        value.putInt(PAGE_SOURCE_KIND, ref.getPageKind());
        value.putInt(PAGE_SOURCE_CODEC, ref.getCodec());
        value.putInt(PAGE_SOURCE_ROW_COUNT, ref.getRowCount());
        value.putInt(PAGE_SOURCE_FLAGS, ref.getFlags());
        value.putLong(PAGE_TARGET_SEGMENT_ID, -1);
    }

    private static boolean sameSourceMetadata(MapValue value, LiveViewCheckpointStatePageRef ref) {
        return value.getInt(PAGE_SOURCE_DECODED_LENGTH) == ref.getDecodedLength()
                && value.getInt(PAGE_SOURCE_KIND) == ref.getPageKind()
                && value.getInt(PAGE_SOURCE_CODEC) == ref.getCodec()
                && value.getInt(PAGE_SOURCE_ROW_COUNT) == ref.getRowCount()
                && value.getInt(PAGE_SOURCE_FLAGS) == ref.getFlags();
    }

    private final class LivePagePartitionVisitor implements LiveViewCheckpointPartitionMapReader.Visitor {
        @Override
        public void onEntry(@NotNull LiveViewCheckpointPartitionMapEntry entry) {
            for (int p = 0, n = entry.getStatePageCount(); p < n; p++) {
                addLivePage(entry.getStatePageRef(p));
            }
        }
    }

    private final class LivePageTimelineVisitor implements LiveViewCheckpointTimelineReader.Visitor {
        private LiveViewCheckpointRoot checkpointRoot;
        private Path checkpointsDir;
        private LiveViewCheckpointFunctionDirectory functionDirectory;
        private LiveViewCheckpointPageRef functionDirectoryRef;
        private LiveViewCheckpointFunctionRoot functionRoot;
        private LiveViewCheckpointPageRef functionRootRef;
        private LiveViewCheckpointPartitionMapReader partitionReader;
        private LiveViewCheckpointPageRef partitionMapRoot;
        private LiveViewCheckpointStatePageRef scalarRef;

        private void clearBindings() {
            checkpointRoot = null;
            checkpointsDir = null;
            functionDirectory = null;
            functionDirectoryRef = null;
            functionRoot = null;
            functionRootRef = null;
            partitionReader = null;
            partitionMapRoot = null;
            scalarRef = null;
        }

        private void of(
                Path checkpointsDir,
                LiveViewCheckpointRoot checkpointRoot,
                LiveViewCheckpointFunctionDirectory functionDirectory,
                LiveViewCheckpointPageRef functionDirectoryRef,
                LiveViewCheckpointFunctionRoot functionRoot,
                LiveViewCheckpointPageRef functionRootRef,
                LiveViewCheckpointStatePageRef scalarRef,
                LiveViewCheckpointPartitionMapReader partitionReader,
                LiveViewCheckpointPageRef partitionMapRoot
        ) {
            this.checkpointsDir = checkpointsDir;
            this.checkpointRoot = checkpointRoot;
            this.functionDirectory = functionDirectory;
            this.functionDirectoryRef = functionDirectoryRef;
            this.functionRoot = functionRoot;
            this.functionRootRef = functionRootRef;
            this.scalarRef = scalarRef;
            this.partitionReader = partitionReader;
            this.partitionMapRoot = partitionMapRoot;
        }

        @Override
        public void onEntry(@NotNull LiveViewCheckpointTimelineEntry entry) {
            checkpointRoot.of(checkpointsDir, entry.rootRef);
            checkpointRoot.getFunctionDirectoryRef(functionDirectoryRef);
            functionDirectory.of(checkpointsDir, functionDirectoryRef);
            for (int i = 0, n = functionDirectory.size(); i < n; i++) {
                functionDirectory.getRootRef(i, functionRootRef);
                functionRoot.of(checkpointsDir, functionRootRef);
                functionRoot.getScalarStateRef(scalarRef);
                if (!scalarRef.isNull()) {
                    addLivePage(scalarRef);
                }
                functionRoot.getPartitionMapRootRef(partitionMapRoot);
                partitionReader.iterateAll(partitionMapRoot, livePagePartitionVisitor);
            }
        }
    }

    private final class SelectSegmentVisitor implements LiveViewCheckpointSegmentDirectoryReader.Visitor {
        private int maxLiveFractionPercent;
        private int maxSourceSegments;

        private void clearBindings() {
            maxLiveFractionPercent = 0;
            maxSourceSegments = 0;
        }

        private void of(int maxSourceSegments, int maxLiveFractionPercent) {
            this.maxSourceSegments = maxSourceSegments;
            this.maxLiveFractionPercent = maxLiveFractionPercent;
        }

        @Override
        public void onEntry(@NotNull LiveViewCheckpointSegmentDirectoryEntry entry) {
            if (getSelectedSegmentCount() >= maxSourceSegments || entry.isMetadata() || entry.referenceCount <= 0) {
                return;
            }
            final long liveBytes = getLiveBytes(entry.segmentId);
            if (liveBytes <= 0 || liveBytes >= entry.fileLength) {
                return;
            }
            if (liveBytes <= entry.fileLength / 100.0 * maxLiveFractionPercent) {
                addSelectedSegment(entry.segmentId);
            }
        }
    }
}
