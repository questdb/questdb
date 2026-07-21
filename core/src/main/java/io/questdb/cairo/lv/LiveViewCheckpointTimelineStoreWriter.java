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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashSet;

/**
 * Composes the immutable page/root stores into one crash-ordered normal
 * checkpoint publication. This writer is deliberately limited to strictly
 * in-order cadence appends; O3 range splicing is owned by the repair phases.
 */
public class LiveViewCheckpointTimelineStoreWriter implements Closeable {

    public static final int ANCHOR_STATE_PAGE_KIND = 0x40;
    public static final int FUNCTION_STATE_PAGE_KIND = 0x41;
    public static final int RAW_CODEC = 0;
    @TestOnly
    public static final int TEST_FAIL_AFTER_DATA_PUBLISH = 1;
    @TestOnly
    public static final int TEST_FAIL_AFTER_METADATA_PUBLISH = 2;

    private final CairoConfiguration configuration;
    private final MemoryCARW keyBuffer;
    @TestOnly
    private int testFailureStage;

    public LiveViewCheckpointTimelineStoreWriter(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        this.keyBuffer = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        Misc.free(keyBuffer);
    }

    public Result append(
            @Transient @NotNull Path checkpointsDir,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow,
            long definitionTxn,
            long createdLvSeqTxn,
            long normalizedBaseSeqTxn,
            long coveredLvSeqTxn,
            long maxTimestamp,
            long effectiveLvRowPosition
    ) {
        if (definitionTxn < 0
                || createdLvSeqTxn < 0
                || normalizedBaseSeqTxn < 0
                || coveredLvSeqTxn < 0
                || effectiveLvRowPosition < 0
                || createdLvSeqTxn > coveredLvSeqTxn) {
            throw CairoException.critical(0).put("invalid live view normal checkpoint coordinates");
        }
        ensureDirectories(checkpointsDir);

        try (
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration);
                LiveViewCheckpointTimelineReader timelineReader = new LiveViewCheckpointTimelineReader(configuration);
                LiveViewCheckpointRowPositionDeltaReader deltaReader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
                LiveViewCheckpointRoot oldCheckpointRoot = new LiveViewCheckpointRoot(configuration);
                LiveViewCheckpointFunctionDirectory oldFunctionDirectory = new LiveViewCheckpointFunctionDirectory(configuration);
                LiveViewCheckpointSegmentDirectory segmentDirectory = new LiveViewCheckpointSegmentDirectory(configuration);
                LiveViewCheckpointDataSegmentWriter dataWriter = new LiveViewCheckpointDataSegmentWriter(configuration);
                LiveViewCheckpointMetaSegmentWriter metaWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
                LiveViewCheckpointFunctionRootBuilder functionRootBuilder = new LiveViewCheckpointFunctionRootBuilder(configuration);
                LiveViewCheckpointFunctionRoot oldFunctionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                LiveViewCheckpointPartitionMapReader oldPartitionReader = new LiveViewCheckpointPartitionMapReader(configuration);
                LiveViewCheckpointRootBuilder checkpointRootBuilder = new LiveViewCheckpointRootBuilder(configuration);
                LiveViewCheckpointTimelineWriter timelineWriter = new LiveViewCheckpointTimelineWriter(configuration)
        ) {
            metaStore.of(checkpointsDir);
            timelineReader.of(checkpointsDir);
            deltaReader.of(checkpointsDir);
            oldPartitionReader.of(checkpointsDir);
            timelineWriter.of(checkpointsDir);

            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (metaStore.isValid() && superblock.definitionTxn != definitionTxn) {
                throw CairoException.critical(0)
                        .put("live view checkpoint definition identity changed")
                        .put(" [stored=").put(superblock.definitionTxn)
                        .put(", current=").put(definitionTxn).put(']');
            }
            if (metaStore.isValid()
                    && (normalizedBaseSeqTxn < superblock.normalizedBaseSeqTxn
                    || coveredLvSeqTxn < superblock.coveredLvSeqTxn)) {
                throw CairoException.critical(0)
                        .put("live view checkpoint generation watermarks must not move backwards")
                        .put(" [storedBase=").put(superblock.normalizedBaseSeqTxn)
                        .put(", nextBase=").put(normalizedBaseSeqTxn)
                        .put(", storedLv=").put(superblock.coveredLvSeqTxn)
                        .put(", nextLv=").put(coveredLvSeqTxn).put(']');
            }

            final long generation = metaStore.isValid()
                    ? checkedIncrement(superblock.generation, "generation")
                    : 1;
            final long checkpointId = metaStore.isValid() ? superblock.nextCheckpointId : 0;
            checkedIncrement(checkpointId, "checkpoint id");

            final LiveViewCheckpointPageRef oldTimelineRoot = copy(superblock.timelineRootRef);
            final LiveViewCheckpointPageRef oldDeltaRoot = copy(superblock.rowPositionDeltaRootRef);
            final LiveViewCheckpointPageRef oldDirectoryRoot = copy(superblock.segmentDirectoryRootRef);
            final LiveViewCheckpointTimelineEntry previousEntry = new LiveViewCheckpointTimelineEntry();
            final boolean hasPrevious = timelineReader.last(oldTimelineRoot, previousEntry);
            if (hasPrevious && maxTimestamp <= previousEntry.maxTimestamp) {
                throw CairoException.critical(0)
                        .put("normal live view checkpoint boundary overlaps current head")
                        .put(" [head=").put(previousEntry.maxTimestamp)
                        .put(", candidate=").put(maxTimestamp).put(']');
            }

            final LiveViewCheckpointPageRef oldFunctionDirectoryRef = new LiveViewCheckpointPageRef();
            if (hasPrevious) {
                oldCheckpointRoot.of(checkpointsDir, previousEntry.rootRef);
                if (oldCheckpointRoot.getDefinitionTxn() != definitionTxn) {
                    throw CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                            .put("live view checkpoint root definition identity mismatch");
                }
                oldCheckpointRoot.getFunctionDirectoryRef(oldFunctionDirectoryRef);
                oldFunctionDirectory.of(checkpointsDir, oldFunctionDirectoryRef);
            }
            segmentDirectory.of(checkpointsDir, oldDirectoryRoot);

            long nextSegmentId = metaStore.isValid() ? superblock.nextSegmentId : 0;
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final long dataSegmentId = nextSegmentId++;
            dataWriter.of(checkpointsDir, dataSegmentId);

            long logicalStateBytes = 0;
            final LiveViewCheckpointStatePageRef anchorStateRef = new LiveViewCheckpointStatePageRef();
            if (anchorWindow != null) {
                final MemoryA sink = dataWriter.beginPage();
                final long start = sink.getAppendOffset();
                anchorWindow.snapshot(sink);
                final int bytes = checkedIntLength(sink.getAppendOffset() - start, "anchor state");
                dataWriter.endPage(anchorStateRef, bytes, ANCHOR_STATE_PAGE_KIND, RAW_CODEC, 1, 0);
                logicalStateBytes = checkedAdd(logicalStateBytes, bytes);
            }

            final ObjList<FrozenFunction> frozenFunctions = new ObjList<>(functions.size());
            for (int i = 0, n = functions.size(); i < n; i++) {
                final WindowFunction function = functions.getQuick(i);
                if (!function.supportsCheckpointState()) {
                    continue;
                }
                final LiveViewCheckpointFunctionIdentity identity = function.checkpointFunctionIdentity();
                if (identity == null || function.checkpointDependency() == null) {
                    throw CairoException.critical(0)
                            .put("checkpoint-capable live view function has no compiler metadata");
                }
                final FrozenFunction frozen = new FrozenFunction(
                        identity.getEncoded(),
                        function.checkpointStateFormatVersion(),
                        encodeKeySchema(function.getCheckpointKeyColumnTypes())
                );
                logicalStateBytes = checkedAdd(logicalStateBytes, freezeFunction(dataWriter, function, frozen));
                frozenFunctions.add(frozen);
            }
            if (frozenFunctions.size() == 0) {
                throw CairoException.critical(0).put("cannot seal live view checkpoint without functions");
            }

            final long dataSegmentBytes = dataWriter.commit();
            if (testFailureStage == TEST_FAIL_AFTER_DATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint data publication");
            }

            long metadataBytesAdded = 0;
            final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
            final LongList anchorSegmentIds = new LongList();
            if (anchorWindow != null) {
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                metaWriter.of(checkpointsDir, nextSegmentId++);
                LiveViewCheckpointAnchorRoot.writeTo(metaWriter, anchorStateRef, anchorRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, metaWriter.commit());
                anchorSegmentIds.add(dataSegmentId);
            }

            final ObjList<LiveViewCheckpointPageRef> functionRootRefs = new ObjList<>(frozenFunctions.size());
            for (int i = 0, n = frozenFunctions.size(); i < n; i++) {
                final FrozenFunction frozen = frozenFunctions.getQuick(i);
                final LiveViewCheckpointPageRef oldFunctionRootRef = new LiveViewCheckpointPageRef();
                if (hasPrevious) {
                    oldFunctionDirectory.find(frozen.identity, oldFunctionRootRef);
                }
                functionRootBuilder.of(
                        checkpointsDir,
                        oldFunctionRootRef,
                        frozen.identity,
                        frozen.stateFormatVersion,
                        frozen.keySchema
                );
                if (frozen.scalarStateRef != null) {
                    functionRootBuilder.setScalarStateRef(frozen.scalarStateRef);
                } else {
                    removeMissingPartitions(
                            checkpointsDir,
                            oldFunctionRootRef,
                            oldFunctionRoot,
                            oldPartitionReader,
                            frozen,
                            functionRootBuilder
                    );
                    for (int p = 0, m = frozen.partitions.size(); p < m; p++) {
                        final FrozenPartition partition = frozen.partitions.getQuick(p);
                        functionRootBuilder.putPartition(
                                partition.key,
                                partition.scalarState,
                                partition.statePageRefs
                        );
                    }
                }
                nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                functionRootBuilder.build(nextSegmentId++, functionRootRef);
                metadataBytesAdded = checkedAdd(metadataBytesAdded, functionRootBuilder.getLastSegmentBytes());
                functionRootRefs.add(functionRootRef);
            }

            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            checkpointRootBuilder.begin(
                    checkpointsDir,
                    checkpointId,
                    maxTimestamp,
                    definitionTxn,
                    anchorRootRef,
                    anchorSegmentIds
            );
            for (int i = 0, n = functionRootRefs.size(); i < n; i++) {
                checkpointRootBuilder.addFunction(functionRootRefs.getQuick(i));
            }
            final LiveViewCheckpointPageRef checkpointRootRef = new LiveViewCheckpointPageRef();
            checkpointRootBuilder.build(nextSegmentId++, checkpointRootRef);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, checkpointRootBuilder.getLastSegmentBytes());

            final long prefixCorrection = deltaReader.prefixSum(oldDeltaRoot, maxTimestamp, checkpointId);
            final long baseLvRowPosition;
            try {
                baseLvRowPosition = Math.subtractExact(effectiveLvRowPosition, prefixCorrection);
            } catch (ArithmeticException e) {
                throw CairoException.critical(0).put("live view checkpoint row position overflow");
            }
            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry()
                    .of(maxTimestamp, checkpointId, createdLvSeqTxn, baseLvRowPosition, logicalStateBytes);
            entry.rootRef.of(
                    checkpointRootRef.getSegmentId(),
                    checkpointRootRef.getOffset(),
                    checkpointRootRef.getLength()
            );
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            final LiveViewCheckpointPageRef newTimelineRoot = new LiveViewCheckpointPageRef();
            timelineWriter.append(oldTimelineRoot, entry, nextSegmentId++, newTimelineRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, timelineWriter.getLastSegmentBytes());

            final LongList newRootSegmentIds = new LongList();
            checkpointRootBuilder.getReferencedSegmentIds(newRootSegmentIds);
            segmentDirectory.addSegment(dataSegmentId, dataSegmentBytes, 1);
            final LongList reusedSegmentIds = new LongList();
            for (int i = 0, n = newRootSegmentIds.size(); i < n; i++) {
                final long segmentId = newRootSegmentIds.getQuick(i);
                if (segmentId != dataSegmentId) {
                    reusedSegmentIds.add(segmentId);
                }
            }
            if (reusedSegmentIds.size() > 0) {
                segmentDirectory.applyRootReferenceChanges(new LongList(), reusedSegmentIds, generation);
            }
            nextSegmentId = skipPublishedSegmentIds(checkpointsDir, nextSegmentId);
            metaWriter.of(checkpointsDir, nextSegmentId++);
            final LiveViewCheckpointPageRef newDirectoryRoot = new LiveViewCheckpointPageRef();
            segmentDirectory.writeTo(metaWriter, newDirectoryRoot);
            metadataBytesAdded = checkedAdd(metadataBytesAdded, metaWriter.commit());
            if (testFailureStage == TEST_FAIL_AFTER_METADATA_PUBLISH) {
                throw CairoException.critical(0).put("test failure after live view checkpoint metadata publication");
            }

            superblock.generation = generation;
            superblock.definitionTxn = definitionTxn;
            superblock.normalizedBaseSeqTxn = normalizedBaseSeqTxn;
            superblock.coveredLvSeqTxn = coveredLvSeqTxn;
            superblock.nextCheckpointId = checkedIncrement(checkpointId, "checkpoint id");
            superblock.nextSegmentId = nextSegmentId;
            superblock.metadataBytes = checkedAdd(metaStore.isValid() ? superblock.metadataBytes : 0, metadataBytesAdded);
            superblock.dataBytes = checkedAdd(metaStore.isValid() ? superblock.dataBytes : 0, dataSegmentBytes);
            copy(newTimelineRoot, superblock.timelineRootRef);
            copy(oldDeltaRoot, superblock.rowPositionDeltaRootRef);
            copy(newDirectoryRoot, superblock.segmentDirectoryRootRef);
            metaStore.publish();

            return new Result(
                    generation,
                    checkpointId,
                    logicalStateBytes,
                    dataSegmentBytes,
                    metadataBytesAdded,
                    metaStore.getWalPurgeFloor()
            );
        }
    }

    @TestOnly
    public void setTestFailureStage(int testFailureStage) {
        this.testFailureStage = testFailureStage;
    }

    private static long checkedAdd(long a, long b) {
        try {
            return Math.addExact(a, b);
        } catch (ArithmeticException e) {
            throw CairoException.critical(0).put("live view checkpoint byte count overflow");
        }
    }

    private static int checkedIntLength(long value, CharSequence what) {
        if (value <= 0 || value > Integer.MAX_VALUE) {
            throw CairoException.critical(0).put("live view checkpoint ").put(what)
                    .put(" length out of bounds, bytes=").put(value);
        }
        return (int) value;
    }

    private static long checkedIncrement(long value, CharSequence what) {
        if (value == Long.MAX_VALUE) {
            throw CairoException.critical(0).put("live view checkpoint ").put(what).put(" exhausted");
        }
        return value + 1;
    }

    private static LiveViewCheckpointPageRef copy(LiveViewCheckpointPageRef from) {
        return new LiveViewCheckpointPageRef().of(from.getSegmentId(), from.getOffset(), from.getLength());
    }

    private static void copy(LiveViewCheckpointPageRef from, LiveViewCheckpointPageRef to) {
        to.of(from.getSegmentId(), from.getOffset(), from.getLength());
    }

    private static byte[] encodeKeySchema(@Nullable ColumnTypes keyTypes) {
        final int count = keyTypes == null ? 0 : keyTypes.getColumnCount();
        final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + count * Integer.BYTES);
        buffer.putInt(count);
        for (int i = 0; i < count; i++) {
            buffer.putInt(keyTypes.getColumnType(i));
        }
        return buffer.array();
    }

    private void ensureDirectories(Path checkpointsDir) {
        try (Path path = new Path()) {
            LiveViewCheckpointLayout.metaDirPath(path, checkpointsDir).slash();
            if (configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode()) != 0) {
                throw CairoException.critical(configuration.getFilesFacade().errno())
                        .put("could not create live view checkpoint metadata directory [path=").put(path).put(']');
            }
            LiveViewCheckpointLayout.dataDirPath(path, checkpointsDir).slash();
            if (configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode()) != 0) {
                throw CairoException.critical(configuration.getFilesFacade().errno())
                        .put("could not create live view checkpoint data directory [path=").put(path).put(']');
            }
        }
    }

    private long freezeFunction(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            WindowFunction function,
            FrozenFunction frozen
    ) {
        final Map map = function.getPartitionMap();
        if (map == null) {
            final LiveViewCheckpointStatePageRef ref = freezeStatePage(dataWriter, function, null);
            frozen.scalarStateRef = ref;
            return ref.getDecodedLength();
        }

        long logicalBytes = 0;
        final ColumnTypes keyTypes = function.getCheckpointKeyColumnTypes();
        final int keyStartIndex = function.getCheckpointKeyStartIndex();
        final int tombstoneIndex = function.getTombstoneValueIndex();
        final MapRecordCursor cursor = map.getCursor();
        final MapRecord record = map.getRecord();
        while (cursor.hasNext()) {
            final MapValue value = record.getValue();
            if (tombstoneIndex >= 0 && value.getByte(tombstoneIndex) == 1) {
                continue;
            }
            keyBuffer.jumpTo(0);
            LiveViewSnapshotKeyCodec.writeKey(keyBuffer, record, keyTypes, keyStartIndex);
            final int keyLength = checkedIntLength(keyBuffer.getAppendOffset(), "partition key");
            final byte[] key = new byte[keyLength];
            for (int i = 0; i < keyLength; i++) {
                key[i] = keyBuffer.getByte(i);
            }
            final LiveViewCheckpointStatePageRef stateRef = freezeStatePage(dataWriter, function, value);
            frozen.addPartition(key, stateRef);
            logicalBytes = checkedAdd(logicalBytes, keyLength);
            logicalBytes = checkedAdd(logicalBytes, stateRef.getDecodedLength());
        }
        return logicalBytes;
    }

    private LiveViewCheckpointStatePageRef freezeStatePage(
            LiveViewCheckpointDataSegmentWriter dataWriter,
            WindowFunction function,
            @Nullable MapValue value
    ) {
        final MemoryA sink = dataWriter.beginPage();
        final LiveViewStatePageWriter pageWriter = new LiveViewStatePageWriter().of(sink);
        function.freezeCheckpointState(pageWriter, value);
        final int bytes = checkedIntLength(pageWriter.size(), "function state");
        final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
        dataWriter.endPage(ref, bytes, FUNCTION_STATE_PAGE_KIND, RAW_CODEC, 1, 0);
        return ref;
    }

    private static void removeMissingPartitions(
            Path checkpointsDir,
            LiveViewCheckpointPageRef oldFunctionRootRef,
            LiveViewCheckpointFunctionRoot oldFunctionRoot,
            LiveViewCheckpointPartitionMapReader oldPartitionReader,
            FrozenFunction frozen,
            LiveViewCheckpointFunctionRootBuilder builder
    ) {
        if (oldFunctionRootRef.isNull()) {
            return;
        }
        oldFunctionRoot.of(checkpointsDir, oldFunctionRootRef);
        final LiveViewCheckpointPageRef oldPartitionRoot = new LiveViewCheckpointPageRef();
        oldFunctionRoot.getPartitionMapRootRef(oldPartitionRoot);
        oldPartitionReader.iterateAll(oldPartitionRoot, entry -> {
            if (!frozen.currentKeys.contains(ByteBuffer.wrap(entry.getKey()))) {
                builder.removePartition(entry.getKey());
            }
        });
    }

    private long skipPublishedSegmentIds(Path checkpointsDir, long candidate) {
        try (Path path = new Path()) {
            while (true) {
                LiveViewCheckpointLayout.metaSegmentPath(path, checkpointsDir, candidate);
                final boolean metaExists = configuration.getFilesFacade().exists(path.$());
                LiveViewCheckpointLayout.dataSegmentPath(path, checkpointsDir, candidate);
                if (!metaExists && !configuration.getFilesFacade().exists(path.$())) {
                    if (candidate == Long.MAX_VALUE) {
                        throw CairoException.critical(0).put("live view checkpoint segment id exhausted");
                    }
                    return candidate;
                }
                if (candidate == Long.MAX_VALUE) {
                    throw CairoException.critical(0).put("live view checkpoint segment id exhausted");
                }
                candidate++;
            }
        }
    }

    private static final class FrozenFunction {
        private final HashSet<ByteBuffer> currentKeys = new HashSet<>();
        private final byte[] identity;
        private final byte[] keySchema;
        private final ObjList<FrozenPartition> partitions = new ObjList<>();
        private LiveViewCheckpointStatePageRef scalarStateRef;
        private final int stateFormatVersion;

        private FrozenFunction(byte[] identity, int stateFormatVersion, byte[] keySchema) {
            this.identity = identity;
            this.stateFormatVersion = stateFormatVersion;
            this.keySchema = keySchema;
        }

        private void addPartition(byte[] key, LiveViewCheckpointStatePageRef stateRef) {
            currentKeys.add(ByteBuffer.wrap(key));
            partitions.add(new FrozenPartition(
                    key,
                    new byte[0],
                    new LiveViewCheckpointStatePageRef[]{stateRef}
            ));
        }
    }

    private static final class FrozenPartition {
        private final byte[] key;
        private final byte[] scalarState;
        private final LiveViewCheckpointStatePageRef[] statePageRefs;

        private FrozenPartition(byte[] key, byte[] scalarState, LiveViewCheckpointStatePageRef[] statePageRefs) {
            this.key = key;
            this.scalarState = scalarState;
            this.statePageRefs = statePageRefs;
        }
    }

    public static final class Result {
        private final long checkpointId;
        private final long dataBytesAdded;
        private final long generation;
        private final long logicalStateBytes;
        private final long metadataBytesAdded;
        private final long walPurgeFloor;

        private Result(
                long generation,
                long checkpointId,
                long logicalStateBytes,
                long dataBytesAdded,
                long metadataBytesAdded,
                long walPurgeFloor
        ) {
            this.generation = generation;
            this.checkpointId = checkpointId;
            this.logicalStateBytes = logicalStateBytes;
            this.dataBytesAdded = dataBytesAdded;
            this.metadataBytesAdded = metadataBytesAdded;
            this.walPurgeFloor = walPurgeFloor;
        }

        public long getCheckpointId() {
            return checkpointId;
        }

        public long getDataBytesAdded() {
            return dataBytesAdded;
        }

        public long getGeneration() {
            return generation;
        }

        public long getLogicalStateBytes() {
            return logicalStateBytes;
        }

        public long getMetadataBytesAdded() {
            return metadataBytesAdded;
        }

        public long getWalPurgeFloor() {
            return walPurgeFloor;
        }
    }
}
