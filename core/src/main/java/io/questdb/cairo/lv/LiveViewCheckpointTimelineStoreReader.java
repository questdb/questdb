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
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Resolves and restores one exact logical checkpoint from a pinned timeline
 * generation. Startup reconciliation and root selection are intentionally left
 * to the recovery layer; this component owns only indexed lookup, lazy deep
 * validation, and rehydration of a caller-owned runtime.
 */
public class LiveViewCheckpointTimelineStoreReader implements Closeable {

    private static final int DATA_READER_CACHE_SIZE = 8;
    private final LiveViewCheckpointAnchorRoot anchorRoot;
    private final Path checkpointsDir = new Path();
    private final CairoConfiguration configuration;
    private final LiveViewCheckpointDataSegmentReader[] dataReaders =
            new LiveViewCheckpointDataSegmentReader[DATA_READER_CACHE_SIZE];
    private final long[] dataSegmentIds = new long[DATA_READER_CACHE_SIZE];
    private final LiveViewCheckpointRowPositionDeltaReader deltaReader;
    private final LiveViewCheckpointFunctionDirectory functionDirectory;
    private final LiveViewCheckpointFunctionRoot functionRoot;
    private final MemoryCARW keyMemory;
    private final LiveViewStatePageReader keyPageReader = new LiveViewStatePageReader();
    private final LiveViewCheckpointMetaStore metaStore;
    private final LiveViewCheckpointPartitionMapReader partitionReader;
    private final LiveViewCheckpointRoot root;
    private final LiveViewCheckpointSegmentDirectory segmentDirectory;
    private final LiveViewStatePageReader statePageReader = new LiveViewStatePageReader();
    private final LiveViewCheckpointTimelineReader timelineReader;
    private int dataReaderClock;
    private boolean isOpen;

    public LiveViewCheckpointTimelineStoreReader(@NotNull CairoConfiguration configuration) {
        this.configuration = configuration;
        anchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
        deltaReader = new LiveViewCheckpointRowPositionDeltaReader(configuration);
        functionDirectory = new LiveViewCheckpointFunctionDirectory(configuration);
        functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
        keyMemory = Vm.getCARWInstance(4096, Integer.MAX_VALUE, MemoryTag.NATIVE_DEFAULT);
        metaStore = new LiveViewCheckpointMetaStore(configuration);
        partitionReader = new LiveViewCheckpointPartitionMapReader(configuration);
        root = new LiveViewCheckpointRoot(configuration);
        segmentDirectory = new LiveViewCheckpointSegmentDirectory(configuration);
        timelineReader = new LiveViewCheckpointTimelineReader(configuration);
        Arrays.fill(dataSegmentIds, -1);
    }

    @Override
    public void close() {
        Misc.free(anchorRoot);
        for (int i = 0; i < DATA_READER_CACHE_SIZE; i++) {
            dataReaders[i] = Misc.free(dataReaders[i]);
            dataSegmentIds[i] = -1;
        }
        Misc.free(deltaReader);
        Misc.free(functionDirectory);
        Misc.free(functionRoot);
        Misc.free(keyMemory);
        Misc.free(metaStore);
        Misc.free(partitionReader);
        Misc.free(root);
        Misc.free(segmentDirectory);
        Misc.free(timelineReader);
        Misc.free(checkpointsDir);
        isOpen = false;
    }

    public void of(@Transient @NotNull Path checkpointsDir) {
        if (isOpen) {
            throw CairoException.critical(0).put("live view checkpoint timeline restore reader already open");
        }
        this.checkpointsDir.of(checkpointsDir);
        metaStore.of(checkpointsDir);
        if (!metaStore.isValid()) {
            throw invalid("has no valid generation to restore");
        }
        deltaReader.of(checkpointsDir);
        partitionReader.of(checkpointsDir);
        timelineReader.of(checkpointsDir);
        isOpen = true;
    }

    /**
     * Restores the exact composite timeline key into the supplied, already-open
     * runtime. The caller owns the runtime and may discard it if a function
     * decoder reports semantic corruption.
     */
    public Result restore(
            long maxTimestamp,
            long checkpointId,
            long expectedDefinitionTxn,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow
    ) {
        ensureOpen();
        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            return restorePinned(pin, maxTimestamp, checkpointId, expectedDefinitionTxn, functions, anchorWindow);
        }
    }

    /**
     * Selects and restores the newest root compatible with the reconciled
     * durable live-view coordinates. Selection and lazy root/page validation
     * run under the same generation pin, so publication cannot mix tree roots
     * from different generations.
     */
    public Result restoreLatestCompatible(
            long durableFrontierTimestamp,
            long durableBaseSeqTxn,
            long durableLvSeqTxn,
            long durableLvRowCount,
            long expectedDefinitionTxn,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow
    ) {
        ensureOpen();
        try (LiveViewCheckpointGenerationPin pin = metaStore.pin()) {
            final LiveViewCheckpointSuperblock superblock = metaStore.getSuperblock();
            if (superblock.definitionTxn != expectedDefinitionTxn) {
                throw invalid("definition identity mismatch [stored=").put(superblock.definitionTxn)
                        .put(", expected=").put(expectedDefinitionTxn).put(']');
            }
            if (pin.getNormalizedBaseSeqTxn() > durableBaseSeqTxn
                    || pin.getCoveredLvSeqTxn() > durableLvSeqTxn) {
                throw invalid("generation is ahead of durable materialization")
                        .put(" [generationBase=").put(pin.getNormalizedBaseSeqTxn())
                        .put(", durableBase=").put(durableBaseSeqTxn)
                        .put(", generationLv=").put(pin.getCoveredLvSeqTxn())
                        .put(", durableLv=").put(durableLvSeqTxn).put(']');
            }

            final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
            if (!timelineReader.floor(pin.getTimelineRootRef(), durableFrontierTimestamp, entry)) {
                throw invalid("has no root at or below durable frontier [frontier=")
                        .put(durableFrontierTimestamp).put(']');
            }
            final long effectiveLvRowPosition = deltaReader.effectivePosition(
                    pin.getRowPositionDeltaRootRef(),
                    entry
            );
            if (entry.createdLvSeqTxn > durableLvSeqTxn
                    || effectiveLvRowPosition < 0
                    || effectiveLvRowPosition > durableLvRowCount) {
                throw invalid("logical root is incompatible with durable materialization")
                        .put(" [createdLvSeqTxn=").put(entry.createdLvSeqTxn)
                        .put(", durableLvSeqTxn=").put(durableLvSeqTxn)
                        .put(", effectiveLvRowPosition=").put(effectiveLvRowPosition)
                        .put(", durableLvRowCount=").put(durableLvRowCount).put(']');
            }
            return restorePinned(
                    pin,
                    entry.maxTimestamp,
                    entry.checkpointId,
                    expectedDefinitionTxn,
                    functions,
                    anchorWindow
            );
        }
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

    private static CairoException invalid(CharSequence reason) {
        return CairoException.critical(CairoException.LV_CHECKPOINT_TIMELINE_INVALID)
                .put("live view checkpoint ").put(reason);
    }

    private void ensureOpen() {
        if (!isOpen) {
            throw CairoException.critical(0).put("live view checkpoint timeline restore reader is not open");
        }
    }

    private int findDirectoryIndex(long segmentId) {
        int lo = 0;
        int hi = segmentDirectory.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (segmentDirectory.getSegmentId(mid) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < segmentDirectory.size() && segmentDirectory.getSegmentId(lo) == segmentId ? lo : -1;
    }

    private boolean functionCatalogueContains(long segmentId) {
        int lo = 0;
        int hi = functionRoot.getSegmentUseCountSize();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (functionRoot.getSegmentId(mid) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < functionRoot.getSegmentUseCountSize() && functionRoot.getSegmentId(lo) == segmentId;
    }

    private LiveViewCheckpointDataSegmentReader openStatePage(
            @NotNull LiveViewCheckpointStatePageRef ref,
            boolean requireFunctionCatalogue
    ) {
        if (!rootCatalogueContains(ref.getSegmentId())
                || (requireFunctionCatalogue && !functionCatalogueContains(ref.getSegmentId()))) {
            throw invalid("state page segment is absent from its root catalogue, segmentId=")
                    .put(ref.getSegmentId());
        }
        final int directoryIndex = findDirectoryIndex(ref.getSegmentId());
        if (directoryIndex < 0 || segmentDirectory.getReferenceCountAt(directoryIndex) <= 0) {
            throw invalid("state page segment is absent from the published directory, segmentId=")
                    .put(ref.getSegmentId());
        }
        final LiveViewCheckpointDataSegmentReader reader = readerFor(
                ref.getSegmentId(),
                segmentDirectory.getFileLengthAt(directoryIndex)
        );
        if (ref.getStoredLength() != ref.getDecodedLength() || ref.getRowCount() != 1) {
            throw invalid("raw state page length or row count invalid");
        }
        reader.openPage(
                ref,
                requireFunctionCatalogue
                        ? LiveViewCheckpointTimelineStoreWriter.FUNCTION_STATE_PAGE_KIND
                        : LiveViewCheckpointTimelineStoreWriter.ANCHOR_STATE_PAGE_KIND,
                LiveViewCheckpointTimelineStoreWriter.RAW_CODEC,
                0,
                1,
                Integer.MAX_VALUE
        );
        reader.openStatePageReader(statePageReader);
        return reader;
    }

    private LiveViewCheckpointDataSegmentReader readerFor(long segmentId, long fileLength) {
        for (int i = 0; i < DATA_READER_CACHE_SIZE; i++) {
            if (dataSegmentIds[i] == segmentId) {
                return dataReaders[i];
            }
        }
        final int slot = dataReaderClock;
        dataReaderClock = dataReaderClock + 1 == DATA_READER_CACHE_SIZE ? 0 : dataReaderClock + 1;
        if (dataReaders[slot] == null) {
            dataReaders[slot] = new LiveViewCheckpointDataSegmentReader(configuration);
        }
        dataReaders[slot].of(checkpointsDir, segmentId, fileLength);
        dataSegmentIds[slot] = segmentId;
        return dataReaders[slot];
    }

    private Result restorePinned(
            @NotNull LiveViewCheckpointGenerationPin pin,
            long maxTimestamp,
            long checkpointId,
            long expectedDefinitionTxn,
            @NotNull ObjList<WindowFunction> functions,
            @Nullable LiveViewWindow anchorWindow
    ) {
        final LiveViewCheckpointTimelineEntry entry = new LiveViewCheckpointTimelineEntry();
        if (!timelineReader.findExact(pin.getTimelineRootRef(), maxTimestamp, checkpointId, entry)) {
            throw invalid("logical root not found [maxTimestamp=").put(maxTimestamp)
                    .put(", checkpointId=").put(checkpointId).put(']');
        }
        root.of(checkpointsDir, entry.rootRef);
        if (root.getCheckpointId() != checkpointId
                || root.getMaxTimestamp() != maxTimestamp
                || root.getDefinitionTxn() != expectedDefinitionTxn) {
            throw invalid("logical entry and checkpoint root identity mismatch");
        }
        final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
        root.getFunctionDirectoryRef(functionDirectoryRef);
        functionDirectory.of(checkpointsDir, functionDirectoryRef);
        segmentDirectory.of(checkpointsDir, pin.getSegmentDirectoryRootRef());

        validateAnchor(anchorWindow);
        validateFunctions(functions);
        restoreFunctions(functions);
        restoreAnchor(anchorWindow);

        final long effectiveLvRowPosition = deltaReader.effectivePosition(
                pin.getRowPositionDeltaRootRef(),
                entry
        );
        return new Result(
                pin.getGeneration(),
                pin.getNormalizedBaseSeqTxn(),
                pin.getCoveredLvSeqTxn(),
                entry.createdLvSeqTxn,
                entry.maxTimestamp,
                entry.checkpointId,
                effectiveLvRowPosition,
                entry.logicalStateBytes
        );
    }

    private void restoreAnchor(@Nullable LiveViewWindow anchorWindow) {
        if (anchorWindow == null) {
            return;
        }
        final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
        root.getAnchorRootRef(anchorRootRef);
        anchorRoot.of(checkpointsDir, anchorRootRef);
        final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
        anchorRoot.getStatePageRef(ref);
        openStatePage(ref, false);
        anchorWindow.restoreCheckpointState(statePageReader);
    }

    private void restoreFunction(WindowFunction function, LiveViewCheckpointPageRef functionRootRef) {
        functionRoot.of(checkpointsDir, functionRootRef);
        final int formatVersion = functionRoot.getStateFormatVersion();
        function.onCheckpointRestoreBegin();
        final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
        functionRoot.getScalarStateRef(scalarRef);
        if (!scalarRef.isNull()) {
            final LiveViewCheckpointDataSegmentReader reader = openStatePage(scalarRef, true);
            final long consumed = function.restoreCheckpointState(statePageReader, 0, null, formatVersion);
            reader.assertFullyConsumed(scalarRef.getStoredLength(), consumed, 1);
            return;
        }
        final Map map = function.getPartitionMap();
        final LiveViewCheckpointPageRef partitionRootRef = new LiveViewCheckpointPageRef();
        functionRoot.getPartitionMapRootRef(partitionRootRef);
        partitionReader.iterateAll(partitionRootRef, entry -> {
            keyMemory.jumpTo(0);
            final byte[] encodedKey = entry.getKey();
            for (int i = 0; i < encodedKey.length; i++) {
                keyMemory.putByte(encodedKey[i]);
            }
            keyPageReader.of(keyMemory, 0, encodedKey.length);
            final MapKey key = map.withKey();
            final long keyBytes = LiveViewSnapshotKeyCodec.readKey(
                    key,
                    keyPageReader,
                    0,
                    function.getCheckpointKeyColumnTypes()
            );
            if (keyBytes != encodedKey.length) {
                throw invalid("partition key decoder did not consume reference exactly");
            }
            final MapValue value = key.createValue();
            if (!value.isNew()) {
                throw invalid("function root contains a duplicate partition key");
            }
            final LiveViewCheckpointStatePageRef ref = entry.getStatePageRef(0);
            final LiveViewCheckpointDataSegmentReader reader = openStatePage(ref, true);
            final long consumed = function.restoreCheckpointState(statePageReader, 0, value, formatVersion);
            reader.assertFullyConsumed(ref.getStoredLength(), consumed, 1);
        });
    }

    private void restoreFunctions(@NotNull ObjList<WindowFunction> functions) {
        final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()) {
                continue;
            }
            functionDirectory.find(function.checkpointFunctionIdentity().getEncoded(), functionRootRef);
            restoreFunction(function, functionRootRef);
        }
    }

    private boolean rootCatalogueContains(long segmentId) {
        int lo = 0;
        int hi = root.getSegmentIdCount();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (root.getSegmentId(mid) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo < root.getSegmentIdCount() && root.getSegmentId(lo) == segmentId;
    }

    private void validateAnchor(@Nullable LiveViewWindow anchorWindow) {
        final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
        root.getAnchorRootRef(anchorRootRef);
        if ((anchorWindow == null) != anchorRootRef.isNull()) {
            throw invalid("anchor presence does not match the compiled runtime");
        }
        if (anchorWindow != null) {
            anchorRoot.of(checkpointsDir, anchorRootRef);
            final LiveViewCheckpointStatePageRef ref = new LiveViewCheckpointStatePageRef();
            anchorRoot.getStatePageRef(ref);
            openStatePage(ref, false);
            anchorWindow.validateCheckpointState(statePageReader);
        }
    }

    private void validateFunction(WindowFunction function, LiveViewCheckpointPageRef functionRootRef) {
        functionRoot.of(checkpointsDir, functionRootRef);
        final byte[] identity = function.checkpointFunctionIdentity().getEncoded();
        if (!Arrays.equals(identity, functionRoot.getFunctionIdentity())) {
            throw invalid("function directory and root identities differ");
        }
        if (!Arrays.equals(encodeKeySchema(function.getCheckpointKeyColumnTypes()), functionRoot.getKeySchema())) {
            throw invalid("function key schema does not match the compiled runtime");
        }
        final int formatVersion = functionRoot.getStateFormatVersion();
        if (formatVersion < function.checkpointStateMinSupportedVersion()
                || formatVersion > function.checkpointStateFormatVersion()) {
            throw invalid("function state format version unsupported, version=").put(formatVersion);
        }
        for (int i = 0, n = functionRoot.getSegmentUseCountSize(); i < n; i++) {
            final long segmentId = functionRoot.getSegmentId(i);
            if (!rootCatalogueContains(segmentId) || findDirectoryIndex(segmentId) < 0) {
                throw invalid("function data segment is absent from its parent root, segmentId=").put(segmentId);
            }
        }
        final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
        functionRoot.getScalarStateRef(scalarRef);
        final Map map = function.getPartitionMap();
        if ((map == null) != !scalarRef.isNull()) {
            throw invalid("function scalar/partition shape does not match the compiled runtime");
        }
        if (!scalarRef.isNull()) {
            openStatePage(scalarRef, true);
            return;
        }
        final LiveViewCheckpointPageRef partitionRootRef = new LiveViewCheckpointPageRef();
        functionRoot.getPartitionMapRootRef(partitionRootRef);
        partitionReader.iterateAll(partitionRootRef, entry -> {
            if (entry.getScalarState().length != 0 || entry.getStatePageCount() != 1) {
                throw invalid("function partition entry shape invalid");
            }
            keyMemory.jumpTo(0);
            final byte[] encodedKey = entry.getKey();
            for (int i = 0; i < encodedKey.length; i++) {
                keyMemory.putByte(encodedKey[i]);
            }
            keyPageReader.of(keyMemory, 0, encodedKey.length);
            final long consumed = LiveViewSnapshotKeyCodec.validateKey(
                    keyPageReader,
                    0,
                    function.getCheckpointKeyColumnTypes()
            );
            if (consumed != encodedKey.length) {
                throw invalid("partition key decoder did not consume reference exactly");
            }
            openStatePage(entry.getStatePageRef(0), true);
        });
    }

    private void validateFunctions(@NotNull ObjList<WindowFunction> functions) {
        int capableCount = 0;
        final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
        for (int i = 0, n = functions.size(); i < n; i++) {
            final WindowFunction function = functions.getQuick(i);
            if (!function.supportsCheckpointState()) {
                continue;
            }
            capableCount++;
            if (function.checkpointFunctionIdentity() == null || function.checkpointDependency() == null) {
                throw invalid("checkpoint-capable function has no compiler metadata");
            }
            if (!functionDirectory.find(function.checkpointFunctionIdentity().getEncoded(), functionRootRef)) {
                throw invalid("checkpoint root is missing a compiled function");
            }
            validateFunction(function, functionRootRef);
        }
        if (capableCount != functionDirectory.size()) {
            throw invalid("checkpoint function count does not match the compiled runtime");
        }
    }

    public static final class Result {
        public final long checkpointId;
        public final long coveredLvSeqTxn;
        public final long createdLvSeqTxn;
        public final long effectiveLvRowPosition;
        public final long generation;
        public final long logicalStateBytes;
        public final long maxTimestamp;
        public final long normalizedBaseSeqTxn;

        private Result(
                long generation,
                long normalizedBaseSeqTxn,
                long coveredLvSeqTxn,
                long createdLvSeqTxn,
                long maxTimestamp,
                long checkpointId,
                long effectiveLvRowPosition,
                long logicalStateBytes
        ) {
            this.generation = generation;
            this.normalizedBaseSeqTxn = normalizedBaseSeqTxn;
            this.coveredLvSeqTxn = coveredLvSeqTxn;
            this.createdLvSeqTxn = createdLvSeqTxn;
            this.maxTimestamp = maxTimestamp;
            this.checkpointId = checkpointId;
            this.effectiveLvRowPosition = effectiveLvRowPosition;
            this.logicalStateBytes = logicalStateBytes;
        }
    }
}
