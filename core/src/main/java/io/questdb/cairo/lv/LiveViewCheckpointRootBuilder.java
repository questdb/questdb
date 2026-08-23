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
import io.questdb.std.LongList;
import io.questdb.std.Misc;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Builds a checkpoint root and sorted function directory. Unchanged functions
 * are represented by their existing immutable function-root references; only
 * the small directory and checkpoint root are rewritten for an adjacent seal.
 * <p>
 * The root also states the segments its whole closure names - the data segments
 * its functions' state pages sit in, and the metadata segments holding its own
 * page, its function directory, and every anchor-root, function-root and
 * partition-map page below them. Both halves come from the subordinate roots'
 * own per-segment counts rather than a walk, and both are read the same way when
 * a boundary is written or retired: a repair splice or a truncate hands the union
 * to the catalogue as one reference transaction, so a segment retires exactly
 * when the last boundary naming it does.
 */
public class LiveViewCheckpointRootBuilder implements Closeable {

    private final LiveViewCheckpointAnchorRoot anchorRoot;
    private final LiveViewCheckpointPageRef anchorRootRef = new LiveViewCheckpointPageRef();
    private long checkpointId;
    private final Path checkpointsDir = new Path();
    private long definitionTxn;
    private byte[][] functionIdentities = new byte[8][];
    private int functionCount;
    private final LiveViewCheckpointFunctionRoot functionRoot;
    private LiveViewCheckpointPageRef[] functionRootRefs = new LiveViewCheckpointPageRef[8];
    private boolean initialized;
    private long lastSegmentBytes;
    private long maxTimestamp;
    private final LiveViewCheckpointRoot resultRoot;
    private final LongList segmentIds = new LongList();
    private final LiveViewCheckpointMetaSegmentWriter segmentWriter;

    public LiveViewCheckpointRootBuilder(@NotNull CairoConfiguration configuration) {
        anchorRoot = new LiveViewCheckpointAnchorRoot(configuration);
        functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
        resultRoot = new LiveViewCheckpointRoot(configuration);
        segmentWriter = new LiveViewCheckpointMetaSegmentWriter(configuration);
    }

    public void addFunction(@NotNull LiveViewCheckpointPageRef functionRootRef) {
        ensureInitialized();
        LiveViewCheckpointMetadata.validateMetaRef(functionRootRef, false, "function root");
        functionRoot.of(checkpointsDir, functionRootRef);
        ensureFunctionCapacity(functionCount + 1);
        functionIdentities[functionCount] = Arrays.copyOf(
                functionRoot.getFunctionIdentity(),
                functionRoot.getFunctionIdentity().length
        );
        this.functionRootRefs[functionCount] = new LiveViewCheckpointPageRef().of(
                functionRootRef.getSegmentId(), functionRootRef.getOffset(), functionRootRef.getLength()
        );
        functionCount++;
        for (int i = 0; i < functionRoot.getSegmentUseCountSize(); i++) {
            addSegmentId(functionRoot.getSegmentId(i));
        }
    }

    /**
     * Starts one checkpoint root. The anchor root reference may be null when the
     * live view has no anchored WINDOW; it contributes no data segment, because
     * an anchor entry's whole state is scalar metadata inside its own map pages.
     * It does contribute metadata segments - its own page and the anchor-map pages
     * below it, which older seals may have written - so a non-null reference is
     * read here for the set it names.
     */
    public void begin(
            @Transient @NotNull Path checkpointsDir,
            long checkpointId,
            long maxTimestamp,
            long definitionTxn,
            @NotNull LiveViewCheckpointPageRef anchorRootRef
    ) {
        initialized = false;
        if (checkpointId < 0 || definitionTxn < 0) {
            throw CairoException.critical(0).put("live view checkpoint root identity invalid");
        }
        LiveViewCheckpointMetadata.validateMetaRef(anchorRootRef, true, "anchor root");
        this.checkpointsDir.of(checkpointsDir);
        this.checkpointId = checkpointId;
        this.maxTimestamp = maxTimestamp;
        this.definitionTxn = definitionTxn;
        this.anchorRootRef.of(anchorRootRef.getSegmentId(), anchorRootRef.getOffset(), anchorRootRef.getLength());
        functionCount = 0;
        segmentIds.clear();
        if (!anchorRootRef.isNull()) {
            anchorRoot.of(checkpointsDir, anchorRootRef);
            for (int i = 0, n = anchorRoot.getSegmentUseCountSize(); i < n; i++) {
                addSegmentId(anchorRoot.getSegmentId(i));
            }
        }
        initialized = true;
    }

    /**
     * Writes the root and its function directory, and reports the page reference naming it.
     * <p>
     * An empty function directory is a legitimate root rather than a lost one: a view whose
     * every window function is stateless has no state to image, and what the root still
     * carries - the boundary's timestamp, its {@code lvSeqTxn} and its segment catalogue - is
     * what a resume and a restart read off it.
     */
    public void build(long metadataSegmentId, @NotNull LiveViewCheckpointPageRef out) {
        ensureInitialized();
        sortFunctions();
        for (int i = 1; i < functionCount; i++) {
            if (LiveViewCheckpointMetadata.compareBytes(functionIdentities[i - 1], functionIdentities[i]) == 0) {
                throw CairoException.critical(0).put("duplicate live view checkpoint function identity");
            }
        }
        // The root page and its function directory land here, so this segment is
        // part of the boundary's closure as much as the ones below it are.
        addSegmentId(metadataSegmentId);
        segmentWriter.of(checkpointsDir, metadataSegmentId);
        final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
        LiveViewCheckpointFunctionDirectory.writeTo(
                functionIdentities,
                functionRootRefs,
                functionCount,
                segmentWriter,
                functionDirectoryRef
        );
        resultRoot.ofBuilder(
                checkpointId,
                maxTimestamp,
                definitionTxn,
                anchorRootRef,
                functionDirectoryRef,
                segmentIds
        );
        resultRoot.writeTo(segmentWriter, out);
        lastSegmentBytes = segmentWriter.commit();
    }

    @Override
    public void close() {
        Misc.free(anchorRoot);
        Misc.free(functionRoot);
        Misc.free(resultRoot);
        Misc.free(segmentWriter);
        Misc.free(checkpointsDir);
    }

    public void getReferencedSegmentIds(@NotNull LongList out) {
        out.clear();
        out.add(segmentIds);
    }

    public long getLastSegmentBytes() {
        return lastSegmentBytes;
    }

    private void addSegmentId(long segmentId) {
        int lo = 0;
        int hi = segmentIds.size();
        while (lo < hi) {
            final int mid = (lo + hi) >>> 1;
            if (segmentIds.getQuick(mid) < segmentId) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == segmentIds.size() || segmentIds.getQuick(lo) != segmentId) {
            segmentIds.add(lo, segmentId);
        }
    }

    private void ensureFunctionCapacity(int capacity) {
        if (capacity <= functionIdentities.length) {
            return;
        }
        final int newCapacity = functionIdentities.length * 2;
        functionIdentities = Arrays.copyOf(functionIdentities, newCapacity);
        functionRootRefs = Arrays.copyOf(functionRootRefs, newCapacity);
    }

    private void ensureInitialized() {
        if (!initialized) {
            throw CairoException.critical(0).put("live view checkpoint root builder is not initialized");
        }
    }

    private void sortFunctions() {
        for (int i = 1; i < functionCount; i++) {
            final byte[] identity = functionIdentities[i];
            final LiveViewCheckpointPageRef ref = functionRootRefs[i];
            int j = i;
            while (j > 0 && LiveViewCheckpointMetadata.compareBytes(functionIdentities[j - 1], identity) > 0) {
                functionIdentities[j] = functionIdentities[j - 1];
                functionRootRefs[j] = functionRootRefs[j - 1];
                j--;
            }
            functionIdentities[j] = identity;
            functionRootRefs[j] = ref;
        }
    }
}
