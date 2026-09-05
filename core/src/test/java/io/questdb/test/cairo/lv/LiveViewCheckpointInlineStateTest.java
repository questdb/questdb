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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapReader;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineEntry;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewStatePageReader;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Coverage for a fixed-width whole-state image carried in the partition-map leaf
 * instead of in a data page the entry names with a 40-byte reference.
 * <p>
 * The entry shape is the whole of it. A leaf already distinguishes scalar bytes
 * from page references - the anchor root inlines, a RANGE ring entry inlines
 * beside its chunks - so inlining needs no new node encoding; what it needs is
 * that exactly two shapes are written and exactly two are accepted:
 * {@code (key, image, no refs)} for a function that declared its width, and
 * {@code (key, nothing, one page)} for everything else. The leaf carries no
 * length of its own for an inlined image, so the declaration is what a decoder
 * slices by, and an entry that does not match it exactly is turned away rather
 * than decoded past its state.
 * <p>
 * The cases drive {@link LiveViewCheckpointTimelineStoreWriter#append} and
 * {@link LiveViewCheckpointTimelineStoreReader#restoreLatest} directly with a
 * partitioned stub whose declared width, emitted image and per-key state each
 * case controls. No production function can be made to declare one width on one
 * seal and another on the next, which is exactly what the upgrade a real
 * deployment performs looks like from the root's point of view: a page-backed
 * predecessor written by the old binary, and a new one that inlines.
 */
public class LiveViewCheckpointInlineStateTest extends AbstractCairoTest {

    private static final long DEFINITION_TXN = 11;
    private static final long LIFECYCLE_IDENTITY = 103;
    // Fits the per-component inline budget, and is what the production
    // accumulators this step is aimed at declare.
    private static final int INLINE_STATE_BYTES = Long.BYTES;
    private static final String LV_DIR = "lv_inline_state";
    // Past MAX_INLINE_COMPONENT_STATE_BYTES, so the seal keeps the page-backed
    // shape for it however fixed the width is.
    private static final int WIDE_STATE_BYTES = LiveViewCheckpointContracts.MAX_INLINE_COMPONENT_STATE_BYTES + 8;

    @Before
    public void setUp() {
        super.setUp();
        try (Path dir = new Path(); Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(dir);
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
    }

    @Test
    public void testAnInlineEntryIsRejectedByAFunctionThatWouldSizeItDifferently() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(INLINE_STATE_BYTES, INLINE_STATE_BYTES);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.putState(1, 10);
                seal(writer, stub, 1);
            }

            // A build that widened the image would read this entry's 8 bytes as the
            // first half of a 16-byte one and take whatever follows it in the leaf as
            // the rest. The declaration is the only length there is, so a disagreement
            // has to be corruption rather than a decode.
            assertRestoreRejected(2 * INLINE_STATE_BYTES);
            // And a build that no longer declares a width at all cannot size the entry
            // by anything, so it must not try.
            assertRestoreRejected(-1);
        });
    }

    @Test
    public void testAPageBackedRootConvertsWholeOnTheNextSealAndRestores() throws Exception {
        assertMemoryLeak(() -> {
            long convertedLogicalBytes;
            try (
                    // The upgrade in miniature: one binary seals page-backed images,
                    // the next declares the same image's width and inlines it.
                    PartitionedStateStub legacy = new PartitionedStateStub(-1, INLINE_STATE_BYTES);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                legacy.putState(1, 10);
                legacy.putState(2, 20);
                legacy.putState(3, 30);
                seal(writer, legacy, 1);
                assertEntries(
                        pageBacked(1, INLINE_STATE_BYTES),
                        pageBacked(2, INLINE_STATE_BYTES),
                        pageBacked(3, INLINE_STATE_BYTES)
                );

                try (PartitionedStateStub upgraded = new PartitionedStateStub(INLINE_STATE_BYTES, INLINE_STATE_BYTES)) {
                    upgraded.putState(1, 10);
                    upgraded.putState(2, 20);
                    upgraded.putState(3, 30);
                    // A cadence seal freezes the whole live domain, so the conversion is
                    // complete at the first seal above a legacy root rather than
                    // trickling in over the keys the batch happened to touch.
                    final LiveViewCheckpointTimelineStoreWriter.Result result = seal(writer, upgraded, 2);
                    Assert.assertEquals(
                            "the converted boundary writes no data page at all",
                            0,
                            result.getDataBytesAdded()
                    );
                    convertedLogicalBytes = result.getLogicalStateBytes();
                }
            }
            assertEntries(inline(1, 10), inline(2, 20), inline(3, 30));
            assertRestores(convertedLogicalBytes, INLINE_STATE_BYTES, 1, 10, 2, 20, 3, 30);
        });
    }

    @Test
    public void testAWidthPastTheInlineBudgetKeepsItsStatePage() throws Exception {
        assertMemoryLeak(() -> {
            final long logicalBytes;
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(WIDE_STATE_BYTES, WIDE_STATE_BYTES);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.putState(1, 10);
                stub.putState(2, 20);
                // A width past the budget is a storage decision, not a malformed
                // declaration: the function is valid and simply keeps its page.
                Assert.assertFalse(LiveViewCheckpointContracts.isInlineableStateLength(WIDE_STATE_BYTES));
                final LiveViewCheckpointTimelineStoreWriter.Result result = seal(writer, stub, 1);
                logicalBytes = result.getLogicalStateBytes();
                Assert.assertTrue("a page-backed seal writes its images to data/", result.getDataBytesAdded() > 0);
            }
            assertEntries(pageBacked(1, WIDE_STATE_BYTES), pageBacked(2, WIDE_STATE_BYTES));
            assertRestores(logicalBytes, WIDE_STATE_BYTES, 1, 10, 2, 20);
        });
    }

    @Test
    public void testFixedWidthStateIsInlinedAndTheSealPublishesNoDataSegment() throws Exception {
        assertMemoryLeak(() -> {
            final long logicalBytes;
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(INLINE_STATE_BYTES, INLINE_STATE_BYTES);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.putState(1, 10);
                stub.putState(2, 20);
                stub.putState(3, 30);
                final LiveViewCheckpointTimelineStoreWriter.Result result = seal(writer, stub, 1);
                logicalBytes = result.getLogicalStateBytes();
                Assert.assertEquals("an all-inline seal writes no state page", 0, result.getDataBytesAdded());
            }
            assertEntries(inline(1, 10), inline(2, 20), inline(3, 30));
            // The reserved data segment is discarded rather than committed, so an
            // all-inline view leaves no empty file behind either.
            Assert.assertEquals("data/ must hold no segment at all", 0, dataDirBytes());
            assertRestores(logicalBytes, INLINE_STATE_BYTES, 1, 10, 2, 20, 3, 30);
        });
    }

    @Test
    public void testTheSealRejectsAnInlineImageThatMissesItsDeclaredWidth() throws Exception {
        assertMemoryLeak(() -> {
            try (
                    PartitionedStateStub stub = new PartitionedStateStub(INLINE_STATE_BYTES, INLINE_STATE_BYTES + 4);
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.putState(1, 10);
                try {
                    seal(writer, stub, 1);
                    Assert.fail("expected the declared-width mismatch to fail the seal");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "function state length does not match the declared fixed width"
                    );
                    Assert.assertEquals(
                            "an implementation defect must not be classified as recoverable corruption",
                            0,
                            e.getErrno()
                    );
                }
            }
        });
    }

    @Test
    public void testUnchangedInlineKeysStageNoPartitionPutAtAll() throws Exception {
        assertMemoryLeak(() -> {
            assertOnlyTouchedKeysArePut(INLINE_STATE_BYTES);
            // The control. The page-backed arm has always short-circuited above the
            // tree, and inlining must not quietly move the comparison down into it.
            assertOnlyTouchedKeysArePut(-1);
        });
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    /**
     * Published data-segment bytes. A segment carries payload only, so zero here
     * means the seal wrote no state page and left no file for one.
     */
    private static long dataDirBytes() {
        long bytes = 0;
        try (Path checkpointsDir = new Path(); Path dataDir = new Path()) {
            checkpointsDir(checkpointsDir);
            LiveViewCheckpointLayout.dataDirPath(dataDir, checkpointsDir);
            final File[] files = new File(dataDir.toString()).listFiles();
            if (files != null) {
                for (File file : files) {
                    if (Chars.startsWith(file.getName(), LiveViewCheckpointLayout.DATA_SEGMENT_PREFIX)
                            && !file.getName().endsWith(LiveViewCheckpointLayout.TMP_SUFFIX)) {
                        bytes += file.length();
                    }
                }
            }
        }
        return bytes;
    }

    private static Entry inline(long key, long state) {
        return new Entry(key, state, INLINE_STATE_BYTES, true);
    }

    private static Entry pageBacked(long key, int stateLength) {
        return new Entry(key, 0, stateLength, false);
    }

    /**
     * The head boundary's function-root entries, one per live key, ascending by
     * encoded key - which for a LONG key is ascending by key.
     */
    private List<Entry> headEntries() {
        final List<Entry> out = new ArrayList<>();
        try (
                Path dir = new Path();
                LiveViewCheckpointMetaStore metaStore = new LiveViewCheckpointMetaStore(configuration)
        ) {
            metaStore.of(checkpointsDir(dir));
            try (
                    LiveViewCheckpointGenerationPin pin = metaStore.pin();
                    LiveViewCheckpointTimelineReader timeline = new LiveViewCheckpointTimelineReader(configuration);
                    LiveViewCheckpointRoot root = new LiveViewCheckpointRoot(configuration);
                    LiveViewCheckpointFunctionDirectory functions = new LiveViewCheckpointFunctionDirectory(configuration);
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration);
                    LiveViewCheckpointPartitionMapReader partitions = new LiveViewCheckpointPartitionMapReader(configuration)
            ) {
                timeline.of(dir);
                partitions.of(dir);
                final LiveViewCheckpointTimelineEntry head = new LiveViewCheckpointTimelineEntry();
                Assert.assertTrue("the timeline must hold a boundary", timeline.last(pin.getTimelineRootRef(), head));
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef partitionMapRoot = new LiveViewCheckpointPageRef();
                root.of(dir, head.rootRef);
                root.getFunctionDirectoryRef(functionDirectoryRef);
                functions.of(dir, functionDirectoryRef);
                Assert.assertEquals("the stub is the sole function", 1, functions.size());
                functions.getRootRef(0, functionRootRef);
                functionRoot.of(dir, functionRootRef);
                functionRoot.getPartitionMapRootRef(partitionMapRoot);
                partitions.iterateAll(partitionMapRoot, entry -> out.add(Entry.of(entry)));
            }
        }
        return out;
    }

    private void assertEntries(Entry... expected) {
        Assert.assertEquals(Arrays.asList(expected).toString(), headEntries().toString());
    }

    /**
     * Seals three keys, then seals again with one of them moved, and asserts that
     * only the moved key reached a root builder. The short-circuit publishes
     * nothing of its own - an equal put is dropped by the partition-map writer
     * anyway - so the count is the only place the difference shows.
     */
    private void assertOnlyTouchedKeysArePut(int declaredLength) {
        try (
                PartitionedStateStub stub = new PartitionedStateStub(declaredLength, INLINE_STATE_BYTES);
                LiveViewCheckpointTimelineStoreWriter writer =
                        new LiveViewCheckpointTimelineStoreWriter(configuration)
        ) {
            stub.putState(1, 10);
            stub.putState(2, 20);
            stub.putState(3, 30);
            seal(writer, stub, 1);
            Assert.assertEquals("the first seal images every key", 3, writer.getLastBoundaryPartitionPuts());

            stub.putState(2, 21);
            seal(writer, stub, 2);
            Assert.assertEquals(
                    "declaredLength=" + declaredLength + ": only the key that moved may be put",
                    1,
                    writer.getLastBoundaryPartitionPuts()
            );

            seal(writer, stub, 3);
            Assert.assertEquals(
                    "declaredLength=" + declaredLength + ": a seal over an unmoved map puts nothing",
                    0,
                    writer.getLastBoundaryPartitionPuts()
            );
        } finally {
            // Each invocation starts from an empty timeline: the two arms write
            // incompatible entry shapes into the same function identity.
            clearCheckpointsDir();
        }
    }

    private void assertRestoreRejected(int declaredLength) {
        try (
                Path dir = new Path();
                PartitionedStateStub stub = new PartitionedStateStub(declaredLength, INLINE_STATE_BYTES);
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            reader.of(checkpointsDir(dir));
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(stub);
            try {
                reader.restoreLatest(DEFINITION_TXN, functions, null);
                Assert.fail("expected the entry shape to be rejected");
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "function partition entry shape invalid");
                Assert.assertEquals(
                        "a wrong entry shape is recoverable checkpoint corruption",
                        CairoException.LV_CHECKPOINT_TIMELINE_INVALID,
                        e.getErrno()
                );
            }
        }
    }

    /**
     * Restores the head into a fresh function and asserts it holds exactly the
     * {@code key, state} pairs given, and that the logical size it accounts for
     * what it read is the figure the seal froze.
     */
    private void assertRestores(long frozenLogicalBytes, int stateLength, long... keysAndStates) {
        try (
                Path dir = new Path();
                PartitionedStateStub restored = new PartitionedStateStub(stateLength, stateLength);
                LiveViewCheckpointTimelineStoreReader reader =
                        new LiveViewCheckpointTimelineStoreReader(configuration)
        ) {
            reader.of(checkpointsDir(dir));
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(restored);
            reader.restoreLatest(DEFINITION_TXN, functions, null);
            for (int i = 0; i < keysAndStates.length; i += 2) {
                Assert.assertEquals(
                        "key " + keysAndStates[i],
                        keysAndStates[i + 1],
                        restored.readState(keysAndStates[i])
                );
            }
            Assert.assertEquals(keysAndStates.length / 2, restored.getPartitionMap().size());
            Assert.assertEquals(
                    "restore must account for the same logical bytes the freeze charged",
                    frozenLogicalBytes,
                    restored.restoredLogicalStateBytes
            );
        }
    }

    private void clearCheckpointsDir() {
        try (Path dir = new Path(); Path path = new Path()) {
            final FilesFacade ff = configuration.getFilesFacade();
            checkpointsDir(dir);
            ff.rmdir(path.of(dir).slash());
            ff.mkdirs(LiveViewCheckpointLayout.metaDirPath(path, dir).slash(), configuration.getMkDirMode());
            ff.mkdirs(LiveViewCheckpointLayout.dataDirPath(path, dir).slash(), configuration.getMkDirMode());
        }
    }

    private LiveViewCheckpointTimelineStoreWriter.Result seal(
            LiveViewCheckpointTimelineStoreWriter writer,
            PartitionedStateStub stub,
            long seq
    ) {
        try (Path dir = new Path()) {
            checkpointsDir(dir);
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(stub);
            return writer.append(
                    dir,
                    functions,
                    null,
                    DEFINITION_TXN,
                    0,
                    seq,
                    seq,
                    0,
                    LIFECYCLE_IDENTITY,
                    true,
                    seq * 1_000_000L,
                    seq,
                    // Strictly above the previous boundary, which is what lets the
                    // seal compare a key against the entry the root below holds.
                    seq * 1_000_000L,
                    Numbers.LONG_NULL,
                    null
            );
        }
    }

    /**
     * One function-root partition entry, reduced to what the shape assertions
     * care about and rendered so a mismatch reads as a diff.
     */
    private static final class Entry {
        private final boolean isInline;
        private final long key;
        private final long state;
        private final int stateLength;

        private Entry(long key, long state, int stateLength, boolean isInline) {
            this.key = key;
            this.state = state;
            this.stateLength = stateLength;
            this.isInline = isInline;
        }

        private static Entry of(LiveViewCheckpointPartitionMapEntry entry) {
            final byte[] scalar = entry.getScalarState();
            final long key = readLong(entry.getKey(), 0);
            if (scalar.length != 0) {
                Assert.assertEquals("an inline entry names no page", 0, entry.getStatePageCount());
                return new Entry(key, readLong(scalar, 0), scalar.length, true);
            }
            Assert.assertEquals("a page-backed entry names exactly one", 1, entry.getStatePageCount());
            return new Entry(key, 0, entry.getStatePageRef(0).getDecodedLength(), false);
        }

        private static long readLong(byte[] bytes, int offset) {
            long value = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                value |= (bytes[offset + i] & 0xFFL) << (8 * i);
            }
            return value;
        }

        @Override
        public String toString() {
            return isInline
                    ? "key=" + key + " inline[" + stateLength + "]=" + state
                    : "key=" + key + " page[" + stateLength + "]";
        }
    }

    /**
     * A partitioned whole-state function over a LONG partition key and one LONG
     * of state. The declared width and the emitted image length are independent,
     * so one case can inline, one can stay page-backed, and one can declare a
     * width it does not keep.
     */
    private static final class PartitionedStateStub extends BaseWindowFunction {
        private static final ColumnTypes KEY_TYPES = new SingleColumnType(ColumnType.LONG);
        private final int declaredLength;
        private final int emittedLength;
        private final Map map;
        private long restoredLogicalStateBytes = Numbers.LONG_NULL;

        private PartitionedStateStub(int declaredLength, int emittedLength) {
            super(null);
            this.declaredLength = declaredLength;
            this.emittedLength = emittedLength;
            this.map = new OrderedMap(
                    1024,
                    KEY_TYPES,
                    new SingleColumnType(ColumnType.LONG),
                    16,
                    0.7,
                    8
            );
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "partitioned_state_stub()",
                            0,
                            "k",
                            "ts asc",
                            "partitioned-state-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.FIXED_ANCHOR_SEGMENT,
                            "k",
                            "ts asc",
                            0,
                            0,
                            0,
                            ColumnType.TIMESTAMP,
                            false,
                            false,
                            false,
                            LiveViewCheckpointDependency.StructuralConvergence.EXACT,
                            LiveViewCheckpointDependency.NumericConvergence.EXACT
                    )
            );
        }

        @Override
        public int checkpointStateFixedLength() {
            return declaredLength;
        }

        @Override
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            final long state = value.getLong(0);
            for (int i = 0; i < emittedLength; i++) {
                sink.putByte((byte) (state >>> (8 * (i % Long.BYTES))));
            }
        }

        @Override
        public ColumnTypes getCheckpointKeyColumnTypes() {
            return KEY_TYPES;
        }

        @Override
        public int getCheckpointKeyStartIndex() {
            // [value0, key0]: one value slot ahead of the key.
            return 1;
        }

        @Override
        public String getName() {
            return "partitioned_state_stub";
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void onCheckpointPersisted(long logicalStateBytes, long generation) {
            restoredLogicalStateBytes = logicalStateBytes;
        }

        @Override
        public void onCheckpointRestoreBegin() {
            map.clear();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public long restoreCheckpointState(LiveViewStatePageReader source, long offset, MapValue value) {
            long state = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                state |= (source.getByte(offset + i) & 0xFFL) << (8 * i);
            }
            value.putLong(0, state);
            return emittedLength;
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }

        private void putState(long key, long state) {
            final MapKey mapKey = map.withKey();
            mapKey.putLong(key);
            mapKey.createValue().putLong(0, state);
        }

        private long readState(long key) {
            final MapKey mapKey = map.withKey();
            mapKey.putLong(key);
            final MapValue value = mapKey.findValue();
            Assert.assertNotNull("restored map must hold key " + key, value);
            return value.getLong(0);
        }
    }
}
