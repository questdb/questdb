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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointContracts;
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentReader;
import io.questdb.cairo.lv.LiveViewCheckpointDependency;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionDirectory;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointGenerationPin;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaStore;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointRoot;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineReader;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreWriter;
import io.questdb.cairo.lv.LiveViewStatePageWriter;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.FilesFacade;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Standalone coverage for the scalar (map-less) whole-state page across seals.
 * A scalar function freezes one state image per boundary and the seal reuses
 * the previous boundary's page when the image has not moved; what is under test
 * here are the two fallbacks that must publish a fresh page instead:
 * <ul>
 *     <li>a predecessor whose stored length differs from the current encode -
 *     the comparison is skipped outright, since the images cannot be equal;</li>
 *     <li>a predecessor page that cannot be read - the seal pays the bytes for
 *     a fresh image rather than failing the publication, and the elision
 *     resumes against the fresh page at the next seal.</li>
 * </ul>
 * The seals run through {@link LiveViewCheckpointTimelineStoreWriter#append}
 * with a stub scalar function whose image bytes each case controls exactly,
 * which is what lets a case change the image's length or keep its bytes
 * identical at will - no production function varies its image length within
 * one definition.
 */
public class LiveViewCheckpointScalarStatePageTest extends AbstractCairoTest {

    private static final long DEFINITION_TXN = 7;
    private static final long LIFECYCLE_IDENTITY = 102;
    private static final String LV_DIR = "lv_scalar_state_page";

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
    public void testScalarStateLengthChangeRepublishesAFreshPage() throws Exception {
        assertMemoryLeak(() -> {
            final byte[] shortImage = filled(16, (byte) 0xA1);
            final byte[] longImage = filled(24, (byte) 0xB2);
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.state = shortImage;
                seal(writer, stub, 1);
                stub.state = shortImage.clone();
                seal(writer, stub, 2);
                stub.state = longImage;
                seal(writer, stub, 3);
                stub.state = longImage.clone();
                seal(writer, stub, 4);
            }

            final List<LiveViewCheckpointStatePageRef> refs = scalarRefs();
            Assert.assertEquals(4, refs.size());
            // The baseline: an unchanged image reuses the previous boundary's page.
            assertSamePage(refs.get(0), refs.get(1));
            assertPageHolds(refs.get(0), shortImage);

            // A predecessor of a different length cannot hold an equal image, so
            // the seal publishes a fresh page carrying the new state.
            final LiveViewCheckpointStatePageRef fresh = refs.get(2);
            Assert.assertEquals(longImage.length, fresh.getStoredLength());
            Assert.assertNotEquals(refs.get(1).getSegmentId(), fresh.getSegmentId());
            assertPageHolds(fresh, longImage);

            // The elision resumes against the fresh page.
            assertSamePage(fresh, refs.get(3));
        });
    }

    @Test
    public void testScalarStateUnreadablePredecessorSealsFreshState() throws Exception {
        assertMemoryLeak(() -> {
            final byte[] image = filled(32, (byte) 0xC3);
            try (
                    ScalarStateStub stub = new ScalarStateStub();
                    LiveViewCheckpointTimelineStoreWriter writer =
                            new LiveViewCheckpointTimelineStoreWriter(configuration)
            ) {
                stub.state = image;
                seal(writer, stub, 1);
                stub.state = image.clone();
                seal(writer, stub, 2);

                final List<LiveViewCheckpointStatePageRef> published = scalarRefs();
                Assert.assertEquals(2, published.size());
                assertSamePage(published.get(0), published.get(1));
                assertPageHolds(published.get(0), image);

                // Unlink the data segment holding the head's scalar page. The next
                // seal's image is byte-identical, so only the comparison read stands
                // between it and a reuse - and that read must fail into a fresh
                // page, not fail the publication.
                try (Path dir = new Path(); Path seg = new Path()) {
                    checkpointsDir(dir);
                    LiveViewCheckpointLayout.dataSegmentPath(seg, dir, published.get(1).getSegmentId());
                    Assert.assertTrue(configuration.getFilesFacade().removeQuiet(seg.$()));
                }
                stub.state = image.clone();
                seal(writer, stub, 3);
                stub.state = image.clone();
                seal(writer, stub, 4);
            }

            final List<LiveViewCheckpointStatePageRef> refs = scalarRefs();
            Assert.assertEquals(4, refs.size());
            // The boundary sealed over the unreadable predecessor carries fresh
            // state in a segment of its own.
            final LiveViewCheckpointStatePageRef fresh = refs.get(2);
            Assert.assertNotEquals(refs.get(1).getSegmentId(), fresh.getSegmentId());
            Assert.assertEquals(image.length, fresh.getStoredLength());
            assertPageHolds(fresh, image);

            // The unreadable predecessor cost one image and nothing else: the next
            // seal reuses the fresh page, so the reader machinery came through the
            // failed open able to answer later comparisons.
            assertSamePage(fresh, refs.get(3));
        });
    }

    private static void assertSamePage(LiveViewCheckpointStatePageRef expected, LiveViewCheckpointStatePageRef actual) {
        Assert.assertEquals("segmentId", expected.getSegmentId(), actual.getSegmentId());
        Assert.assertEquals("offset", expected.getOffset(), actual.getOffset());
        Assert.assertEquals("storedLength", expected.getStoredLength(), actual.getStoredLength());
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    private static LiveViewCheckpointStatePageRef copyOf(LiveViewCheckpointStatePageRef source) {
        return new LiveViewCheckpointStatePageRef().of(
                source.getSegmentId(),
                source.getOffset(),
                source.getStoredLength(),
                source.getDecodedLength(),
                source.getPageKind(),
                source.getCodec(),
                source.getRowCount(),
                source.getFlags()
        );
    }

    private static byte[] filled(int length, byte value) {
        final byte[] bytes = new byte[length];
        java.util.Arrays.fill(bytes, value);
        return bytes;
    }

    private void assertPageHolds(LiveViewCheckpointStatePageRef ref, byte[] expected) {
        try (
                Path dir = new Path();
                Path seg = new Path();
                LiveViewCheckpointDataSegmentReader reader = new LiveViewCheckpointDataSegmentReader(configuration)
        ) {
            checkpointsDir(dir);
            LiveViewCheckpointLayout.dataSegmentPath(seg, dir, ref.getSegmentId());
            final long fileLength = configuration.getFilesFacade().length(seg.$());
            reader.of(dir, ref.getSegmentId(), fileLength);
            reader.openPage(
                    ref,
                    LiveViewCheckpointTimelineStoreWriter.FUNCTION_STATE_PAGE_KIND,
                    LiveViewCheckpointTimelineStoreWriter.RAW_CODEC,
                    0,
                    1,
                    Integer.MAX_VALUE
            );
            Assert.assertEquals(expected.length, reader.getPageStoredLength());
            final long address = reader.getPageAddress();
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals("byte " + i, expected[i], Unsafe.getUnsafe().getByte(address + i));
            }
        }
    }

    /**
     * The scalar page each published boundary names, ascending by boundary.
     */
    private List<LiveViewCheckpointStatePageRef> scalarRefs() {
        final List<LiveViewCheckpointStatePageRef> out = new ArrayList<>();
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
                    LiveViewCheckpointFunctionRoot functionRoot = new LiveViewCheckpointFunctionRoot(configuration)
            ) {
                timeline.of(dir);
                final LiveViewCheckpointPageRef functionDirectoryRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointPageRef functionRootRef = new LiveViewCheckpointPageRef();
                final LiveViewCheckpointStatePageRef scalarRef = new LiveViewCheckpointStatePageRef();
                timeline.iterateAll(pin.getTimelineRootRef(), entry -> {
                    root.of(dir, entry.rootRef);
                    root.getFunctionDirectoryRef(functionDirectoryRef);
                    functions.of(dir, functionDirectoryRef);
                    Assert.assertEquals("the stub is the sole function", 1, functions.size());
                    functions.getRootRef(0, functionRootRef);
                    functionRoot.of(dir, functionRootRef);
                    functionRoot.getScalarStateRef(scalarRef);
                    Assert.assertFalse("a scalar boundary names a state page", scalarRef.isNull());
                    out.add(copyOf(scalarRef));
                });
            }
        }
        return out;
    }

    private void seal(LiveViewCheckpointTimelineStoreWriter writer, ScalarStateStub stub, long seq) {
        try (Path dir = new Path()) {
            checkpointsDir(dir);
            final ObjList<WindowFunction> functions = new ObjList<>();
            functions.add(stub);
            writer.append(
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
                    seq * 1_000_000L,
                    Numbers.LONG_NULL,
                    null,
                    null
            );
        }
    }

    /**
     * A scalar (map-less) whole-state function whose frozen image is exactly
     * {@link #state}, so a case controls the image's bytes and length per seal.
     */
    private static final class ScalarStateStub extends BaseWindowFunction {
        private byte[] state;

        private ScalarStateStub() {
            super(null);
            setCheckpointCompilerMetadata(
                    new LiveViewCheckpointFunctionIdentity(
                            "w0",
                            "scalar_state_stub()",
                            0,
                            "",
                            "ts asc",
                            "scalar-state-stub-v1"
                    ),
                    new LiveViewCheckpointDependency(
                            LiveViewCheckpointContracts.DependencyKind.UNBOUNDED_CUMULATIVE_NO_RESET,
                            "",
                            "ts asc",
                            Long.MIN_VALUE,
                            0,
                            Long.MIN_VALUE,
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
        public int checkpointStateFormatVersion() {
            return 1;
        }

        @Override
        public void freezeCheckpointState(LiveViewStatePageWriter sink, MapValue value) {
            for (byte b : state) {
                sink.putByte(b);
            }
        }

        @Override
        public String getName() {
            return "scalar_state_stub";
        }

        @Override
        public int getType() {
            return ColumnType.LONG;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }

        @Override
        public boolean supportsCheckpointState() {
            return true;
        }
    }
}
