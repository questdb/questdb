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

import io.questdb.PropertyKey;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionIdentity;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRoot;
import io.questdb.cairo.lv.LiveViewCheckpointFunctionRootBuilder;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointMetaSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.lv.LiveViewCheckpointTimelineStoreReader;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRoot;
import io.questdb.cairo.lv.LiveViewCheckpointWindowRootBuilder;
import io.questdb.cairo.lv.LiveViewInstance;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.lv.LiveViewWindow;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Asserts that the compiled encodings a checkpoint writes - the window name, the key schemas,
 * the function identity, the window identity and the state manifest - keep the exact backing
 * arrays their owners compiled once, and that builders and result roots borrow those arrays
 * rather than cloning them.
 * <p>
 * Backing identity is an {@code ==} property, so every comparison here is a reference check.
 * The owners lend their backing arrays through package-private {@code borrow*} accessors, and the
 * builders and result roots take borrowed arrays through package-private
 * {@code ofBorrowedCompiled} / {@code ofBuilder} methods. Package-private visibility keeps those
 * lending and borrowing members off the package's API, so this test reads the backing arrays
 * through the owners' {@code @TestOnly borrow*ForTest} accessors and drives the borrowing entry
 * points through their {@code @TestOnly ...ForTest} wrappers, which return or take the very arrays
 * the package-private members do. {@code isBorrowingCompiledForTest} reports what a builder or a
 * root holds.
 */
public class LiveViewCompiledEncodingIdentityTest extends AbstractLiveViewTest {

    @Before
    public void setUpCheckpointCadence() {
        setProperty(PropertyKey.CAIRO_LIVE_VIEW_CHECKPOINT_ROWS, 1);
        setCurrentMicros(0);
    }

    @Test
    public void testBuildersAndResultRootsBorrowThenClearAcrossFailureAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (Path dir = new Path(); Path metaDir = new Path()) {
                dir.of(configuration.getDbRoot()).concat("lv_compiled_encoding_identity_checkpoints");
                metaDir.of(dir).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
                configuration.getFilesFacade().mkdirs(metaDir, configuration.getMkDirMode());
                assertBuilderFailureReuse(configuration, dir);
            }
        });
    }

    @Test
    public void testCompiledOwnersKeepExactBackingAcrossFreezeAndRestore() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table tx (ts timestamp, account symbol, amount double) timestamp(ts) partition by day wal");
            execute("create live view lv_a flush every 100ms start from beginning as "
                    + "select ts, account, sum(amount) over window_a s from tx "
                    + "window window_a as (partition by account order by ts anchor daily '00:00')");
            execute("create live view lv_b flush every 100ms start from beginning as "
                    + "select ts, amount, count() over window_b c from tx "
                    + "window window_b as (partition by amount order by ts anchor daily '00:00')");

            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                driveSeedToCompletion(job, "lv_a");
                driveSeedToCompletion(job, "lv_b");
                insert(job, "2026-01-01T00:00:01.000000Z", "acct-a", 1.0);

                final LiveViewInstance a = instance("lv_a");
                final LiveViewInstance b = instance("lv_b");
                final Snapshot aBytes = capture(a);
                final Snapshot bBytes = capture(b);
                assertDistinct(aBytes, bBytes);

                insert(job, "2026-01-01T00:00:02.000000Z", "acct-b", 2.0);
                assertSameOwners(aBytes, a, true);
                assertSameOwners(bBytes, b, true);
                restoreTwice(a);
                restoreTwice(b);
                assertSameOwners(aBytes, a, true);
                assertSameOwners(bBytes, b, true);

                Assert.assertFalse(a.getAnchorWindow().bindCheckpointWindowStatePlan(null));
                insert(job, "2026-01-01T00:00:03.000000Z", "acct-a", 3.0);
                assertSameOwners(aBytes, a, false);
                restoreTwice(a);
                assertSameOwners(aBytes, a, false);
            }
            assertMalformedUnicodeGoldenBytesAndEmptySchemaSingleton(configuration);
        });
    }

    private static void assertBuilderFailureReuse(CairoConfiguration configuration, Path dir) {
        final byte[] invalidSchema = schema(ColumnType.INT);
        final byte[] invalidIdentity = new byte[]{9};
        final byte[] invalidManifest = new byte[]{8};
        final byte[] schemaA = schema(ColumnType.STRING);
        final byte[] schemaB = schema(ColumnType.DOUBLE);
        final byte[] identityA = new byte[]{1};
        final byte[] identityB = new byte[]{2};
        final byte[] manifestA = new byte[]{3};
        final byte[] manifestB = new byte[]{4};
        final LiveViewCheckpointPageRef nullMetaRef = new LiveViewCheckpointPageRef();
        final LiveViewCheckpointStatePageRef nullStateRef = new LiveViewCheckpointStatePageRef();
        final LiveViewCheckpointPageRef invalidMetaRef = new LiveViewCheckpointPageRef().of(
                999_999,
                0,
                LiveViewCheckpointLayout.PAGE_HEADER_SIZE
        );
        final LongList noSegments = new LongList();

        try (LiveViewCheckpointFunctionRootBuilder builder = new LiveViewCheckpointFunctionRootBuilder(configuration);
             LiveViewCheckpointMetaSegmentWriter unopened = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
            expectInitializerFailure("function root metadata page reference invalid", () ->
                    builder.ofBorrowedCompiledForTest(dir, invalidMetaRef, invalidIdentity, 1, invalidSchema));
            Assert.assertFalse("function builder retained invalid-predecessor bytes",
                    builder.isBorrowingCompiledForTest(invalidIdentity, invalidSchema));
            builder.ofBorrowedCompiledForTest(dir, nullMetaRef, identityB, 1, schemaB);
            Assert.assertTrue("function builder did not reuse after invalid predecessor",
                    builder.isBorrowingCompiledForTest(identityB, schemaB));
            builder.ofBorrowedCompiledForTest(dir, nullMetaRef, identityA, 1, schemaA);
            Assert.assertTrue("function builder cloned compiled bytes", builder.isBorrowingCompiledForTest(identityA, schemaA));
            Assert.assertFalse("function builder borrows its key schema with its identity",
                    builder.isBorrowingCompiledForTest(identityA, schemaB));
            expectBuildFailure(() -> builder.buildIntoOpenSegment(78, unopened, new LiveViewCheckpointPageRef()));
            Assert.assertFalse("function builder retained failed-view bytes", builder.isBorrowingCompiledForTest(identityA, schemaA));
            builder.ofBorrowedCompiledForTest(dir, nullMetaRef, identityB, 1, schemaB);
            Assert.assertTrue("function builder did not borrow replacement bytes", builder.isBorrowingCompiledForTest(identityB, schemaB));
            Assert.assertFalse("function builder leaked prior-view bytes", builder.isBorrowingCompiledForTest(identityA, schemaA));
        }

        try (LiveViewCheckpointWindowRootBuilder builder = new LiveViewCheckpointWindowRootBuilder(configuration);
             LiveViewCheckpointMetaSegmentWriter unopened = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
            expectInitializerFailure("window state root metadata page reference invalid", () -> builder.ofBorrowedCompiledForTest(
                    dir, invalidMetaRef, invalidIdentity, ColumnType.TIMESTAMP_MICRO,
                    invalidSchema, invalidManifest, 16, true, null
            ));
            Assert.assertFalse("window builder retained invalid-predecessor bytes",
                    builder.isBorrowingCompiledForTest(invalidIdentity, invalidSchema, invalidManifest));
            builder.ofBorrowedCompiledForTest(
                    dir, nullMetaRef, identityB, ColumnType.TIMESTAMP_MICRO, schemaB, manifestB, 16, true, null
            );
            Assert.assertTrue("window builder did not reuse after invalid predecessor",
                    builder.isBorrowingCompiledForTest(identityB, schemaB, manifestB));
            builder.ofBorrowedCompiledForTest(dir, nullMetaRef, identityA, ColumnType.TIMESTAMP_MICRO, schemaA, manifestA, 16, true, null);
            Assert.assertTrue("window builder cloned compiled bytes", builder.isBorrowingCompiledForTest(identityA, schemaA, manifestA));
            Assert.assertFalse("window builder borrows its key schema with its identity",
                    builder.isBorrowingCompiledForTest(identityA, schemaB, manifestA));
            Assert.assertFalse("window builder borrows its manifest with its identity and key schema",
                    builder.isBorrowingCompiledForTest(identityA, schemaA, manifestB));
            expectBuildFailure(() -> builder.buildIntoOpenSegment(79, unopened, new LiveViewCheckpointPageRef()));
            Assert.assertFalse("window builder retained failed-view bytes", builder.isBorrowingCompiledForTest(identityA, schemaA, manifestA));
            builder.ofBorrowedCompiledForTest(dir, nullMetaRef, identityB, ColumnType.TIMESTAMP_MICRO, schemaB, manifestB, 16, true, null);
            Assert.assertTrue("window builder did not borrow replacement bytes", builder.isBorrowingCompiledForTest(identityB, schemaB, manifestB));
            Assert.assertFalse("window builder leaked prior-view bytes", builder.isBorrowingCompiledForTest(identityA, schemaA, manifestA));
        }

        final LiveViewCheckpointPageRef functionPredecessor = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointFunctionRootBuilder builder = new LiveViewCheckpointFunctionRootBuilder(configuration)) {
            builder.ofBorrowedCompiledForTest(dir, nullMetaRef, identityA, 1, schemaA);
            builder.build(81, functionPredecessor);
            expectInitializerFailure("function root identity or schema mismatch", () ->
                    builder.ofBorrowedCompiledForTest(dir, functionPredecessor, identityB, 1, schemaB));
            Assert.assertFalse("function builder retained semantic-mismatch bytes",
                    builder.isBorrowingCompiledForTest(identityB, schemaB));
            builder.ofBorrowedCompiledForTest(dir, functionPredecessor, identityA, 1, schemaA);
            Assert.assertTrue("function builder did not reuse after semantic mismatch",
                    builder.isBorrowingCompiledForTest(identityA, schemaA));
        }

        try (LiveViewCheckpointFunctionRoot root = new LiveViewCheckpointFunctionRoot(configuration)) {
            root.ofBuilderForTest(identityA, 1, schemaA, nullStateRef, nullMetaRef, noSegments);
            Assert.assertTrue("function result root cloned compiled bytes", root.isBorrowingCompiledForTest(identityA, schemaA));
            Assert.assertFalse("function result root borrows its key schema with its identity",
                    root.isBorrowingCompiledForTest(identityA, schemaB));
            root.clearBorrowedCompiledForTest();
            Assert.assertNull("function result root kept its identity past the clear", root.getFunctionIdentity());
            Assert.assertNull("function result root kept its key schema past the clear", root.getKeySchema());
            Assert.assertFalse("function result root kept borrowed bytes past the clear",
                    root.isBorrowingCompiledForTest(identityA, schemaA));
            root.ofBuilderForTest(identityB, 1, schemaB, nullStateRef, nullMetaRef, noSegments);
            Assert.assertTrue("function result root did not replace bytes", root.isBorrowingCompiledForTest(identityB, schemaB));
        }
        try (LiveViewCheckpointWindowRoot root = new LiveViewCheckpointWindowRoot(configuration)) {
            root.ofBuilderForTest(identityA, ColumnType.TIMESTAMP_MICRO, schemaA, manifestA, 16, nullMetaRef, noSegments);
            Assert.assertTrue("window result root cloned compiled bytes", root.isBorrowingCompiledForTest(identityA, schemaA, manifestA));
            Assert.assertFalse("window result root borrows its key schema with its identity",
                    root.isBorrowingCompiledForTest(identityA, schemaB, manifestA));
            Assert.assertFalse("window result root borrows its manifest with its identity and key schema",
                    root.isBorrowingCompiledForTest(identityA, schemaA, manifestB));
            root.clearBorrowedCompiledForTest();
            Assert.assertNull("window result root kept its identity past the clear", root.getWindowIdentity());
            Assert.assertNull("window result root kept its key schema past the clear", root.getKeySchema());
            Assert.assertNull("window result root kept its manifest past the clear", root.getManifest());
            Assert.assertFalse("window result root kept borrowed bytes past the clear",
                    root.isBorrowingCompiledForTest(identityA, schemaA, manifestA));
            root.ofBuilderForTest(identityB, ColumnType.TIMESTAMP_MICRO, schemaB, manifestB, 16, nullMetaRef, noSegments);
            Assert.assertTrue("window result root did not replace bytes", root.isBorrowingCompiledForTest(identityB, schemaB, manifestB));
        }
    }

    private static void assertDistinct(Snapshot left, Snapshot right) {
        Assert.assertNotSame("alternating views share window-name backing", left.windowName, right.windowName);
        Assert.assertNotSame("alternating views share nonempty schema backing", left.windowSchema, right.windowSchema);
        Assert.assertNotSame("alternating views share function identity backing", left.functionIdentity, right.functionIdentity);
        Assert.assertNotSame("alternating views share function schema backing", left.functionSchema, right.functionSchema);
        Assert.assertNotSame("alternating views share window identity backing", left.windowIdentity, right.windowIdentity);
        Assert.assertNotSame("alternating views share manifest backing", left.manifest, right.manifest);
        Assert.assertFalse("alternating views have equal window names", Arrays.equals(left.windowName, right.windowName));
        Assert.assertFalse("alternating views have equal schemas", Arrays.equals(left.windowSchema, right.windowSchema));
    }

    private static void assertMalformedUnicodeGoldenBytesAndEmptySchemaSingleton(CairoConfiguration configuration) {
        final String malformedName = "H\ud800L\udc00P\ud83d\ude00Z";
        final byte[] expectedUtf8 = new byte[]{
                'H', '?', 'L', '?', 'P', (byte) 0xf0, (byte) 0x9f, (byte) 0x98, (byte) 0x80, 'Z'
        };
        final LiveViewWindow window = new LiveViewWindow(
                configuration,
                malformedName,
                null,
                ColumnType.TIMESTAMP_MICRO,
                new ArrayColumnTypes(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new ObjList<>(),
                false,
                null,
                null
        );
        Assert.assertArrayEquals("malformed window name UTF-8 bytes changed",
                expectedUtf8, window.borrowCheckpointWindowNameUtf8ForTest());
        final LiveViewCheckpointFunctionIdentity malformed = new LiveViewCheckpointFunctionIdentity(
                malformedName, "f", 0, "", "o", "c", null
        );
        final LiveViewCheckpointFunctionIdentity emptyAgain = new LiveViewCheckpointFunctionIdentity(
                malformedName, "g", 1, "", "o", "c", null
        );
        final byte[] expectedIdentity = new byte[]{
                0x4c, 0x56, 0x46, 0x49,
                0, 0, 0, 1,
                0, 0, 0, 0,
                0, 0, 0, 10,
                'H', '?', 'L', '?', 'P', (byte) 0xf0, (byte) 0x9f, (byte) 0x98, (byte) 0x80, 'Z',
                0, 0, 0, 1, 'f',
                0, 0, 0, 0,
                0, 0, 0, 1, 'o',
                0, 0, 0, 1, 'c'
        };
        Assert.assertArrayEquals("malformed function identity bytes changed",
                expectedIdentity, malformed.borrowEncodedForTest());
        Assert.assertSame("malformed Unicode identity was recreated",
                malformed.borrowEncodedForTest(), malformed.borrowEncodedForTest());
        Assert.assertNotSame("public function identity is not defensive",
                malformed.borrowEncodedForTest(), malformed.getEncoded());
        Assert.assertSame("empty schemas do not share singleton",
                malformed.borrowEncodedKeySchemaForTest(), emptyAgain.borrowEncodedKeySchemaForTest());
        // An empty key list encodes as a zero column count, the one array every empty key
        // schema shares, whether a window or a function identity encodes it.
        Assert.assertArrayEquals("empty window key schema bytes changed",
                new byte[]{0, 0, 0, 0}, window.borrowCheckpointKeySchemaForTest());
        Assert.assertSame("the window's empty schema is not the shared singleton",
                window.borrowCheckpointKeySchemaForTest(), malformed.borrowEncodedKeySchemaForTest());
    }

    private static void assertSameOwners(Snapshot snapshot, LiveViewInstance instance, boolean isPlanExpected) {
        final LiveViewWindow window = instance.getAnchorWindow();
        final WindowFunction function = firstFunction(instance);
        final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
        Assert.assertSame("window name backing changed",
                snapshot.windowName, window.borrowCheckpointWindowNameUtf8ForTest());
        Assert.assertSame("window schema backing changed",
                snapshot.windowSchema, window.borrowCheckpointKeySchemaForTest());
        Assert.assertSame("function identity backing changed",
                snapshot.functionIdentity, function.checkpointFunctionIdentity().borrowEncodedForTest());
        Assert.assertSame("function schema backing changed",
                snapshot.functionSchema, function.checkpointFunctionIdentity().borrowEncodedKeySchemaForTest());
        Assert.assertNotSame("public function identity is not defensive",
                snapshot.functionIdentity, function.checkpointFunctionIdentity().getEncoded());
        // The public getter copies the very array the borrow lends.
        Assert.assertArrayEquals("borrowed function identity differs from its public copy",
                function.checkpointFunctionIdentity().getEncoded(), function.checkpointFunctionIdentity().borrowEncodedForTest());
        if (isPlanExpected) {
            Assert.assertNotNull("fused plan disappeared", plan);
            Assert.assertArrayEquals("borrowed window identity differs from its public copy",
                    plan.getWindowIdentity(), plan.borrowWindowIdentityForTest());
            Assert.assertArrayEquals("borrowed manifest differs from its public copy",
                    plan.getManifest().getEncoded(), plan.getManifest().borrowEncodedForTest());
            Assert.assertSame("window identity backing changed",
                    snapshot.windowIdentity, plan.borrowWindowIdentityForTest());
            Assert.assertSame("manifest backing changed",
                    snapshot.manifest, plan.getManifest().borrowEncodedForTest());
            Assert.assertNotSame("public window identity is not defensive",
                    snapshot.windowIdentity, plan.getWindowIdentity());
            Assert.assertNotSame("public manifest is not defensive",
                    snapshot.manifest, plan.getManifest().getEncoded());
        } else {
            Assert.assertNull("legacy/residual path unexpectedly retained fused plan", plan);
        }
    }

    private static Snapshot capture(LiveViewInstance instance) {
        final LiveViewWindow window = instance.getAnchorWindow();
        final WindowFunction function = firstFunction(instance);
        final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
        Assert.assertNotNull("expected fused window plan", plan);
        return new Snapshot(
                window.borrowCheckpointWindowNameUtf8ForTest(),
                window.borrowCheckpointKeySchemaForTest(),
                function.checkpointFunctionIdentity().borrowEncodedForTest(),
                function.checkpointFunctionIdentity().borrowEncodedKeySchemaForTest(),
                plan.borrowWindowIdentityForTest(),
                plan.getManifest().borrowEncodedForTest()
        );
    }

    private static Path checkpointsDir(LiveViewInstance instance) {
        return new Path().of(configuration.getDbRoot())
                .concat(instance.getLiveViewToken())
                .concat(LiveViewCheckpointLayout.CHECKPOINT_DIR_NAME);
    }

    private static void expectBuildFailure(CheckedRunnable action) {
        try {
            action.run();
            throw new AssertionError("expected aggregate segment mismatch");
        } catch (CairoException e) {
            final String message = e.getFlyweightMessage().toString();
            Assert.assertTrue("wrong builder failure: " + message,
                    message.contains("aggregate segment id mismatch") || message.contains("metadata segment writer is not open"));
        }
    }

    private static void expectInitializerFailure(String expectedMessage, CheckedRunnable action) {
        try {
            action.run();
            throw new AssertionError("expected builder initializer failure");
        } catch (CairoException e) {
            final String message = e.getFlyweightMessage().toString();
            Assert.assertTrue("wrong initializer failure: " + message, message.contains(expectedMessage));
        }
    }

    private static WindowFunction firstFunction(LiveViewInstance instance) {
        final ObjList<WindowFunction> functions = instance.getCompiledPlan().getWindowFactory().getWindowFunctions();
        return functions.getQuick(0);
    }

    private static LiveViewInstance instance(String name) {
        final LiveViewInstance instance = engine.getLiveViewRegistry().getViewInstance(name);
        Assert.assertNotNull(instance);
        return instance;
    }

    private static void restoreTwice(LiveViewInstance instance) {
        final ObjList<WindowFunction> functions = unwrapWindowFunctions(instance);
        try (Path dir = checkpointsDir(instance); LiveViewCheckpointTimelineStoreReader reader = new LiveViewCheckpointTimelineStoreReader(configuration)) {
            reader.of(dir);
            reader.restoreLatest(instance.getLiveViewToken().getTableId(), functions, instance.getAnchorWindow());
            reader.restoreLatest(instance.getLiveViewToken().getTableId(), functions, instance.getAnchorWindow());
        }
    }

    private static byte[] schema(int columnType) {
        final byte[] bytes = new byte[2 * Integer.BYTES];
        bytes[3] = 1;
        bytes[4] = (byte) (columnType >>> 24);
        bytes[5] = (byte) (columnType >>> 16);
        bytes[6] = (byte) (columnType >>> 8);
        bytes[7] = (byte) columnType;
        return bytes;
    }

    private void insert(LiveViewRefreshJob job, String ts, String account, double amount) throws Exception {
        execute("insert into tx values ('" + ts + "', '" + account + "', " + amount + ")");
        drainWalQueue();
        driveRefreshToQuiescence(job);
    }

    @FunctionalInterface
    private interface CheckedRunnable {
        void run();
    }

    private static final class Snapshot {
        private final byte[] functionIdentity;
        private final byte[] functionSchema;
        private final byte[] manifest;
        private final byte[] windowIdentity;
        private final byte[] windowName;
        private final byte[] windowSchema;

        private Snapshot(byte[] windowName, byte[] windowSchema, byte[] functionIdentity,
                         byte[] functionSchema, byte[] windowIdentity, byte[] manifest) {
            this.windowName = windowName;
            this.windowSchema = windowSchema;
            this.functionIdentity = functionIdentity;
            this.functionSchema = functionSchema;
            this.windowIdentity = windowIdentity;
            this.manifest = manifest;
        }
    }
}
