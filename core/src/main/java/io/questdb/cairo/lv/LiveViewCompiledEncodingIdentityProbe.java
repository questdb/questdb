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
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

/**
 * Opaque backing-identity assertions for tests outside the {@code io.questdb} module.
 * No mutable compiled byte array crosses this API: {@link Snapshot} deliberately exposes
 * no state, and every comparison is performed here in the arrays' own package.
 */
@TestOnly
public final class LiveViewCompiledEncodingIdentityProbe {

    private LiveViewCompiledEncodingIdentityProbe() {
    }

    public static Snapshot capture(LiveViewInstance instance) {
        final LiveViewWindow window = instance.getAnchorWindow();
        final WindowFunction function = firstFunction(instance);
        final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
        if (plan == null) {
            throw new AssertionError("expected fused window plan");
        }
        return new Snapshot(
                window.borrowCheckpointWindowNameUtf8(),
                window.borrowCheckpointKeySchema(),
                function.checkpointFunctionIdentity().borrowEncoded(),
                function.checkpointFunctionIdentity().borrowEncodedKeySchema(),
                plan.borrowWindowIdentity(),
                plan.getManifest().borrowEncoded()
        );
    }

    public static void assertBuilderFailureReuse(CairoConfiguration configuration, Path dir) {
        final byte[] invalidName = new byte[]{'x'};
        final byte[] invalidSchema = schema(ColumnType.INT);
        final byte[] invalidIdentity = new byte[]{9};
        final byte[] invalidManifest = new byte[]{8};
        final byte[] nameA = new byte[]{'a'};
        final byte[] nameB = new byte[]{'b'};
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

        try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration);
             LiveViewCheckpointMetaSegmentWriter unopened = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
            expectInitializerFailure(() -> builder.ofBorrowedCompiled(
                    dir, invalidMetaRef, invalidName, ColumnType.TIMESTAMP_MICRO, invalidSchema, true
            ));
            require(!builder.isBorrowingCompiledForTest(invalidName, invalidSchema),
                    "anchor builder retained invalid-predecessor bytes");
            builder.ofBorrowedCompiled(dir, nullMetaRef, nameB, ColumnType.TIMESTAMP_MICRO, schemaB, true);
            require(builder.isBorrowingCompiledForTest(nameB, schemaB),
                    "anchor builder did not reuse after invalid predecessor");
            builder.ofBorrowedCompiled(dir, nullMetaRef, nameA, ColumnType.TIMESTAMP_MICRO, schemaA, true);
            require(builder.isBorrowingCompiledForTest(nameA, schemaA), "anchor builder cloned compiled bytes");
            expectBuildFailure(() -> builder.buildIntoOpenSegment(77, unopened, new LiveViewCheckpointPageRef()));
            require(!builder.isBorrowingCompiledForTest(nameA, schemaA), "anchor builder retained failed-view bytes");
            builder.ofBorrowedCompiled(dir, nullMetaRef, nameB, ColumnType.TIMESTAMP_MICRO, schemaB, true);
            require(builder.isBorrowingCompiledForTest(nameB, schemaB), "anchor builder did not borrow replacement bytes");
            require(!builder.isBorrowingCompiledForTest(nameA, schemaA), "anchor builder leaked prior-view bytes");
        }

        try (LiveViewCheckpointFunctionRootBuilder builder = new LiveViewCheckpointFunctionRootBuilder(configuration);
             LiveViewCheckpointMetaSegmentWriter unopened = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
            expectInitializerFailure(() ->
                    builder.ofBorrowedCompiled(dir, invalidMetaRef, invalidIdentity, 1, invalidSchema));
            require(!builder.isBorrowingCompiledForTest(invalidIdentity, invalidSchema),
                    "function builder retained invalid-predecessor bytes");
            builder.ofBorrowedCompiled(dir, nullMetaRef, identityB, 1, schemaB);
            require(builder.isBorrowingCompiledForTest(identityB, schemaB),
                    "function builder did not reuse after invalid predecessor");
            builder.ofBorrowedCompiled(dir, nullMetaRef, identityA, 1, schemaA);
            require(builder.isBorrowingCompiledForTest(identityA, schemaA), "function builder cloned compiled bytes");
            expectBuildFailure(() -> builder.buildIntoOpenSegment(78, unopened, new LiveViewCheckpointPageRef()));
            require(!builder.isBorrowingCompiledForTest(identityA, schemaA), "function builder retained failed-view bytes");
            builder.ofBorrowedCompiled(dir, nullMetaRef, identityB, 1, schemaB);
            require(builder.isBorrowingCompiledForTest(identityB, schemaB), "function builder did not borrow replacement bytes");
            require(!builder.isBorrowingCompiledForTest(identityA, schemaA), "function builder leaked prior-view bytes");
        }

        try (LiveViewCheckpointWindowRootBuilder builder = new LiveViewCheckpointWindowRootBuilder(configuration);
             LiveViewCheckpointMetaSegmentWriter unopened = new LiveViewCheckpointMetaSegmentWriter(configuration)) {
            expectInitializerFailure(() -> builder.ofBorrowedCompiled(
                    dir, invalidMetaRef, invalidIdentity, ColumnType.TIMESTAMP_MICRO,
                    invalidSchema, invalidManifest, 16, true, null
            ));
            require(!builder.isBorrowingCompiledForTest(invalidIdentity, invalidSchema, invalidManifest),
                    "window builder retained invalid-predecessor bytes");
            builder.ofBorrowedCompiled(
                    dir, nullMetaRef, identityB, ColumnType.TIMESTAMP_MICRO, schemaB, manifestB, 16, true, null
            );
            require(builder.isBorrowingCompiledForTest(identityB, schemaB, manifestB),
                    "window builder did not reuse after invalid predecessor");
            builder.ofBorrowedCompiled(dir, nullMetaRef, identityA, ColumnType.TIMESTAMP_MICRO, schemaA, manifestA, 16, true, null);
            require(builder.isBorrowingCompiledForTest(identityA, schemaA, manifestA), "window builder cloned compiled bytes");
            expectBuildFailure(() -> builder.buildIntoOpenSegment(79, unopened, new LiveViewCheckpointPageRef()));
            require(!builder.isBorrowingCompiledForTest(identityA, schemaA, manifestA), "window builder retained failed-view bytes");
            builder.ofBorrowedCompiled(dir, nullMetaRef, identityB, ColumnType.TIMESTAMP_MICRO, schemaB, manifestB, 16, true, null);
            require(builder.isBorrowingCompiledForTest(identityB, schemaB, manifestB), "window builder did not borrow replacement bytes");
            require(!builder.isBorrowingCompiledForTest(identityA, schemaA, manifestA), "window builder leaked prior-view bytes");
        }

        final LiveViewCheckpointPageRef anchorPredecessor = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointAnchorRootBuilder builder = new LiveViewCheckpointAnchorRootBuilder(configuration)) {
            builder.ofBorrowedCompiled(dir, nullMetaRef, nameA, ColumnType.TIMESTAMP_MICRO, schemaA, true);
            builder.build(80, anchorPredecessor);
            expectInitializerFailure(() -> builder.ofBorrowedCompiled(
                    dir, anchorPredecessor, nameB, ColumnType.TIMESTAMP_MICRO, schemaB, true
            ));
            require(!builder.isBorrowingCompiledForTest(nameB, schemaB),
                    "anchor builder retained semantic-mismatch bytes");
            builder.ofBorrowedCompiled(
                    dir, anchorPredecessor, nameA, ColumnType.TIMESTAMP_MICRO, schemaA, true
            );
            require(builder.isBorrowingCompiledForTest(nameA, schemaA),
                    "anchor builder did not reuse after semantic mismatch");
        }

        final LiveViewCheckpointPageRef functionPredecessor = new LiveViewCheckpointPageRef();
        try (LiveViewCheckpointFunctionRootBuilder builder = new LiveViewCheckpointFunctionRootBuilder(configuration)) {
            builder.ofBorrowedCompiled(dir, nullMetaRef, identityA, 1, schemaA);
            builder.build(81, functionPredecessor);
            expectInitializerFailure(() ->
                    builder.ofBorrowedCompiled(dir, functionPredecessor, identityB, 1, schemaB));
            require(!builder.isBorrowingCompiledForTest(identityB, schemaB),
                    "function builder retained semantic-mismatch bytes");
            builder.ofBorrowedCompiled(dir, functionPredecessor, identityA, 1, schemaA);
            require(builder.isBorrowingCompiledForTest(identityA, schemaA),
                    "function builder did not reuse after semantic mismatch");
        }

        try (LiveViewCheckpointAnchorRoot root = new LiveViewCheckpointAnchorRoot(configuration)) {
            root.ofBuilder(nameA, ColumnType.TIMESTAMP_MICRO, schemaA, nullMetaRef, noSegments);
            require(root.isBorrowingCompiledForTest(nameA, schemaA), "anchor result root cloned compiled bytes");
            root.clearBorrowedCompiled();
            root.ofBuilder(nameB, ColumnType.TIMESTAMP_MICRO, schemaB, nullMetaRef, noSegments);
            require(root.isBorrowingCompiledForTest(nameB, schemaB), "anchor result root did not replace bytes");
        }
        try (LiveViewCheckpointFunctionRoot root = new LiveViewCheckpointFunctionRoot(configuration)) {
            root.ofBuilder(identityA, 1, schemaA, nullStateRef, nullMetaRef, noSegments);
            require(root.isBorrowingCompiledForTest(identityA, schemaA), "function result root cloned compiled bytes");
            root.clearBorrowedCompiled();
            root.ofBuilder(identityB, 1, schemaB, nullStateRef, nullMetaRef, noSegments);
            require(root.isBorrowingCompiledForTest(identityB, schemaB), "function result root did not replace bytes");
        }
        try (LiveViewCheckpointWindowRoot root = new LiveViewCheckpointWindowRoot(configuration)) {
            root.ofBuilder(identityA, ColumnType.TIMESTAMP_MICRO, schemaA, manifestA, 16, nullMetaRef, noSegments);
            require(root.isBorrowingCompiledForTest(identityA, schemaA, manifestA), "window result root cloned compiled bytes");
            root.clearBorrowedCompiled();
            root.ofBuilder(identityB, ColumnType.TIMESTAMP_MICRO, schemaB, manifestB, 16, nullMetaRef, noSegments);
            require(root.isBorrowingCompiledForTest(identityB, schemaB, manifestB), "window result root did not replace bytes");
        }
    }

    public static void assertDistinct(Snapshot left, Snapshot right) {
        require(left.windowName != right.windowName, "alternating views share window-name backing");
        require(left.windowSchema != right.windowSchema, "alternating views share nonempty schema backing");
        require(left.functionIdentity != right.functionIdentity, "alternating views share function identity backing");
        require(left.functionSchema != right.functionSchema, "alternating views share function schema backing");
        require(left.windowIdentity != right.windowIdentity, "alternating views share window identity backing");
        require(left.manifest != right.manifest, "alternating views share manifest backing");
        require(!Arrays.equals(left.windowName, right.windowName), "alternating views have equal window names");
        require(!Arrays.equals(left.windowSchema, right.windowSchema), "alternating views have equal schemas");
    }

    public static void assertMalformedUnicodeGoldenBytesAndEmptySchemaSingleton(CairoConfiguration configuration) {
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
                new ObjList<>(),
                false,
                null,
                null
        );
        require(Arrays.equals(expectedUtf8, window.borrowCheckpointWindowNameUtf8()),
                "malformed window name UTF-8 bytes changed");
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
        require(Arrays.equals(expectedIdentity, malformed.borrowEncoded()),
                "malformed function identity bytes changed");
        require(malformed.borrowEncoded() == malformed.borrowEncoded(), "malformed Unicode identity was recreated");
        require(malformed.borrowEncoded() != malformed.getEncoded(), "public function identity is not defensive");
        require(malformed.borrowEncodedKeySchema() == emptyAgain.borrowEncodedKeySchema(), "empty schemas do not share singleton");
    }

    public static void assertSameOwners(Snapshot snapshot, LiveViewInstance instance, boolean expectPlan) {
        final LiveViewWindow window = instance.getAnchorWindow();
        final WindowFunction function = firstFunction(instance);
        final LiveViewWindowStatePlan plan = window.getCheckpointWindowStatePlan();
        require(snapshot.windowName == window.borrowCheckpointWindowNameUtf8(), "window name backing changed");
        require(snapshot.windowSchema == window.borrowCheckpointKeySchema(), "window schema backing changed");
        require(snapshot.functionIdentity == function.checkpointFunctionIdentity().borrowEncoded(), "function identity backing changed");
        require(snapshot.functionSchema == function.checkpointFunctionIdentity().borrowEncodedKeySchema(), "function schema backing changed");
        require(snapshot.functionIdentity != function.checkpointFunctionIdentity().getEncoded(), "public function identity is not defensive");
        if (expectPlan) {
            require(plan != null, "fused plan disappeared");
            require(snapshot.windowIdentity == plan.borrowWindowIdentity(), "window identity backing changed");
            require(snapshot.manifest == plan.getManifest().borrowEncoded(), "manifest backing changed");
            require(snapshot.windowIdentity != plan.getWindowIdentity(), "public window identity is not defensive");
            require(snapshot.manifest != plan.getManifest().getEncoded(), "public manifest is not defensive");
        } else {
            require(plan == null, "legacy/residual path unexpectedly retained fused plan");
        }
    }

    private static WindowFunction firstFunction(LiveViewInstance instance) {
        final ObjList<WindowFunction> functions = instance.getCompiledPlan().getWindowFactory().getWindowFunctions();
        return functions.getQuick(0);
    }

    private static void expectBuildFailure(CheckedRunnable action) {
        try {
            action.run();
            throw new AssertionError("expected aggregate segment mismatch");
        } catch (CairoException e) {
            final String message = e.getFlyweightMessage().toString();
            require(message.contains("aggregate segment id mismatch") || message.contains("metadata segment writer is not open"),
                    "wrong builder failure: " + message);
        }
    }

    private static void expectInitializerFailure(CheckedRunnable action) {
        try {
            action.run();
            throw new AssertionError("expected builder initializer failure");
        } catch (CairoException ignored) {
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
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

    @FunctionalInterface
    private interface CheckedRunnable {
        void run();
    }

    public static final class Snapshot {
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
