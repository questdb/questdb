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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewAccumulatorDescriptor;
import io.questdb.cairo.lv.LiveViewWindowStateManifest;
import io.questdb.cairo.lv.LiveViewWindowStatePlan;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.griffin.engine.window.WindowRecordCursorFactory;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Golden bytes for everything a fused live-view window state writes to disk: each
 * accumulator component's encoded identity, the whole-state image the component codec
 * packs into a leaf, the manifest that lays those components out, and the window-group
 * identity a root is compared by.
 * <p>
 * Every other case in this package asserts these encodings against <i>themselves</i> -
 * reordering a SELECT list must not move the manifest, recompiling one view must
 * reproduce it, a component's codec must equal its contributor's image. Those are the
 * right properties and they are all self-referential: a refactor that changed the
 * encoder would keep every one of them green while making an existing checkpoint
 * unreadable. Only a literal pins the bytes, so that is what is here.
 * <p>
 * The bytes matter because a fused leaf entry carries no tags and no lengths of its own.
 * Its meaning comes entirely from the manifest of the root that names it, and a writer
 * may build incrementally on a predecessor's leaves only when the two manifests are
 * byte-equal - so a manifest that shifted by one field would not be rejected, it would be
 * read, at the right total length, out of the wrong fields. The component identity has
 * the same property one level down: it is what decides whether two calls share a slice,
 * and it embeds the argument's {@code (column index, column type)} pair, so a
 * {@code ColumnType} renumbering is a persisted-format change and is meant to fail here.
 * <p>
 * What these cases exist for is the runtime/durable split: the accumulator descriptor,
 * projection and plan are being pulled apart into a runtime-neutral pair plus a live-view
 * durable wrapper, and the persisted encoding must come through that unchanged. They are
 * deliberately blind to which class produces the bytes.
 */
public class LiveViewWindowStateGoldenEncodingTest extends AbstractLiveViewTest {

    /**
     * {@code count(d)} over the DECIMAL64 column of {@code gold}, at base index 4. The
     * argument type carries the decimal's precision and scale, so a count over
     * {@code decimal(18,3)} and one over {@code decimal(18,2)} are two components - which
     * is what the {@code 000c121f} in the last field says.
     */
    private static final String GOLDEN_COUNT_DECIMAL_COL4 =
            "4c564143" + "00000001" + "00000002" + "00000001" + "00000002" + "00000004" + "000c121f";
    /**
     * {@code count(x)} over the DOUBLE column of {@code gold}, at base index 2. It counts
     * under {@code CONTRIBUTION_FINITE_DOUBLE}, exactly as the sum beside it does, which is
     * the whole reason the two can share one counter.
     */
    private static final String GOLDEN_COUNT_DOUBLE_COL2 =
            "4c564143" + "00000001" + "00000002" + "00000001" + "00000001" + "00000002" + "0000000a";
    /**
     * {@code count(y)} over the LONG column of {@code gold}, at base index 3. A LONG
     * argument reaches the DOUBLE-stated factories by widening, so it counts under the
     * same predicate a DOUBLE does - and still keys a different component, because the
     * widening is what the predicate was proved through.
     */
    private static final String GOLDEN_COUNT_LONG_COL3 =
            "4c564143" + "00000001" + "00000002" + "00000001" + "00000001" + "00000003" + "00000006";
    /**
     * {@code count(k)} over the SYMBOL column of {@code gold}, at base index 1. A SYMBOL
     * counts under its own null test rather than under {@code isFinite}, which is what
     * keeps it apart from the counter inside a DOUBLE sum however alike the two read.
     */
    private static final String GOLDEN_COUNT_SYMBOL_COL1 =
            "4c564143" + "00000001" + "00000002" + "00000001" + "00000002" + "00000001" + "0000000c";
    private static final String GOLDEN_COUNT_STATE_IMAGE = "0300000000000000";
    /**
     * {@code ksum(x)} over the DOUBLE column of {@code gold}, at base index 2. It counts
     * the rows a plain {@code sum(x)} counts - the same {@code isFinite} test - and keeps a
     * different total, which is why it is its own family carrying the same contribution
     * kind.
     */
    private static final String GOLDEN_KAHAN_SUM_COUNT_DOUBLE_COL2 =
            "4c564143" + "00000001" + "00000009" + "00000001" + "00000001" + "00000002" + "0000000a";
    /**
     * {@code (sum, compensation, nonNullCount)} holding {@code 17.5}, {@code 18.5} and
     * {@code 5}. The counter is last, which is the fact a {@code count(x)} folded onto a
     * {@code ksum(x)} reads its own image out of - and pinned separately from the Welford
     * image it happens to equal, because the two are separate claims about separate
     * implementations.
     */
    private static final String GOLDEN_KAHAN_SUM_COUNT_STATE_IMAGE =
            "0000000000803140" + "0000000000803240" + "0500000000000000";
    /**
     * A running extremum holding {@code 17.5}: one little-endian 64-bit field and no
     * counter beside it, because the family's empty state is the slot's own NULL.
     */
    private static final String GOLDEN_EXTREMUM_DOUBLE_STATE_IMAGE = "0000000000803140";
    /**
     * A 64-bit running extremum holding {@code 3}. The same one field as the DOUBLE one at
     * the other state width, and the same bytes a one-slot counter's image has - which the
     * identity, not the image, is what tells apart.
     */
    private static final String GOLDEN_EXTREMUM_LONG_STATE_IMAGE = "0300000000000000";
    /**
     * The manifest of a two-component group built directly rather than compiled: a row
     * count and a Welford accumulator, which no compiled case below produces. The two
     * carry the remaining families, so every family the plan admits reaches a manifest
     * golden.
     */
    private static final String GOLDEN_MANIFEST_ROW_COUNT_AND_WELFORD =
            "4c56574d" + "00000001" + "00000028" + "00000000" + "00000008" + "00000002"
                    + "00000001" + "00000001" + "00000008" + "00000008" + "0000001c"
                    + "4c564143" + "00000001" + "00000003" + "00000001" + "00000003" + "ffffffff" + "00000000"
                    + "00000001" + "00000001" + "00000010" + "00000018" + "0000001c"
                    + "4c564143" + "00000001" + "00000004" + "00000001" + "00000001" + "00000002" + "0000000a";
    /**
     * The manifest of {@code sum(x) + count(k)} over one window: the shape the fused work
     * was built for, and the one whose two counters must never merge. Two components,
     * 8-byte anchor plus 16 plus 8, so a 32-byte leaf entry.
     */
    private static final String GOLDEN_MANIFEST_SUM_AND_KEY_COUNT =
            "4c56574d" + "00000001" + "00000020" + "00000000" + "00000008" + "00000002"
                    + "00000001" + "00000001" + "00000008" + "00000010" + "0000001c"
                    + "4c564143" + "00000001" + "00000001" + "00000001" + "00000001" + "00000002" + "0000000a"
                    + "00000001" + "00000001" + "00000018" + "00000008" + "0000001c"
                    + "4c564143" + "00000001" + "00000002" + "00000001" + "00000002" + "00000001" + "0000000c";
    /**
     * The manifest of {@code sum(x) + avg(x) + count(x)} over one window: one component,
     * because sum and avg merge outright and the count folds onto the counter beside their
     * sum. 8-byte anchor plus 16, so a 24-byte leaf entry for three calls.
     */
    private static final String GOLDEN_MANIFEST_SUM_AVG_COUNT =
            "4c56574d" + "00000001" + "00000018" + "00000000" + "00000008" + "00000001"
                    + "00000001" + "00000001" + "00000008" + "00000010" + "0000001c"
                    + "4c564143" + "00000001" + "00000001" + "00000001" + "00000001" + "00000002" + "0000000a";
    /**
     * {@code max(x)} over the DOUBLE column of {@code gold}, at base index 2. It reads its
     * argument through the same {@code getDouble} a sum does and skips a row on the same
     * {@code isFinite} test, so it carries {@code CONTRIBUTION_FINITE_DOUBLE} - and stays a
     * component of its own, because a running maximum is not readable out of a total.
     */
    private static final String GOLDEN_MAX_DOUBLE_COL2 =
            "4c564143" + "00000001" + "00000005" + "00000001" + "00000001" + "00000002" + "0000000a";
    /**
     * {@code max(y)} over the LONG column of {@code gold}, at base index 3. A 64-bit
     * extremum keeps the argument's payload word and skips the row whose word is
     * {@code LONG_NULL}, which is its own null test rather than the DOUBLE one.
     */
    private static final String GOLDEN_MAX_LONG_COL3 =
            "4c564143" + "00000001" + "00000007" + "00000001" + "00000002" + "00000003" + "00000006";
    /**
     * {@code min(x)} over the DOUBLE column of {@code gold}, at base index 2. Separate from
     * {@link #GOLDEN_MAX_DOUBLE_COL2} by one field, which is the whole of what keeps a
     * running minimum from being read out of a running maximum.
     */
    private static final String GOLDEN_MIN_DOUBLE_COL2 =
            "4c564143" + "00000001" + "00000006" + "00000001" + "00000001" + "00000002" + "0000000a";
    /**
     * {@code min(y)} over the LONG column of {@code gold}, at base index 3.
     */
    private static final String GOLDEN_MIN_LONG_COL3 =
            "4c564143" + "00000001" + "00000008" + "00000001" + "00000002" + "00000003" + "00000006";
    /**
     * {@code count(*)} and partitioned {@code row_number()}. The family takes no argument
     * at all, and its identity says so with one exact pair - {@code ffffffff} for the
     * column index and {@code 00000000} for the type - rather than with an absence.
     */
    private static final String GOLDEN_ROW_COUNT =
            "4c564143" + "00000001" + "00000003" + "00000001" + "00000003" + "ffffffff" + "00000000";
    /**
     * {@code sum(x)} and {@code avg(x)} over the DOUBLE column of {@code gold}, at base
     * index 2. One identity for both, which is what merges them onto one slice.
     */
    private static final String GOLDEN_SUM_COUNT_DOUBLE_COL2 =
            "4c564143" + "00000001" + "00000001" + "00000001" + "00000001" + "00000002" + "0000000a";
    /**
     * {@code (sum, nonNullCount)} holding {@code 17.5} and {@code 4}. Two little-endian
     * 64-bit fields in slot order, which is the image the contributing function's own
     * {@code freezeCheckpointState} writes.
     */
    private static final String GOLDEN_SUM_COUNT_STATE_IMAGE = "0000000000803140" + "0400000000000000";
    private static final String GOLDEN_WELFORD_DOUBLE_COL2 =
            "4c564143" + "00000001" + "00000004" + "00000001" + "00000001" + "00000002" + "0000000a";
    /**
     * {@code (mean, m2, nonNullCount)} holding {@code 17.5}, {@code 18.5} and {@code 5}.
     * The counter is last, which is the fact a {@code count(x)} folded onto a
     * {@code stddev(x)} reads its own image out of.
     */
    private static final String GOLDEN_WELFORD_STATE_IMAGE =
            "0000000000803140" + "0000000000803240" + "0500000000000000";
    /**
     * {@code WINDOW w AS (PARTITION BY k ORDER BY ts ...)}, as the compiled shapes below
     * produce it. Length-prefixed throughout, so a delimiter inside an identifier cannot
     * alias another window's identity.
     */
    private static final String GOLDEN_WINDOW_IDENTITY =
            "4c565749" + "00000001"
                    + "00000001" + "77"
                    + "00000006" + "313a313a6b3b"
                    + "00000009" + "313a323a74733a303b";
    private static final String GOLD_WINDOW = " from gold window w as (partition by k order by ts anchor daily '00:00')";
    private static final String SUM_AND_KEY_COUNT_SELECT =
            "select ts, k, sum(x) over w as s, count(k) over w as c" + GOLD_WINDOW;
    private static final String SUM_AVG_COUNT_SELECT =
            "select ts, k, sum(x) over w as s, avg(x) over w as a, count(x) over w as c" + GOLD_WINDOW;

    @Test
    public void testAccumulatorComponentIdentityBytes() {
        assertGolden(
                "sum(x)/avg(x) over a DOUBLE column",
                GOLDEN_SUM_COUNT_DOUBLE_COL2,
                component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT, 2, ColumnType.DOUBLE).getEncoded()
        );
        assertGolden(
                "count(x) over a DOUBLE column",
                GOLDEN_COUNT_DOUBLE_COL2,
                component(WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 2, ColumnType.DOUBLE).getEncoded()
        );
        assertGolden(
                "count(y) over a LONG column",
                GOLDEN_COUNT_LONG_COL3,
                component(WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 3, ColumnType.LONG).getEncoded()
        );
        assertGolden(
                "count(k) over a SYMBOL column",
                GOLDEN_COUNT_SYMBOL_COL1,
                component(WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 1, ColumnType.SYMBOL).getEncoded()
        );
        assertGolden(
                "count(d) over a DECIMAL(18,3) column",
                GOLDEN_COUNT_DECIMAL_COL4,
                component(
                        WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                        4,
                        ColumnType.getDecimalType(18, 3)
                ).getEncoded()
        );
        assertGolden(
                "count(*) / row_number()",
                GOLDEN_ROW_COUNT,
                rowCountComponent().getEncoded()
        );
        assertGolden(
                "the dispersion family over a DOUBLE column",
                GOLDEN_WELFORD_DOUBLE_COL2,
                component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD, 2, ColumnType.DOUBLE).getEncoded()
        );
        assertGolden(
                "ksum(x) over a DOUBLE column",
                GOLDEN_KAHAN_SUM_COUNT_DOUBLE_COL2,
                component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT, 2, ColumnType.DOUBLE).getEncoded()
        );
        assertGolden(
                "max(x) over a DOUBLE column",
                GOLDEN_MAX_DOUBLE_COL2,
                component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX, 2, ColumnType.DOUBLE).getEncoded()
        );
        assertGolden(
                "min(x) over a DOUBLE column",
                GOLDEN_MIN_DOUBLE_COL2,
                component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN, 2, ColumnType.DOUBLE).getEncoded()
        );
        assertGolden(
                "max(y) over a LONG column",
                GOLDEN_MAX_LONG_COL3,
                component(WindowAccumulatorDescriptor.FAMILY_LONG_MAX, 3, ColumnType.LONG).getEncoded()
        );
        assertGolden(
                "min(y) over a LONG column",
                GOLDEN_MIN_LONG_COL3,
                component(WindowAccumulatorDescriptor.FAMILY_LONG_MIN, 3, ColumnType.LONG).getEncoded()
        );
    }

    @Test
    public void testComponentStateImageBytes() throws Exception {
        assertMemoryLeak(() -> {
            // The leaf inlines these bytes at the manifest's offset and carries no length
            // that would catch a field moving, so the image is pinned field for field
            // rather than only round-tripped.
            assertStateImage(
                    component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT, 2, ColumnType.DOUBLE),
                    GOLDEN_SUM_COUNT_STATE_IMAGE
            );
            assertStateImage(
                    component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD, 2, ColumnType.DOUBLE),
                    GOLDEN_WELFORD_STATE_IMAGE
            );
            assertStateImage(
                    component(WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 2, ColumnType.DOUBLE),
                    GOLDEN_COUNT_STATE_IMAGE
            );
            assertStateImage(rowCountComponent(), GOLDEN_COUNT_STATE_IMAGE);
            assertStateImage(
                    component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT, 2, ColumnType.DOUBLE),
                    GOLDEN_KAHAN_SUM_COUNT_STATE_IMAGE
            );
            assertStateImage(
                    component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX, 2, ColumnType.DOUBLE),
                    GOLDEN_EXTREMUM_DOUBLE_STATE_IMAGE
            );
            assertStateImage(
                    component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN, 2, ColumnType.DOUBLE),
                    GOLDEN_EXTREMUM_DOUBLE_STATE_IMAGE
            );
            assertStateImage(
                    component(WindowAccumulatorDescriptor.FAMILY_LONG_MAX, 3, ColumnType.LONG),
                    GOLDEN_EXTREMUM_LONG_STATE_IMAGE
            );
            assertStateImage(
                    component(WindowAccumulatorDescriptor.FAMILY_LONG_MIN, 3, ColumnType.LONG),
                    GOLDEN_EXTREMUM_LONG_STATE_IMAGE
            );
        });
    }

    @Test
    public void testEveryAdmittedFamilyHasAGoldenIdentity() {
        // Walked rather than listed: a family added without a golden identity above must
        // fail here instead of shipping an encoding nothing pinned. The bound is well past
        // the nine ids in use and costs nothing.
        final IntList admitted = new IntList();
        for (int family = 0; family < 64; family++) {
            if (LiveViewAccumulatorDescriptor.familyCodecVersion(family) >= 0) {
                admitted.add(family);
            }
        }
        // In ascending family-id order, which is the order the walk above discovers them
        // in - the ids themselves are arbitrary and only their persistence matters.
        final IntList golden = new IntList();
        golden.add(WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT);
        golden.add(WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT);
        golden.add(WindowAccumulatorDescriptor.FAMILY_ROW_COUNT);
        golden.add(WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD);
        golden.add(WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX);
        golden.add(WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN);
        golden.add(WindowAccumulatorDescriptor.FAMILY_LONG_MAX);
        golden.add(WindowAccumulatorDescriptor.FAMILY_LONG_MIN);
        golden.add(WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT);
        Assert.assertEquals(
                "a family without a golden identity is a persisted encoding nothing pins",
                golden.toString(),
                admitted.toString()
        );
    }

    @Test
    public void testManifestBytesForADirectlyBuiltComponentList() {
        final ObjList<LiveViewAccumulatorDescriptor> components = new ObjList<>();
        components.add(rowCountComponent());
        components.add(component(WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD, 2, ColumnType.DOUBLE));
        final IntList offsets = new IntList();
        offsets.add(LiveViewWindowStatePlan.ANCHOR_STATE_OFFSET + LiveViewWindowStatePlan.ANCHOR_STATE_BYTES);
        offsets.add(offsets.getQuick(0) + components.getQuick(0).getStateLength());
        final int total = offsets.getQuick(1) + components.getQuick(1).getStateLength();
        final LiveViewWindowStateManifest manifest = new LiveViewWindowStateManifest(
                components,
                offsets,
                LiveViewWindowStatePlan.ANCHOR_STATE_OFFSET,
                LiveViewWindowStatePlan.ANCHOR_STATE_BYTES,
                total
        );
        assertGolden("a row count beside a Welford accumulator", GOLDEN_MANIFEST_ROW_COUNT_AND_WELFORD, manifest.getEncoded());
    }

    @Test
    public void testTheSumAndKeyCountManifestBytes() throws Exception {
        assertMemoryLeak(() -> {
            createGoldenTable();
            assertPlan(SUM_AND_KEY_COUNT_SELECT, plan -> {
                Assert.assertNotNull(plan);
                Assert.assertEquals(2, plan.getComponentCount());
                assertGolden("sum(x) + count(k)", GOLDEN_MANIFEST_SUM_AND_KEY_COUNT, plan.getManifest().getEncoded());
                assertGolden("the window group", GOLDEN_WINDOW_IDENTITY, plan.getWindowIdentity());
            });
        });
    }

    @Test
    public void testTheSumAvgCountManifestBytes() throws Exception {
        assertMemoryLeak(() -> {
            createGoldenTable();
            assertPlan(SUM_AVG_COUNT_SELECT, plan -> {
                Assert.assertNotNull(plan);
                Assert.assertEquals(1, plan.getComponentCount());
                Assert.assertEquals(3, plan.getProjectionCount());
                assertGolden("sum(x) + avg(x) + count(x)", GOLDEN_MANIFEST_SUM_AVG_COUNT, plan.getManifest().getEncoded());
                assertGolden("the window group", GOLDEN_WINDOW_IDENTITY, plan.getWindowIdentity());
            });
        });
    }

    @Test
    public void testWindowGroupIdentityBytes() {
        assertGolden(
                "a named window over one key and one order term",
                GOLDEN_WINDOW_IDENTITY,
                LiveViewWindowStatePlan.encodeWindowIdentity("w", "1:1:k;", "1:2:ts:0;")
        );
        // Every field is length-prefixed, so moving a character across the boundary
        // between two of them is a different identity rather than the same bytes.
        Assert.assertNotEquals(
                hex(LiveViewWindowStatePlan.encodeWindowIdentity("w", "1:1:k;", "1:2:ts:0;")),
                hex(LiveViewWindowStatePlan.encodeWindowIdentity("w1", ":1:k;", "1:2:ts:0;"))
        );
    }

    private static void assertGolden(String what, String expectedHex, byte[] actual) {
        Assert.assertEquals(what, expectedHex, hex(actual));
    }

    /**
     * Compiles {@code sql} the way a live view compiles it and hands the resulting plan to
     * {@code check}, with the factory still open so the plan's non-owning references are
     * live.
     */
    private static void assertPlan(String sql, PlanCheck check) throws Exception {
        sqlExecutionContext.setLiveViewCompile(true);
        try (SqlCompiler compiler = engine.getSqlCompiler();
             RecordCursorFactory factory = select(compiler, sql, sqlExecutionContext)) {
            RecordCursorFactory root = factory;
            while (root != null && !(root instanceof WindowRecordCursorFactory)) {
                root = root.getBaseFactory();
            }
            Assert.assertNotNull(sql, root);
            check.run(((WindowRecordCursorFactory) root).getCheckpointWindowStatePlan());
        } finally {
            sqlExecutionContext.setLiveViewCompile(false);
        }
    }

    private static LiveViewAccumulatorDescriptor component(int family, int argumentColumnIndex, int argumentColumnType) {
        final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                family,
                argumentColumnIndex,
                argumentColumnType
        );
        Assert.assertNotNull(component);
        return component;
    }

    private static String hex(byte[] bytes) {
        final StringBuilder sink = new StringBuilder(bytes.length * 2);
        for (int i = 0; i < bytes.length; i++) {
            final int b = bytes[i] & 0xff;
            sink.append(Character.forDigit(b >>> 4, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sink.toString();
    }

    private static LiveViewAccumulatorDescriptor rowCountComponent() {
        return component(
                WindowAccumulatorDescriptor.FAMILY_ROW_COUNT,
                WindowAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX,
                ColumnType.UNDEFINED
        );
    }

    /**
     * Fills {@code component}'s slots with one distinct value each, freezes them through
     * the component codec and requires the bytes to be {@code expectedHex}, then restores
     * them and requires the same numbers back.
     * <p>
     * Distinct per slot, so a codec that transposed two fields of equal width would fail
     * rather than pass on equal bytes.
     */
    private void assertStateImage(LiveViewAccumulatorDescriptor component, String expectedHex) {
        final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
        for (int i = 0, n = component.getSlotCount(); i < n; i++) {
            valueTypes.add(component.getSlotColumnType(i));
        }
        final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        keyTypes.add(ColumnType.LONG);
        final Map scratch = MapFactory.createUnorderedMap(configuration, keyTypes, valueTypes);
        try {
            final MapKey key = scratch.withKey();
            key.putLong(1);
            final MapValue value = key.createValue();
            for (int i = 0, n = component.getSlotCount(); i < n; i++) {
                if (component.getSlotColumnType(i) == ColumnType.DOUBLE) {
                    value.putDouble(i, 17.5 + i);
                } else {
                    value.putLong(i, 3L + i);
                }
            }
            final byte[] image = new byte[component.getStateLength()];
            component.freezeStateInto(value, 0, image, 0);
            assertGolden("family " + component.getFamily() + " state image", expectedHex, image);

            for (int i = 0, n = component.getSlotCount(); i < n; i++) {
                if (component.getSlotColumnType(i) == ColumnType.DOUBLE) {
                    value.putDouble(i, 0.0);
                } else {
                    value.putLong(i, 0L);
                }
            }
            component.restoreStateFrom(image, 0, value, 0);
            for (int i = 0, n = component.getSlotCount(); i < n; i++) {
                if (component.getSlotColumnType(i) == ColumnType.DOUBLE) {
                    Assert.assertEquals(17.5 + i, value.getDouble(i), 0.0);
                } else {
                    Assert.assertEquals(3L + i, value.getLong(i));
                }
            }
        } finally {
            Misc.free(scratch);
        }
    }

    /**
     * The base every compiled golden below is keyed against. The column order is part of
     * the golden bytes: a component identity carries its argument's base column index, so
     * {@code k} at 1, {@code x} at 2, {@code y} at 3 and {@code d} at 4 are what the
     * pinned identities name.
     */
    private void createGoldenTable() throws Exception {
        execute("create table gold (ts timestamp, k symbol, x double, y long, d decimal(18,3)) "
                + "timestamp(ts) partition by day wal");
    }

    @FunctionalInterface
    private interface PlanCheck {
        void run(LiveViewWindowStatePlan plan);
    }
}
