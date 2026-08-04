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

package io.questdb.test.griffin.engine.window;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.lv.LiveViewAccumulatorDescriptor;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.constants.IntConstant;
import io.questdb.griffin.engine.window.WindowAccumulatorDescriptor;
import io.questdb.griffin.engine.window.WindowAccumulatorProjection;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.junit.Assert;
import org.junit.Test;

/**
 * The seams between a runtime accumulator component and the live-view wrapper that
 * persists it.
 * <p>
 * {@link WindowAccumulatorDescriptor} owns the family, contribution and slot tables and
 * {@link LiveViewAccumulatorDescriptor} owns the codec, the encoded identity and the byte
 * layout. Three things the durable side no longer states for itself follow from the
 * runtime side instead, and each is load-bearing for a persisted format:
 * <ul>
 *     <li>the durable width and every field offset, which follow the slot layout because
 *     the component codec writes one 64-bit field per slot;</li>
 *     <li>the canonical component order, which a plan takes from the runtime comparison
 *     and a manifest writes in encoded-byte order;</li>
 *     <li>containment, whose family-pair table is the runtime's and whose codec pinning
 *     is the durable side's.</li>
 * </ul>
 * A divergence in any of them would move a manifest offset or fold two components whose
 * images do not nest, which the golden bytes in
 * {@code LiveViewWindowStateGoldenEncodingTest} would only catch for the shapes it pins.
 * These cases hold the derivations themselves, over every component the two classes
 * admit.
 */
public class WindowAccumulatorDescriptorTest {

    @Test
    public void testContainmentAgreesInSlotsAndBytes() {
        final ObjList<LiveViewAccumulatorDescriptor> components = components();
        for (int i = 0, n = components.size(); i < n; i++) {
            for (int j = 0; j < n; j++) {
                final LiveViewAccumulatorDescriptor host = components.getQuick(i);
                final LiveViewAccumulatorDescriptor guest = components.getQuick(j);
                final int slot = host.getRuntime().derivedSlotOffset(guest.getRuntime());
                final String what = label(host) + " containing " + label(guest);
                // Every family in this build is at the codec version the containment was
                // proved at, so the durable answer withholds nothing the runtime allows.
                Assert.assertEquals(what + ": slots", slot, host.derivedSlotOffset(guest));
                Assert.assertEquals(
                        what + ": bytes",
                        slot < 0 ? -1 : slot * Long.BYTES,
                        host.derivedStateOffset(guest)
                );
                if (slot >= 0) {
                    // A guest's whole state has to fit inside the host from that offset, or
                    // the fold would hand its decoder a neighbour's bytes.
                    Assert.assertTrue(
                            what + ": the guest must fit",
                            slot + guest.getSlotCount() <= host.getSlotCount()
                    );
                }
            }
        }
    }

    @Test
    public void testDirectColumnIndexResolvesOnlyAColumnOfItsOwnType() {
        final GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP));
        metadata.add(new TableColumnMetadata("i", ColumnType.INT));
        metadata.add(new TableColumnMetadata("l", ColumnType.LONG));

        Assert.assertEquals(1, WindowAccumulatorDescriptor.directColumnIndex(IntColumn.newInstance(1), metadata));
        Assert.assertEquals(2, WindowAccumulatorDescriptor.directColumnIndex(LongColumn.newInstance(2), metadata));
        // A column function whose type is not the column's own reads through something the
        // component identity would then key by the wrong contribution semantics.
        Assert.assertEquals(-1, WindowAccumulatorDescriptor.directColumnIndex(LongColumn.newInstance(1), metadata));
        Assert.assertEquals(-1, WindowAccumulatorDescriptor.directColumnIndex(IntColumn.newInstance(9), metadata));
        // An expression is not a direct column reference, and neither is nothing at all.
        Assert.assertEquals(-1, WindowAccumulatorDescriptor.directColumnIndex(IntConstant.newInstance(1), metadata));
        Assert.assertEquals(-1, WindowAccumulatorDescriptor.directColumnIndex(null, metadata));
    }

    @Test
    public void testTheKahanSumLendsOnlyItsCounter() {
        // The compensated sum is the case that says a component's identity is the arithmetic
        // and not the layout. It agrees with a plain sum on the argument, on the contribution
        // predicate and on the zero identity, and differs in exactly one thing no width or
        // slot type records: the totals are different numbers over the same rows.
        final WindowAccumulatorDescriptor kahan = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertNotNull(kahan);
        Assert.assertEquals(3, kahan.getSlotCount());
        Assert.assertEquals(0, kahan.getFieldSlot(WindowAccumulatorDescriptor.FIELD_SUM));
        Assert.assertEquals(1, kahan.getFieldSlot(WindowAccumulatorDescriptor.FIELD_KAHAN_COMPENSATION));
        Assert.assertEquals(2, kahan.getFieldSlot(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT));
        Assert.assertEquals(ColumnType.DOUBLE, kahan.getSlotColumnType(0));
        Assert.assertEquals(ColumnType.DOUBLE, kahan.getSlotColumnType(1));
        Assert.assertEquals(ColumnType.LONG, kahan.getSlotColumnType(2));
        // Zero, and meant: a compensated sum starts empty at (0, 0, 0), which is where this
        // family parts company with the extremum ones admitted beside it.
        for (int slot = 0; slot < 3; slot++) {
            Assert.assertEquals("slot " + slot, 0L, kahan.getSlotIdentityBits(slot));
        }
        // Runtime-only: no component codec, and so no durable wrapper and no manifest.
        Assert.assertEquals(
                -1,
                LiveViewAccumulatorDescriptor.familyCodecVersion(WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT)
        );
        Assert.assertNull(LiveViewAccumulatorDescriptor.of(kahan));

        final WindowAccumulatorDescriptor count = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                2,
                ColumnType.DOUBLE
        );
        final WindowAccumulatorDescriptor sum = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        final WindowAccumulatorDescriptor otherCount = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                3,
                ColumnType.DOUBLE
        );
        Assert.assertNotNull(count);
        Assert.assertNotNull(sum);
        Assert.assertNotNull(otherCount);
        // The counter is a run inside it at the slot the compensation term displaced it to.
        Assert.assertEquals(2, kahan.derivedSlotOffset(count));
        // A counter over another column is another counter, whatever it is stored beside.
        Assert.assertEquals(-1, kahan.derivedSlotOffset(otherCount));
        // Neither total is readable out of the other, in either direction, and neither is the
        // Kahan state a run inside the plain one or the other way about.
        Assert.assertEquals(-1, kahan.derivedSlotOffset(sum));
        Assert.assertEquals(-1, sum.derivedSlotOffset(kahan));
        Assert.assertEquals(-1, count.derivedSlotOffset(kahan));
        // The same separation stated where a projection would reach for it: a ksum output
        // reads this family and a sum output cannot, which is the whole reason the two
        // projection kinds are not one.
        Assert.assertTrue(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_KAHAN_SUM
        ));
        Assert.assertFalse(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_SUM
        ));
        Assert.assertFalse(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_KAHAN_SUM
        ));
        // The counter is the one reading that does cross.
        Assert.assertTrue(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_COUNT
        ));
    }

    @Test
    public void testTheExtremumFamiliesAreRuntimeOnlyAndStartAtNull() {
        // The four extremum families are the first this build admits at runtime and not on
        // disk, and the first whose starting state is not a zeroed slice. Both halves matter
        // beyond their own arithmetic: a durable descriptor for a family whose codec nothing
        // pinned would ship an unpinned encoding, and a slice reset to zero would read back as
        // "the largest value so far is zero" for a partition nothing has contributed to.
        final ObjList<WindowAccumulatorDescriptor> extrema = new ObjList<>();
        addExtremum(extrema, WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX, 2, ColumnType.DOUBLE);
        addExtremum(extrema, WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN, 2, ColumnType.DOUBLE);
        addExtremum(extrema, WindowAccumulatorDescriptor.FAMILY_LONG_MAX, 3, ColumnType.LONG);
        addExtremum(extrema, WindowAccumulatorDescriptor.FAMILY_LONG_MIN, 3, ColumnType.LONG);
        addExtremum(extrema, WindowAccumulatorDescriptor.FAMILY_LONG_MAX, 0, ColumnType.TIMESTAMP);

        for (int i = 0, n = extrema.size(); i < n; i++) {
            final WindowAccumulatorDescriptor component = extrema.getQuick(i);
            final String what = "family " + component.getFamily()
                    + " over column " + component.getArgumentColumnIndex();
            // One slot, and it is the extremum's - no counter, which is why a bound function's
            // "am I fused" answer is the binding rather than a named field.
            Assert.assertEquals(what, 1, component.getSlotCount());
            Assert.assertEquals(what, 0, component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_EXTREMUM));
            Assert.assertEquals(what, -1, component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_SUM));
            Assert.assertEquals(
                    what,
                    -1,
                    component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT)
            );
            final boolean isDoubleState = component.getFamily() == WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX
                    || component.getFamily() == WindowAccumulatorDescriptor.FAMILY_DOUBLE_MIN;
            Assert.assertEquals(
                    what,
                    isDoubleState ? ColumnType.DOUBLE : ColumnType.LONG,
                    component.getSlotColumnType(0)
            );
            // The identity is the NULL the family's own contribution predicate refuses, so an
            // empty state can never be mistaken for a contributed one.
            Assert.assertEquals(
                    what + ": identity",
                    isDoubleState ? Double.doubleToRawLongBits(Double.NaN) : Numbers.LONG_NULL,
                    component.getSlotIdentityBits(0)
            );
            // Runtime-only: no component codec, and so no durable wrapper and no manifest.
            Assert.assertEquals(what, -1, LiveViewAccumulatorDescriptor.familyCodecVersion(component.getFamily()));
            Assert.assertNull(what, LiveViewAccumulatorDescriptor.of(component));
        }

        // No containment in either direction, against each other or against every durable
        // component this build has - an extremum is neither a run inside anything wider nor
        // wide enough to hold anything.
        final ObjList<LiveViewAccumulatorDescriptor> others = components();
        for (int i = 0, n = extrema.size(); i < n; i++) {
            final WindowAccumulatorDescriptor extremum = extrema.getQuick(i);
            for (int j = 0; j < n; j++) {
                final WindowAccumulatorDescriptor other = extrema.getQuick(j);
                Assert.assertEquals(
                        "extremum " + i + " containing extremum " + j,
                        i == j ? 0 : -1,
                        extremum.derivedSlotOffset(other)
                );
            }
            for (int j = 0, m = others.size(); j < m; j++) {
                final WindowAccumulatorDescriptor other = others.getQuick(j).getRuntime();
                Assert.assertEquals(-1, extremum.derivedSlotOffset(other));
                Assert.assertEquals(-1, other.derivedSlotOffset(extremum));
            }
        }
    }

    @Test
    public void testTheDurableImageIsOneLongPerRuntimeSlot() {
        final IntList fields = new IntList();
        fields.add(WindowAccumulatorDescriptor.FIELD_SUM);
        fields.add(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT);
        fields.add(WindowAccumulatorDescriptor.FIELD_MEAN);
        fields.add(WindowAccumulatorDescriptor.FIELD_M2);
        final ObjList<LiveViewAccumulatorDescriptor> components = components();
        for (int i = 0, n = components.size(); i < n; i++) {
            final LiveViewAccumulatorDescriptor component = components.getQuick(i);
            final String what = label(component);
            final int slots = component.getSlotCount();
            Assert.assertTrue(what, slots > 0);
            Assert.assertEquals(what + ": width", slots * Long.BYTES, component.getStateLength());
            Assert.assertEquals(
                    what + ": width",
                    component.getStateLength(),
                    LiveViewAccumulatorDescriptor.familyStateLength(component.getFamily())
            );
            for (int s = 0; s < slots; s++) {
                // The codec writes each slot through one putLong, so a narrower slot type
                // would leave the image describing fields the layout does not have.
                Assert.assertEquals(
                        what + ": slot " + s,
                        Long.BYTES,
                        ColumnType.sizeOf(component.getSlotColumnType(s))
                );
            }
            for (int f = 0, m = fields.size(); f < m; f++) {
                final int field = fields.getQuick(f);
                final int slot = component.getFieldSlot(field);
                Assert.assertEquals(
                        what + ": field " + field,
                        slot < 0 ? -1 : slot * Long.BYTES,
                        component.getFieldOffset(field)
                );
            }
        }
    }

    @Test
    public void testTheRuntimeOrderAgreesWithTheDurableEncodedOrder() {
        final ObjList<LiveViewAccumulatorDescriptor> components = components();
        for (int i = 0, n = components.size(); i < n; i++) {
            for (int j = 0; j < n; j++) {
                final LiveViewAccumulatorDescriptor left = components.getQuick(i);
                final LiveViewAccumulatorDescriptor right = components.getQuick(j);
                final String what = label(left) + " against " + label(right);
                Assert.assertEquals(
                        what + ": identity",
                        left.isSameIdentity(right),
                        left.getRuntime().isSameIdentity(right.getRuntime())
                );
                // The sign is the whole of what a plan reads: it sorts components by the
                // runtime comparison and the manifest lays them out in encoded-byte order,
                // so a disagreement would be a persisted offset nobody chose.
                Assert.assertEquals(
                        what + ": order",
                        Integer.signum(left.compareIdentity(right)),
                        Integer.signum(left.getRuntime().compareIdentity(right.getRuntime()))
                );
            }
        }
    }

    /**
     * One component per family, plus the argument types that reach a different
     * contribution predicate. The argumentless row count is here too, because its
     * {@code 0xffffffff} argument key is the one place the encoded order is unsigned and
     * the runtime order has to follow it.
     */
    private static ObjList<LiveViewAccumulatorDescriptor> components() {
        final ObjList<LiveViewAccumulatorDescriptor> components = new ObjList<>();
        add(components, WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT, 2, ColumnType.DOUBLE);
        add(components, WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT, 3, ColumnType.LONG);
        add(components, WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD, 2, ColumnType.DOUBLE);
        add(components, WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 2, ColumnType.DOUBLE);
        add(components, WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 3, ColumnType.LONG);
        add(components, WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 1, ColumnType.SYMBOL);
        add(components, WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT, 4, ColumnType.getDecimalType(18, 3));
        add(
                components,
                WindowAccumulatorDescriptor.FAMILY_ROW_COUNT,
                WindowAccumulatorDescriptor.NO_ARGUMENT_COLUMN_INDEX,
                ColumnType.UNDEFINED
        );
        return components;
    }

    private static void addExtremum(
            ObjList<WindowAccumulatorDescriptor> components,
            int family,
            int argumentColumnIndex,
            int argumentColumnType
    ) {
        final WindowAccumulatorDescriptor component = WindowAccumulatorDescriptor.of(
                family,
                argumentColumnIndex,
                argumentColumnType
        );
        Assert.assertNotNull(component);
        components.add(component);
    }

    private static void add(
            ObjList<LiveViewAccumulatorDescriptor> components,
            int family,
            int argumentColumnIndex,
            int argumentColumnType
    ) {
        final LiveViewAccumulatorDescriptor component = LiveViewAccumulatorDescriptor.of(
                family,
                argumentColumnIndex,
                argumentColumnType
        );
        Assert.assertNotNull(component);
        components.add(component);
    }

    private static String label(LiveViewAccumulatorDescriptor component) {
        return "family " + component.getFamily() + " over column " + component.getArgumentColumnIndex();
    }
}
