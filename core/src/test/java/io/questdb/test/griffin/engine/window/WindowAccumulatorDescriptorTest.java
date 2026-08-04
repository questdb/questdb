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
import io.questdb.std.Decimals;
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
    public void testTheDecimalExtremumFamiliesKeepTheirArgumentsOwnWidth() {
        // The first families whose layout is a function of the argument and not only of the
        // family: a DECIMAL max accumulates at its argument's own width, so the slot is a LONG
        // for the four narrow widths - which is what those implementations store - and the
        // argument's own type for the two wide ones. Three things follow, and none of them is
        // visible in a rendered row.
        final int[] types = {
                ColumnType.getDecimalType(2, 1),
                ColumnType.getDecimalType(4, 1),
                ColumnType.getDecimalType(9, 3),
                ColumnType.getDecimalType(18, 2),
                ColumnType.getDecimalType(38, 6),
                ColumnType.getDecimalType(60, 0),
        };
        final int[] slotTypes = {
                ColumnType.LONG,
                ColumnType.LONG,
                ColumnType.LONG,
                ColumnType.LONG,
                ColumnType.DECIMAL128,
                ColumnType.DECIMAL256,
        };
        final long[] identities = {
                Decimals.DECIMAL8_NULL,
                Decimals.DECIMAL16_NULL,
                Decimals.DECIMAL32_NULL,
                Decimals.DECIMAL64_NULL,
                0,
                0,
        };
        final ObjList<WindowAccumulatorDescriptor> extrema = new ObjList<>();
        for (int i = 0; i < types.length; i++) {
            for (int direction = 0; direction < 2; direction++) {
                final int family = direction == 0
                        ? WindowAccumulatorDescriptor.FAMILY_DECIMAL_MAX
                        : WindowAccumulatorDescriptor.FAMILY_DECIMAL_MIN;
                final WindowAccumulatorDescriptor component = WindowAccumulatorDescriptor.of(family, 2, types[i]);
                Assert.assertNotNull(component);
                extrema.add(component);
                final String what = "family " + family + " over " + ColumnType.nameOf(types[i]);
                // One slot, and it is the extremum's - an extremum keeps no counter whatever
                // its width.
                Assert.assertEquals(what, 1, component.getSlotCount());
                Assert.assertEquals(what, 0, component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_EXTREMUM));
                Assert.assertEquals(what, -1, component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_SUM));
                Assert.assertEquals(what, slotTypes[i], component.getSlotColumnType(0));
                if (slotTypes[i] == ColumnType.LONG) {
                    // The identity is this width's own null sentinel, which is what the
                    // implementation compares the slot back against - and Byte.MIN_VALUE is
                    // that for a DECIMAL8 and an ordinary payload for every wider width, which
                    // is why one sentinel for all four would be wrong.
                    Assert.assertEquals(what + ": identity", identities[i], component.getSlotIdentityBits(0));
                } else {
                    // A wide slot's identity is the SQL NULL of its own type, which is not one
                    // word - so it is written by resetState and refused here rather than
                    // quietly answered.
                    try {
                        component.getSlotIdentityBits(0);
                        Assert.fail(what + ": a wide DECIMAL slot has no one-word identity");
                    } catch (UnsupportedOperationException expected) {
                        // as required
                    }
                }
                // Runtime-only: no component codec, and so no durable wrapper and no manifest.
                Assert.assertEquals(what, -1, LiveViewAccumulatorDescriptor.familyCodecVersion(family));
                Assert.assertNull(what, LiveViewAccumulatorDescriptor.of(component));
                Assert.assertTrue(what, WindowAccumulatorProjection.isCompatible(
                        family,
                        WindowAccumulatorProjection.PROJECTION_EXTREMUM
                ));
                // It keeps no counter, so nothing may read one off it.
                Assert.assertFalse(what, WindowAccumulatorProjection.isCompatible(
                        family,
                        WindowAccumulatorProjection.PROJECTION_COUNT
                ));
            }
        }
        // Twelve components, no two of them the same and none a run inside another: the width
        // and the direction each keep them apart, and a max is not a slice of anything.
        for (int i = 0, n = extrema.size(); i < n; i++) {
            for (int j = 0; j < n; j++) {
                Assert.assertEquals(
                        "component " + i + " containing component " + j,
                        i == j ? 0 : -1,
                        extrema.getQuick(i).derivedSlotOffset(extrema.getQuick(j))
                );
            }
        }
        // A narrow DECIMAL extremum and a 64-bit one land in the same LONG slot and are still
        // two states: one keeps a scaled payload whose absent value is its width's sentinel and
        // the other a raw word whose absent value is LONG_NULL.
        final WindowAccumulatorDescriptor decimal64Max = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DECIMAL_MAX,
                2,
                ColumnType.getDecimalType(18, 2)
        );
        final WindowAccumulatorDescriptor longMax = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_LONG_MAX,
                2,
                ColumnType.LONG
        );
        Assert.assertNotNull(decimal64Max);
        Assert.assertNotNull(longMax);
        Assert.assertEquals(ColumnType.LONG, decimal64Max.getSlotColumnType(0));
        Assert.assertEquals(ColumnType.LONG, longMax.getSlotColumnType(0));
        Assert.assertFalse(decimal64Max.isSameIdentity(longMax));
        Assert.assertEquals(-1, decimal64Max.derivedSlotOffset(longMax));
        Assert.assertEquals(-1, longMax.derivedSlotOffset(decimal64Max));
        // A non-DECIMAL argument reaches a different max() implementation keeping a different
        // state, so the family declines it outright rather than describing it.
        Assert.assertEquals(
                WindowAccumulatorDescriptor.CONTRIBUTION_NONE,
                WindowAccumulatorDescriptor.contributionKindFor(
                        WindowAccumulatorDescriptor.FAMILY_DECIMAL_MAX,
                        ColumnType.DOUBLE
                )
        );
        Assert.assertNull(WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DECIMAL_MIN,
                2,
                ColumnType.LONG
        ));
    }

    @Test
    public void testTheRingBackedFamiliesKeepPartOfTheirStateOutsideTheValue() {
        // The bounded-ROWS families are the first whose state is not wholly in the map value: the
        // slice carries the frame's accumulator and the ring's address, and the ring itself lives
        // in the contributing function's own arena. Four things follow that no rendered row shows.
        final WindowAccumulatorDescriptor rowsSum = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_ROWS_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        final WindowAccumulatorDescriptor rowsCount = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_ROWS_NON_NULL_COUNT,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertNotNull(rowsSum);
        Assert.assertNotNull(rowsCount);

        // The layout, which is the accumulator a cumulative component would keep plus the two
        // slots that address the ring.
        Assert.assertEquals(4, rowsSum.getSlotCount());
        Assert.assertEquals(0, rowsSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_SUM));
        Assert.assertEquals(1, rowsSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT));
        Assert.assertEquals(2, rowsSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_INDEX));
        Assert.assertEquals(3, rowsSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_OFFSET));
        Assert.assertEquals(ColumnType.DOUBLE, rowsSum.getSlotColumnType(0));
        Assert.assertEquals(3, rowsCount.getSlotCount());
        Assert.assertEquals(0, rowsCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT));
        Assert.assertEquals(1, rowsCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_INDEX));
        Assert.assertEquals(2, rowsCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_OFFSET));
        Assert.assertEquals(-1, rowsCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_SUM));

        final ObjList<WindowAccumulatorDescriptor> ringBacked = new ObjList<>();
        ringBacked.add(rowsSum);
        ringBacked.add(rowsCount);
        for (int i = 0, n = ringBacked.size(); i < n; i++) {
            final WindowAccumulatorDescriptor component = ringBacked.getQuick(i);
            final String what = "family " + component.getFamily();
            Assert.assertTrue(what, component.isRingBacked());
            final int ringOffsetSlot = component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_OFFSET);
            for (int slot = 0, slots = component.getSlotCount(); slot < slots; slot++) {
                if (slot != ringOffsetSlot) {
                    // A bounded total and a bounded counter both start at zero and mean it, and
                    // so does the ring's index.
                    Assert.assertEquals(what + ": slot " + slot, 0L, component.getSlotIdentityBits(slot));
                } else {
                    // The one slot in this build whose identity is not a value the arithmetic
                    // could produce: zero is the first partition's perfectly ordinary ring
                    // address, so an absent ring has to say so out of band.
                    Assert.assertEquals(
                            what + ": ring offset",
                            WindowAccumulatorDescriptor.RING_STATE_UNALLOCATED,
                            component.getSlotIdentityBits(slot)
                    );
                    Assert.assertEquals(ColumnType.LONG, component.getSlotColumnType(slot));
                }
            }
            // Runtime-only: no component codec, so no durable wrapper, no manifest, and a live
            // view keeps such a function residual with its own map and its own ring.
            Assert.assertEquals(what, -1, LiveViewAccumulatorDescriptor.familyCodecVersion(component.getFamily()));
            Assert.assertNull(what, LiveViewAccumulatorDescriptor.of(component));
            // The slots would copy cleanly and the copy would address another function's arena,
            // so the move is refused rather than half-performed. The refusal is decided before
            // either value is read, which is what lets this ask for it without one.
            try {
                component.copyState(null, 0, null, 0);
                Assert.fail(what + ": a ring-backed state must not move between maps");
            } catch (UnsupportedOperationException expected) {
                // as required
            }
        }

        // Nothing folds, in either direction. The bounded count's answer really is the counter
        // the bounded sum keeps beside its total - so this is the one decline in the table that
        // costs a group a slot it could have shared - and what the relation licenses is wider
        // than the arithmetic: the guest's whole state would have to be a run inside the host's,
        // and this guest's continues into a ring of flags where the host's holds doubles.
        Assert.assertEquals(-1, rowsSum.derivedSlotOffset(rowsCount));
        Assert.assertEquals(-1, rowsCount.derivedSlotOffset(rowsSum));
        Assert.assertEquals(0, rowsSum.derivedSlotOffset(rowsSum));
        // Nor against the cumulative families, which agree with these on the argument and the
        // contribution predicate and differ in the one thing that matters: a cumulative frame
        // never gives a row back.
        final ObjList<LiveViewAccumulatorDescriptor> durable = components();
        for (int i = 0, n = ringBacked.size(); i < n; i++) {
            final WindowAccumulatorDescriptor component = ringBacked.getQuick(i);
            for (int j = 0, m = durable.size(); j < m; j++) {
                final WindowAccumulatorDescriptor other = durable.getQuick(j).getRuntime();
                Assert.assertFalse(component.isSameIdentity(other));
                Assert.assertEquals(-1, component.derivedSlotOffset(other));
                Assert.assertEquals(-1, other.derivedSlotOffset(component));
            }
        }

        // The readings each family admits. A bounded (sum, count) serves a sum and an avg, which
        // is the merge this step is for; a bounded counter serves a count. What a bounded sum does
        // not serve is a count, and that absence is the second lock on the decline above: the
        // fold could not produce the binding and the compatibility table would not accept it.
        Assert.assertTrue(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_ROWS_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_SUM
        ));
        Assert.assertTrue(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_ROWS_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_AVG
        ));
        Assert.assertFalse(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_ROWS_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_COUNT
        ));
        Assert.assertTrue(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_ROWS_NON_NULL_COUNT,
                WindowAccumulatorProjection.PROJECTION_COUNT
        ));
        Assert.assertFalse(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_ROWS_NON_NULL_COUNT,
                WindowAccumulatorProjection.PROJECTION_SUM
        ));

        // The predicates are the cumulative families' own, one argument type at a time, because
        // one class serves every bounded-ROWS count and applies the very lambda the cumulative
        // one does. A type neither admits is declined by both.
        final int[] argumentTypes = {
                ColumnType.DOUBLE,
                ColumnType.LONG,
                ColumnType.SYMBOL,
                ColumnType.VARCHAR,
                ColumnType.getDecimalType(18, 3),
                ColumnType.CHAR,
        };
        for (int i = 0; i < argumentTypes.length; i++) {
            final String what = ColumnType.nameOf(argumentTypes[i]);
            Assert.assertEquals(
                    what,
                    WindowAccumulatorDescriptor.contributionKindFor(
                            WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                            argumentTypes[i]
                    ),
                    WindowAccumulatorDescriptor.contributionKindFor(
                            WindowAccumulatorDescriptor.FAMILY_ROWS_NON_NULL_COUNT,
                            argumentTypes[i]
                    )
            );
            Assert.assertEquals(
                    what,
                    WindowAccumulatorDescriptor.contributionKindFor(
                            WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                            argumentTypes[i]
                    ),
                    WindowAccumulatorDescriptor.contributionKindFor(
                            WindowAccumulatorDescriptor.FAMILY_DOUBLE_ROWS_SUM_COUNT,
                            argumentTypes[i]
                    )
            );
        }
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
