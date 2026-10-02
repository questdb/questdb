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
                final String what = label(host) + " containing " + label(guest);
                // The slot the fold lands on, stated here rather than read back off the
                // implementation: the durable answer is the runtime one, so comparing the two
                // agrees with a counter that moved as readily as with one that did not.
                final int expected = expectedFoldSlot(host, guest);
                Assert.assertEquals(what + ": slots", expected, host.derivedSlotOffset(guest));
                // Every family in this build is at the codec version the containment was
                // proved at, so the durable answer withholds nothing the runtime allows. That
                // is what this comparison is for - a family whose codec version moved would
                // leave the durable side at -1 while the runtime side still folded.
                Assert.assertEquals(
                        what + ": runtime slots",
                        expected,
                        host.getRuntime().derivedSlotOffset(guest.getRuntime())
                );
                if (expected < 0) {
                    Assert.assertEquals(what + ": bytes", -1, host.derivedStateOffset(guest));
                } else {
                    // A guest's whole state has to fit inside the host from that offset, or
                    // the fold would hand its decoder a neighbour's bytes.
                    Assert.assertTrue(
                            what + ": the guest must fit",
                            expected + guest.getSlotCount() <= host.getSlotCount()
                    );
                }
            }
        }

        // The byte offsets a guest's decoder reads its own image at, as literals. Deriving them
        // from the slots above would restate the codec's one-word-per-slot rule by the very
        // multiplication the implementation performs, and so would hold for any width it chose.
        final LiveViewAccumulatorDescriptor sumDouble = LiveViewAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        final LiveViewAccumulatorDescriptor countDouble = LiveViewAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                2,
                ColumnType.DOUBLE
        );
        final LiveViewAccumulatorDescriptor countLong = LiveViewAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                3,
                ColumnType.LONG
        );
        final LiveViewAccumulatorDescriptor welford = LiveViewAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD,
                2,
                ColumnType.DOUBLE
        );
        final LiveViewAccumulatorDescriptor kahan = LiveViewAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertNotNull(sumDouble);
        Assert.assertNotNull(countDouble);
        Assert.assertNotNull(countLong);
        Assert.assertNotNull(welford);
        Assert.assertNotNull(kahan);
        // The whole of a component is at zero inside itself.
        Assert.assertEquals(0, sumDouble.derivedStateOffset(sumDouble));
        // (sum, count) keeps the counter behind the total.
        Assert.assertEquals(Long.BYTES, sumDouble.derivedStateOffset(countDouble));
        // Welford's (mean, m2, count) and the Kahan (sum, compensation, count) keep it behind
        // two words rather than one.
        Assert.assertEquals(2 * Long.BYTES, welford.derivedStateOffset(countDouble));
        Assert.assertEquals(2 * Long.BYTES, kahan.derivedStateOffset(countDouble));
        // A counter holds no total, and a counter over another column is another counter.
        Assert.assertEquals(-1, countDouble.derivedStateOffset(sumDouble));
        Assert.assertEquals(-1, sumDouble.derivedStateOffset(countLong));
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
        // Durable as well as runtime: the implementation writes these three slots in this
        // order as three little-endian words, which is the component codec's image exactly,
        // so the family has a codec and a manifest names it.
        Assert.assertEquals(
                1,
                LiveViewAccumulatorDescriptor.familyCodecVersion(WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT)
        );
        final LiveViewAccumulatorDescriptor durableKahan = LiveViewAccumulatorDescriptor.of(kahan);
        Assert.assertNotNull(durableKahan);
        Assert.assertEquals(3 * Long.BYTES, durableKahan.getStateLength());

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
    public void testTheBoundedRangeFamiliesCarryTheirRingsGeometry() {
        // The RANGE spelling of the pair above, and the one thing about it that is not the ROWS
        // one's: a RANGE frame's length is the data's answer rather than the query's, so the ring
        // grows on demand and the slice carries how long it is and how long it can be beside where
        // it starts. Two more slots each, and everything else about being ring-backed unchanged.
        final WindowAccumulatorDescriptor rangeSum = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_RANGE_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        final WindowAccumulatorDescriptor rangeCount = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_RANGE_NON_NULL_COUNT,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertNotNull(rangeSum);
        Assert.assertNotNull(rangeCount);

        Assert.assertEquals(6, rangeSum.getSlotCount());
        Assert.assertEquals(0, rangeSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_SUM));
        Assert.assertEquals(1, rangeSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT));
        Assert.assertEquals(2, rangeSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_INDEX));
        Assert.assertEquals(3, rangeSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_OFFSET));
        Assert.assertEquals(4, rangeSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_SIZE));
        Assert.assertEquals(5, rangeSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_CAPACITY));
        Assert.assertEquals(ColumnType.DOUBLE, rangeSum.getSlotColumnType(0));
        Assert.assertEquals(5, rangeCount.getSlotCount());
        Assert.assertEquals(0, rangeCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT));
        Assert.assertEquals(1, rangeCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_INDEX));
        Assert.assertEquals(2, rangeCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_OFFSET));
        Assert.assertEquals(3, rangeCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_SIZE));
        Assert.assertEquals(4, rangeCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_CAPACITY));
        Assert.assertEquals(-1, rangeCount.getFieldSlot(WindowAccumulatorDescriptor.FIELD_SUM));
        // The two ring-geometry fields belong to these families and to no other: the ROWS ring is
        // as long as the query says, so its slice names neither.
        final WindowAccumulatorDescriptor rowsSum = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_ROWS_SUM_COUNT,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertNotNull(rowsSum);
        Assert.assertEquals(-1, rowsSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_SIZE));
        Assert.assertEquals(-1, rowsSum.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_CAPACITY));

        final ObjList<WindowAccumulatorDescriptor> ringBacked = new ObjList<>();
        ringBacked.add(rangeSum);
        ringBacked.add(rangeCount);
        for (int i = 0, n = ringBacked.size(); i < n; i++) {
            final WindowAccumulatorDescriptor component = ringBacked.getQuick(i);
            final String what = "family " + component.getFamily();
            Assert.assertTrue(what, component.isRingBacked());
            final int ringOffsetSlot = component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_RING_OFFSET);
            for (int slot = 0, slots = component.getSlotCount(); slot < slots; slot++) {
                if (slot != ringOffsetSlot) {
                    // A total, a counter, a read cursor, a length and a capacity all start at zero
                    // and mean it: an unallocated ring holds nothing and can hold nothing.
                    Assert.assertEquals(what + ": slot " + slot, 0L, component.getSlotIdentityBits(slot));
                } else {
                    Assert.assertEquals(
                            what + ": ring offset",
                            WindowAccumulatorDescriptor.RING_STATE_UNALLOCATED,
                            component.getSlotIdentityBits(slot)
                    );
                    Assert.assertEquals(ColumnType.LONG, component.getSlotColumnType(slot));
                }
            }
            Assert.assertEquals(what, -1, LiveViewAccumulatorDescriptor.familyCodecVersion(component.getFamily()));
            Assert.assertNull(what, LiveViewAccumulatorDescriptor.of(component));
            try {
                component.copyState(null, 0, null, 0);
                Assert.fail(what + ": a ring-backed state must not move between maps");
            } catch (UnsupportedOperationException expected) {
                // as required
            }
        }

        // Nothing folds, in either direction, for the reason the ROWS pair does not - and here a
        // second reason stands behind it: a RANGE counter's five slots are not even a run inside a
        // RANGE (sum, count)'s six, because the host keeps a total in front of its counter.
        Assert.assertEquals(-1, rangeSum.derivedSlotOffset(rangeCount));
        Assert.assertEquals(-1, rangeCount.derivedSlotOffset(rangeSum));
        Assert.assertEquals(0, rangeSum.derivedSlotOffset(rangeSum));
        // Nor across the frames. A bounded ROWS state and a bounded RANGE one over one column
        // agree on the argument and the contribution predicate and are still two states, which is
        // what the two families being separate says; a group never puts them together anyway.
        final WindowAccumulatorDescriptor rowsCount = WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_ROWS_NON_NULL_COUNT,
                2,
                ColumnType.DOUBLE
        );
        Assert.assertNotNull(rowsCount);
        final ObjList<WindowAccumulatorDescriptor> others = new ObjList<>();
        others.add(rowsSum);
        others.add(rowsCount);
        final ObjList<LiveViewAccumulatorDescriptor> durable = components();
        for (int j = 0, m = durable.size(); j < m; j++) {
            others.add(durable.getQuick(j).getRuntime());
        }
        for (int i = 0, n = ringBacked.size(); i < n; i++) {
            final WindowAccumulatorDescriptor component = ringBacked.getQuick(i);
            for (int j = 0, m = others.size(); j < m; j++) {
                final WindowAccumulatorDescriptor other = others.getQuick(j);
                Assert.assertFalse(component.isSameIdentity(other));
                Assert.assertEquals(-1, component.derivedSlotOffset(other));
                Assert.assertEquals(-1, other.derivedSlotOffset(component));
            }
        }

        // The readings each family admits, and the one it must not: a bounded RANGE (sum, count)
        // serves a sum and an avg and never a count, which is the second lock on the decline above.
        Assert.assertTrue(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_RANGE_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_SUM
        ));
        Assert.assertTrue(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_RANGE_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_AVG
        ));
        Assert.assertFalse(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_RANGE_SUM_COUNT,
                WindowAccumulatorProjection.PROJECTION_COUNT
        ));
        Assert.assertTrue(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_RANGE_NON_NULL_COUNT,
                WindowAccumulatorProjection.PROJECTION_COUNT
        ));
        Assert.assertFalse(WindowAccumulatorProjection.isCompatible(
                WindowAccumulatorDescriptor.FAMILY_RANGE_NON_NULL_COUNT,
                WindowAccumulatorProjection.PROJECTION_SUM
        ));

        // The predicates are the cumulative families' own, one argument type at a time: one class
        // serves every bounded-RANGE count and applies the very lambda the cumulative one does.
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
                            WindowAccumulatorDescriptor.FAMILY_RANGE_NON_NULL_COUNT,
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
                            WindowAccumulatorDescriptor.FAMILY_DOUBLE_RANGE_SUM_COUNT,
                            argumentTypes[i]
                    )
            );
        }
    }

    @Test
    public void testTheExtremumFamiliesPersistOneSlotThatStartsAtNull() {
        // The four extremum families are the first whose starting state is not a zeroed
        // slice, which matters beyond their own arithmetic: a slice reset to zero would read
        // back as "the largest value so far is zero" for a partition nothing has contributed
        // to. They are durable as well as runtime components - one slot, one 64-bit field,
        // which is exactly the image their own freezeCheckpointState writes.
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
            // Durable: one component codec, at the version the containment table was proved
            // at, and a wrapper whose whole image is that one slot.
            Assert.assertEquals(what, 1, LiveViewAccumulatorDescriptor.familyCodecVersion(component.getFamily()));
            final LiveViewAccumulatorDescriptor durable = LiveViewAccumulatorDescriptor.of(component);
            Assert.assertNotNull(what, durable);
            Assert.assertEquals(what, Long.BYTES, durable.getStateLength());
            Assert.assertEquals(what, 0, durable.getFieldOffset(WindowAccumulatorDescriptor.FIELD_EXTREMUM));
            Assert.assertEquals(what, -1, durable.getFieldOffset(WindowAccumulatorDescriptor.FIELD_SUM));
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
    public void testTheCaptureFamiliesKeepOneRowsValueAndSayWhichRow() {
        // The six capture families, and the four claims about them no rendered row shows: the
        // layouts, the identities, that the flag belongs to three of the six and not to the
        // others, and that nothing folds in any direction. The last is the one worth stating
        // rather than assuming - a first_value(x) ignore nulls keeps one slot that a
        // first_value(x) appears to contain, and the two hold different rows' values.
        final ObjList<WindowAccumulatorDescriptor> captures = new ObjList<>();
        addExtremum(captures, WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_VALUE, 2, ColumnType.DOUBLE);
        addExtremum(captures, WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE, 2, ColumnType.DOUBLE);
        addExtremum(captures, WindowAccumulatorDescriptor.FAMILY_DOUBLE_LAST_NOT_NULL_VALUE, 2, ColumnType.DOUBLE);
        addExtremum(captures, WindowAccumulatorDescriptor.FAMILY_LONG_FIRST_VALUE, 3, ColumnType.LONG);
        addExtremum(captures, WindowAccumulatorDescriptor.FAMILY_LONG_FIRST_NOT_NULL_VALUE, 3, ColumnType.LONG);
        addExtremum(captures, WindowAccumulatorDescriptor.FAMILY_LONG_LAST_NOT_NULL_VALUE, 0, ColumnType.TIMESTAMP);

        for (int i = 0, n = captures.size(); i < n; i++) {
            final WindowAccumulatorDescriptor component = captures.getQuick(i);
            final int family = component.getFamily();
            final String what = "family " + family + " over column " + component.getArgumentColumnIndex();
            final boolean flagged = family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_VALUE
                    || family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_LAST_NOT_NULL_VALUE
                    || family == WindowAccumulatorDescriptor.FAMILY_LONG_FIRST_VALUE
                    || family == WindowAccumulatorDescriptor.FAMILY_LONG_LAST_NOT_NULL_VALUE;
            final boolean isDoubleState = family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_VALUE
                    || family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE
                    || family == WindowAccumulatorDescriptor.FAMILY_DOUBLE_LAST_NOT_NULL_VALUE;
            Assert.assertEquals(what, flagged ? 2 : 1, component.getSlotCount());
            Assert.assertEquals(
                    what,
                    0,
                    component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_CAPTURED_VALUE)
            );
            // The flag is the two respect-the-first-row layouts' and nobody else's: an IGNORE
            // NULLS first value reads its emptiness off the value slot instead.
            Assert.assertEquals(
                    what + ": flag",
                    flagged ? 1 : -1,
                    component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_CAPTURED)
            );
            Assert.assertEquals(what, -1, component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_SUM));
            Assert.assertEquals(
                    what,
                    -1,
                    component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_NON_NULL_COUNT)
            );
            Assert.assertEquals(what, -1, component.getFieldSlot(WindowAccumulatorDescriptor.FIELD_EXTREMUM));
            Assert.assertEquals(
                    what,
                    isDoubleState ? ColumnType.DOUBLE : ColumnType.LONG,
                    component.getSlotColumnType(0)
            );
            // The captured value starts at its own state type's NULL - which is what a
            // projection reads straight for a partition nothing was captured from - and the flag
            // starts at zero, meaning "this slice has not been written".
            Assert.assertEquals(
                    what + ": identity",
                    isDoubleState ? Double.doubleToRawLongBits(Double.NaN) : Numbers.LONG_NULL,
                    component.getSlotIdentityBits(0)
            );
            if (flagged) {
                Assert.assertEquals(what + ": flag type", ColumnType.LONG, component.getSlotColumnType(1));
                Assert.assertEquals(what + ": flag identity", 0L, component.getSlotIdentityBits(1));
            }
            // The whole state is in the map value: no ring, unlike the bounded families, so a
            // group owning the map owns the whole of a fused capture.
            Assert.assertFalse(what, component.isRingBacked());
            // Runtime-only: no component codec, and so no durable wrapper and no manifest.
            Assert.assertEquals(what, -1, LiveViewAccumulatorDescriptor.familyCodecVersion(family));
            Assert.assertNull(what, LiveViewAccumulatorDescriptor.of(component));
            // The one projection kind that reads them, and the ones that must not.
            Assert.assertTrue(
                    what,
                    WindowAccumulatorProjection.isCompatible(
                            family,
                            WindowAccumulatorProjection.PROJECTION_CAPTURED_VALUE
                    )
            );
            Assert.assertFalse(
                    what,
                    WindowAccumulatorProjection.isCompatible(family, WindowAccumulatorProjection.PROJECTION_SUM)
            );
            Assert.assertFalse(
                    what,
                    WindowAccumulatorProjection.isCompatible(family, WindowAccumulatorProjection.PROJECTION_COUNT)
            );
            Assert.assertFalse(
                    what,
                    WindowAccumulatorProjection.isCompatible(
                            family,
                            WindowAccumulatorProjection.PROJECTION_EXTREMUM
                    )
            );
        }

        // A capture kind reads no other family, which is the other half of the compatibility
        // table: the six above and nothing else.
        final ObjList<LiveViewAccumulatorDescriptor> others = components();
        for (int j = 0, m = others.size(); j < m; j++) {
            Assert.assertFalse(
                    WindowAccumulatorProjection.isCompatible(
                            others.getQuick(j).getFamily(),
                            WindowAccumulatorProjection.PROJECTION_CAPTURED_VALUE
                    )
            );
        }

        // No containment in either direction, against each other or against every durable
        // component this build has. The three DOUBLE captures share an argument and a slot type
        // and are still three states, because they capture three different rows.
        for (int i = 0, n = captures.size(); i < n; i++) {
            final WindowAccumulatorDescriptor capture = captures.getQuick(i);
            for (int j = 0; j < n; j++) {
                Assert.assertEquals(
                        "capture " + i + " containing capture " + j,
                        i == j ? 0 : -1,
                        capture.derivedSlotOffset(captures.getQuick(j))
                );
            }
            for (int j = 0, m = others.size(); j < m; j++) {
                final WindowAccumulatorDescriptor other = others.getQuick(j).getRuntime();
                Assert.assertEquals(-1, capture.derivedSlotOffset(other));
                Assert.assertEquals(-1, other.derivedSlotOffset(capture));
            }
        }

        // The predicates: the respect-nulls families take every row and the IGNORE NULLS ones
        // apply the same test their own implementation does - isFinite through the DOUBLE
        // reading, the payload's own null test through the 64-bit one. Compared arm for arm
        // rather than restated, so a table that stopped agreeing with the extremum families'
        // fails here.
        Assert.assertEquals(
                WindowAccumulatorDescriptor.CONTRIBUTION_EVERY_ROW,
                WindowAccumulatorDescriptor.contributionKindFor(
                        WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_VALUE,
                        ColumnType.DOUBLE
                )
        );
        Assert.assertEquals(
                WindowAccumulatorDescriptor.CONTRIBUTION_EVERY_ROW,
                WindowAccumulatorDescriptor.contributionKindFor(
                        WindowAccumulatorDescriptor.FAMILY_LONG_FIRST_VALUE,
                        ColumnType.TIMESTAMP
                )
        );
        final int[] doubleFamilies = {
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_NOT_NULL_VALUE,
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_LAST_NOT_NULL_VALUE,
        };
        final int[] longFamilies = {
                WindowAccumulatorDescriptor.FAMILY_LONG_FIRST_NOT_NULL_VALUE,
                WindowAccumulatorDescriptor.FAMILY_LONG_LAST_NOT_NULL_VALUE,
        };
        final int[] types = {
                ColumnType.DOUBLE,
                ColumnType.LONG,
                ColumnType.TIMESTAMP,
                ColumnType.SYMBOL,
                ColumnType.getDecimalType(18, 3),
        };
        for (int t = 0; t < types.length; t++) {
            for (int f = 0; f < doubleFamilies.length; f++) {
                Assert.assertEquals(
                        "type " + types[t] + " family " + doubleFamilies[f],
                        WindowAccumulatorDescriptor.contributionKindFor(
                                WindowAccumulatorDescriptor.FAMILY_DOUBLE_MAX,
                                types[t]
                        ),
                        WindowAccumulatorDescriptor.contributionKindFor(doubleFamilies[f], types[t])
                );
            }
            for (int f = 0; f < longFamilies.length; f++) {
                Assert.assertEquals(
                        "type " + types[t] + " family " + longFamilies[f],
                        WindowAccumulatorDescriptor.contributionKindFor(
                                WindowAccumulatorDescriptor.FAMILY_LONG_MAX,
                                types[t]
                        ),
                        WindowAccumulatorDescriptor.contributionKindFor(longFamilies[f], types[t])
                );
            }
        }
        // A type no capture implementation is selected for declines outright rather than fusing
        // under a predicate the runtime does not apply.
        Assert.assertNull(WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_DOUBLE_FIRST_VALUE,
                1,
                ColumnType.SYMBOL
        ));
        Assert.assertNull(WindowAccumulatorDescriptor.of(
                WindowAccumulatorDescriptor.FAMILY_LONG_LAST_NOT_NULL_VALUE,
                4,
                ColumnType.getDecimalType(18, 3)
        ));
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
            // The persisted width, as a literal per family rather than as the slot count times
            // the word: the implementation computes it that way, so an expectation that does
            // the same describes whatever layout it produced instead of the one the format
            // was proved at.
            Assert.assertEquals(
                    what + ": width",
                    expectedStateLength(component.getFamily()),
                    component.getStateLength()
            );
            // The same width by the other route the plan checks: off the family table rather
            // than off a built descriptor.
            Assert.assertEquals(
                    what + ": family width",
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
            // The field offsets a restore decodes at, also as literals, and for the same
            // reason: an expectation derived from getFieldSlot would follow the slot table
            // wherever it moved.
            final int[] expectedOffsets = expectedFieldOffsets(component.getFamily());
            for (int f = 0, m = fields.size(); f < m; f++) {
                final int field = fields.getQuick(f);
                Assert.assertEquals(
                        what + ": field " + field,
                        expectedOffsets[f],
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

    /**
     * The byte offsets of {@code FIELD_SUM}, {@code FIELD_NON_NULL_COUNT},
     * {@code FIELD_MEAN} and {@code FIELD_M2} inside one durable component of
     * {@code family}, in that order, or {@code -1} for a field the family does not carry.
     * <p>
     * A restore reads a field out of a persisted image at these offsets, so they are what
     * the format is, and they are written out here rather than derived from the slot table
     * they are derived from in the implementation.
     */
    private static int[] expectedFieldOffsets(int family) {
        return switch (family) {
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT -> new int[]{0, Long.BYTES, -1, -1};
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD -> new int[]{-1, 2 * Long.BYTES, 0, Long.BYTES};
            case WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                 WindowAccumulatorDescriptor.FAMILY_ROW_COUNT -> new int[]{-1, 0, -1, -1};
            default -> throw new AssertionError("no expected field layout for family " + family);
        };
    }

    /**
     * The slot {@code guest}'s whole state begins at inside {@code host}, or {@code -1} when
     * the two do not fold. The proved pairs written out: a fold needs the same argument and
     * the same contribution predicate, which follows the argument's type, and the only guest
     * this build folds is a counter.
     */
    private static int expectedFoldSlot(
            LiveViewAccumulatorDescriptor host,
            LiveViewAccumulatorDescriptor guest
    ) {
        if (host.getArgumentColumnIndex() != guest.getArgumentColumnIndex()
                || host.getArgumentColumnType() != guest.getArgumentColumnType()) {
            return -1;
        }
        if (host.getFamily() == guest.getFamily()) {
            return 0;
        }
        if (guest.getFamily() != WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT) {
            return -1;
        }
        return switch (host.getFamily()) {
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT -> 1;
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_KAHAN_SUM_COUNT,
                 WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD -> 2;
            default -> -1;
        };
    }

    /**
     * The width one durable component of {@code family} persists at. The number a stored
     * image's length is checked against on restore, so it is stated rather than recomputed.
     */
    private static int expectedStateLength(int family) {
        return switch (family) {
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_SUM_COUNT -> 2 * Long.BYTES;
            case WindowAccumulatorDescriptor.FAMILY_DOUBLE_WELFORD -> 3 * Long.BYTES;
            case WindowAccumulatorDescriptor.FAMILY_NON_NULL_COUNT,
                 WindowAccumulatorDescriptor.FAMILY_ROW_COUNT -> Long.BYTES;
            default -> throw new AssertionError("no expected width for family " + family);
        };
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
