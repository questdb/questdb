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

package io.questdb.test.cairo;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.MillisTimestampDriver;
import io.questdb.cairo.NanosTimestampDriver;
import io.questdb.cairo.TimestampDriver;
import org.junit.Assert;
import org.junit.Test;

public class TimestampDriverMaxUnitValueTest {
    private static final char[] UNITS = {'n', 'u', 'U', 'T', 's', 'm', 'h', 'H', 'd', 'w'};

    @Test
    public void testCeilingIsWhereFromStopsBeingFaithful() {
        assertCeilingIsCliff(NanosTimestampDriver.INSTANCE);
        assertCeilingIsCliff(MicrosTimestampDriver.INSTANCE);
        assertCeilingIsCliff(MillisTimestampDriver.INSTANCE);
    }

    @Test
    public void testCoarserUnitsHaveTighterCeilings() {
        for (TimestampDriver driver : new TimestampDriver[]{
                NanosTimestampDriver.INSTANCE,
                MicrosTimestampDriver.INSTANCE,
                MillisTimestampDriver.INSTANCE
        }) {
            long previous = Long.MAX_VALUE;
            for (char unit : new char[]{'n', 'u', 'T', 's', 'm', 'h', 'd', 'w'}) {
                long ceiling = driver.getMaxUnitValue(unit);
                Assert.assertTrue(
                        "unit " + unit + " must not admit more than the finer unit below it",
                        ceiling <= previous
                );
                previous = ceiling;
            }
        }
    }

    @Test
    public void testPinnedCeilings() {
        // nanos overflow first: 106_751 days is a shade over 292 years
        Assert.assertEquals(Long.MAX_VALUE, NanosTimestampDriver.INSTANCE.getMaxUnitValue('n'));
        Assert.assertEquals(9_223_372_036_854_775L, NanosTimestampDriver.INSTANCE.getMaxUnitValue('u'));
        Assert.assertEquals(9_223_372_036L, NanosTimestampDriver.INSTANCE.getMaxUnitValue('s'));
        Assert.assertEquals(106_751L, NanosTimestampDriver.INSTANCE.getMaxUnitValue('d'));
        Assert.assertEquals(15_250L, NanosTimestampDriver.INSTANCE.getMaxUnitValue('w'));

        // micros buy three orders of magnitude, up to where the int narrowing takes over
        Assert.assertEquals(Long.MAX_VALUE, MicrosTimestampDriver.INSTANCE.getMaxUnitValue('u'));
        Assert.assertEquals(9_223_372_036_854L, MicrosTimestampDriver.INSTANCE.getMaxUnitValue('s'));
        Assert.assertEquals(106_751_991L, MicrosTimestampDriver.INSTANCE.getMaxUnitValue('d'));
        Assert.assertEquals(15_250_284L, MicrosTimestampDriver.INSTANCE.getMaxUnitValue('w'));

        // millis are wide enough that from()'s int narrowing, not the multiply, is the limit
        Assert.assertEquals(Integer.MAX_VALUE, MillisTimestampDriver.INSTANCE.getMaxUnitValue('d'));
        Assert.assertEquals(Integer.MAX_VALUE, MillisTimestampDriver.INSTANCE.getMaxUnitValue('w'));
    }

    @Test
    public void testUnrecognizedUnitAdmitsNothing() {
        // from() answers 0 for a unit it does not know, which is a conversion of no value at all
        Assert.assertEquals(0, NanosTimestampDriver.INSTANCE.getMaxUnitValue('y'));
        Assert.assertEquals(0, MicrosTimestampDriver.INSTANCE.getMaxUnitValue('M'));
        Assert.assertEquals(0, MillisTimestampDriver.INSTANCE.getMaxUnitValue((char) 0));
    }

    /**
     * Asserts that the ceiling sits exactly on the cliff: the ceiling itself converts to a
     * positive value, and one more than the ceiling does not. Both of {@code from()}'s failure
     * modes land on the negative side - the {@code long} multiply wraps past {@code Long.MAX_VALUE}
     * by less than one unit's worth, and the {@code int} narrowing turns {@code Integer.MAX_VALUE + 1}
     * into {@code Integer.MIN_VALUE} - so one test catches both.
     */
    private static void assertCeilingIsCliff(TimestampDriver driver) {
        for (char unit : UNITS) {
            long ceiling = driver.getMaxUnitValue(unit);
            Assert.assertTrue("unit " + unit + " has no ceiling", ceiling > 0);
            Assert.assertTrue(
                    "unit " + unit + " loses its own ceiling",
                    driver.from(ceiling, unit) > 0
            );
            Assert.assertEquals(
                    "unit " + unit + " is not symmetric",
                    -driver.from(ceiling, unit),
                    driver.from(-ceiling, unit)
            );
            if (ceiling < Long.MAX_VALUE) {
                Assert.assertTrue(
                        "unit " + unit + " converts one past its ceiling without wrapping",
                        driver.from(ceiling + 1, unit) < 0
                );
            }
        }
    }
}
