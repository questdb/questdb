/*******************************************************************************
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

// Written by Gil Tene of Azul Systems, and released to the public domain,
// as explained at http://creativecommons.org/publicdomain/zero/1.0/
//
// @author Gil Tene

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.cairo.CairoException;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram;
import io.questdb.std.histogram.org.HdrHistogram.PackedDoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.PackedHistogram;
import io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.zip.Deflater;

import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.constructDoubleHistogram;
import static org.junit.Assert.*;

/**
 * JUnit test for {@link io.questdb.std.histogram.org.HdrHistogram.Histogram}
 */
public class DoubleHistogramTest {
    static final int numberOfSignificantValueDigits = 3;
    static final Class<?>[] testClasses = new Class[]{
            DoubleHistogram.class,
            PackedDoubleHistogram.class
    };
    // static final long testValueLevel = 12340;
    static final double testValueLevel = 4.0;
    static final long trackableValueRangeSize = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units

    @Test
    public void testAdd() {
        for (Class<?> histoClass : testClasses) {
            testAdd(histoClass);
        }
    }

    @Test
    public void testAddWithAutoResize() {
        for (Class<?> histoClass : testClasses) {
            testAddWithAutoResize(histoClass);
        }
    }

    @Test
    public void testConstructionArgumentGets() {
        for (Class<?> histoClass : testClasses) {
            testConstructionArgumentGets(histoClass);
        }
    }

    @Test
    public void testCopy() {
        for (Class<?> histoClass : testClasses) {
            testCopy(histoClass);
        }
    }

    @Test
    public void testCopyInto() {
        for (Class<?> histoClass : testClasses) {
            testCopyInto(histoClass);
        }
    }

    @Test
    public void testDataRange() {
        for (Class<?> histoClass : testClasses) {
            testDataRange(histoClass);
        }
    }

    @Test
    public void testHighestEquivalentValue() {
        for (Class<?> histoClass : testClasses) {
            testHighestEquivalentValue(histoClass);
        }
    }

    @Test
    public void testLowestEquivalentValue() {
        for (Class<?> histoClass : testClasses) {
            testLowestEquivalentValue(histoClass);
        }
    }

    @Test
    public void testMedianEquivalentValue() {
        for (Class<?> histoClass : testClasses) {
            testMedianEquivalentValue(histoClass);
        }
    }

    @Test
    public void testNextNonEquivalentValue() {
        for (Class<?> histoClass : testClasses) {
            testNextNonEquivalentValue(histoClass);
        }
    }

    @Test
    public void testNumberOfSignificantValueDigitsMustBeLessThanSix() {
        for (Class<?> histoClass : testClasses) {
            testNumberOfSignificantValueDigitsMustBeLessThanSix(histoClass);
        }
    }

    @Test
    public void testNumberOfSignificantValueDigitsMustBePositive() {
        for (Class<?> histoClass : testClasses) {
            testNumberOfSignificantValueDigitsMustBePositive(histoClass);
        }
    }

    @Test
    public void testRecordValue() {
        for (Class<?> histoClass : testClasses) {
            testRecordValue(histoClass);
        }
    }

    @Test
    public void testRecordValueWithExpectedInterval() {
        for (Class<?> histoClass : testClasses) {
            testRecordValueWithExpectedInterval(histoClass);
        }
    }

    @Test
    public void testRecordValue_Overflow_ShouldThrowException() {
        for (Class<?> histoClass : testClasses) {
            testRecordValue_Overflow_ShouldThrowException(histoClass);
        }
    }

    @Test
    public void testReset() {
        for (Class<?> histoClass : testClasses) {
            testReset(histoClass);
        }
    }

    @Test
    public void testResize() {
        for (Class<?> histoClass : testClasses) {
            testResize(histoClass);
        }
    }

    @Test
    public void testResizeInternals() {
        final Class<?> histoClass = DoubleHistogram.class;
        // Verify resize behavior for various underlying internal integer histogram implementations:
        genericResizeTest(constructDoubleHistogram(histoClass, 2));
        genericResizeTest(constructDoubleHistogram(histoClass, 2, IntCountsHistogram.class));
        genericResizeTest(constructDoubleHistogram(histoClass, 2, ShortCountsHistogram.class));
        genericResizeTest(constructDoubleHistogram(histoClass, 2, PackedHistogram.class));
    }

    @Test
    public void testSerialization() throws Exception {
        for (Class<?> histoClass : testClasses) {
            testSerialization(histoClass);
        }
    }

    @Test
    public void testSerializationWithInternals() throws Exception {
        final Class<?> histoClass = DoubleHistogram.class;
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, 3);
        testDoubleHistogramSerialization(histogram);
        DoubleHistogram withIntHistogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, 3, IntCountsHistogram.class);
        testDoubleHistogramSerialization(withIntHistogram);
        DoubleHistogram withShortHistogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, 3, ShortCountsHistogram.class);
        testDoubleHistogramSerialization(withShortHistogram);
        histogram = constructDoubleHistogram(histoClass, trackableValueRangeSize, 2, Histogram.class);
        testDoubleHistogramSerialization(histogram);
        withIntHistogram = constructDoubleHistogram(histoClass, trackableValueRangeSize, 2, IntCountsHistogram.class);
        testDoubleHistogramSerialization(withIntHistogram);
        withShortHistogram = constructDoubleHistogram(histoClass, trackableValueRangeSize, 2, ShortCountsHistogram.class);
        testDoubleHistogramSerialization(withShortHistogram);
    }

    @Test
    public void testSizeOfEquivalentValueRange() {
        for (Class<?> histoClass : testClasses) {
            testSizeOfEquivalentValueRange(histoClass);
        }
    }

    @Test
    public void testTrackableValueRangeMustBeGreaterThanTwo() {
        for (Class<?> histoClass : testClasses) {
            testTrackableValueRangeMustBeGreaterThanTwo(histoClass);
        }
    }

    private void assertEqual(DoubleHistogram expectedHistogram, DoubleHistogram actualHistogram) {
        assertEquals(expectedHistogram, actualHistogram);
        assertEquals(expectedHistogram.hashCode(), actualHistogram.hashCode());
        assertEquals(
                expectedHistogram.getCountAtValue(testValueLevel),
                actualHistogram.getCountAtValue(testValueLevel));
        assertEquals(
                expectedHistogram.getCountAtValue(testValueLevel * 10),
                actualHistogram.getCountAtValue(testValueLevel * 10));
        assertEquals(
                expectedHistogram.getTotalCount(),
                actualHistogram.getTotalCount());
    }

    private int findContainingBinaryOrderOfMagnitude(long longNumber) {
        int pow2ceiling = 64 - Long.numberOfLeadingZeros(longNumber); // smallest power of 2 containing value
        pow2ceiling = Math.min(pow2ceiling, 62);
        return pow2ceiling;
    }

    private void genericResizeTest(DoubleHistogram h) {
        h.recordValue(0);
        h.recordValue(5);
        h.recordValue(1);
        h.recordValue(8);
        h.recordValue(9);

        Assert.assertEquals(9.0, h.getValueAtPercentile(100), 0.1d);
    }

    private void testAdd(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        DoubleHistogram other =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);

        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 1000);
        other.recordValue(testValueLevel);
        other.recordValue(testValueLevel * 1000);
        histogram.add(other);
        assertEquals(2L, histogram.getCountAtValue(testValueLevel));
        assertEquals(2L, histogram.getCountAtValue(testValueLevel * 1000));
        assertEquals(4L, histogram.getTotalCount());

        DoubleHistogram biggerOther =
                constructDoubleHistogram(histoClass, trackableValueRangeSize * 2, numberOfSignificantValueDigits);
        biggerOther.recordValue(testValueLevel);
        biggerOther.recordValue(testValueLevel * 1000);

        // Adding the smaller histogram to the bigger one should work:
        biggerOther.add(histogram);
        assertEquals(3L, biggerOther.getCountAtValue(testValueLevel));
        assertEquals(3L, biggerOther.getCountAtValue(testValueLevel * 1000));
        assertEquals(6L, biggerOther.getTotalCount());

        // Since we are auto-sized, trying to add a larger histogram into a smaller one should work if no
        // overflowing data is there:
        try {
            // This should throw:
            histogram.add(biggerOther);
        } catch (CairoException e) {
            fail("Should not throw with out of bounds error");
        }

        // But trying to add smaller values to a larger histogram that actually uses it's range should throw an AIOOB:
        histogram.recordValue(1.0);
        other.recordValue(1.0);
        biggerOther.recordValue(trackableValueRangeSize * 8);

        try {
            // This should throw:
            biggerOther.add(histogram);
            fail("Should have thrown with out of bounds error");
        } catch (CairoException ignore) {
        }
    }

    private void testAddWithAutoResize(final Class<?> histoClass) {
        DoubleHistogram histo1 = constructDoubleHistogram(histoClass, 3);
        histo1.setAutoResize(true);
        histo1.recordValue(6.0);
        histo1.recordValue(1.0);
        histo1.recordValue(5.0);
        histo1.recordValue(8.0);
        histo1.recordValue(3.0);
        histo1.recordValue(7.0);
        DoubleHistogram histo2 = constructDoubleHistogram(histoClass, 3);
        histo2.setAutoResize(true);
        histo2.recordValue(9.0);
        DoubleHistogram histo3 = constructDoubleHistogram(histoClass, 3);
        histo3.setAutoResize(true);
        histo3.recordValue(4.0);
        histo3.recordValue(2.0);
        histo3.recordValue(10.0);

        DoubleHistogram merged = constructDoubleHistogram(histoClass, 3);
        merged.setAutoResize(true);
        merged.add(histo1);
        merged.add(histo2);
        merged.add(histo3);

        assertEquals(merged.getTotalCount(),
                histo1.getTotalCount() + histo2.getTotalCount() + histo3.getTotalCount());
        assertEquals(1.0, merged.getMinValue(), 0.01);
        assertEquals(10.0, merged.getMaxValue(), 0.01);
    }

    private void testConstructionArgumentGets(Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record 1.0, and verify that the range adjust to it:
        histogram.recordValue(Math.pow(2.0, 20));
        histogram.recordValue(1.0);
        assertEquals(1.0, histogram.getCurrentLowestTrackableNonZeroValue(), 0.001);
        assertEquals(trackableValueRangeSize, histogram.getHighestToLowestValueRatio());
        assertEquals(numberOfSignificantValueDigits, histogram.getNumberOfSignificantValueDigits());

        DoubleHistogram histogram2 =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record a larger value, and verify that the range adjust to it too:
        histogram2.recordValue(2048.0 * 1024.0 * 1024.0);
        assertEquals(2048.0 * 1024.0 * 1024.0, histogram2.getCurrentLowestTrackableNonZeroValue(), 0.001);

        DoubleHistogram histogram3 =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        // Record a value that is 1000x outside of the initially set range, which should scale us by 1/1024x:
        histogram3.recordValue(1 / 1000.0);
        assertEquals(1.0 / 1024, histogram3.getCurrentLowestTrackableNonZeroValue(), 0.001);
    }

    private void testCopy(final Class<?> histoClass) {
        DoubleHistogram histogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getCurrentHighestTrackableValue() - 1, 31000);

        System.out.println("Testing copy of DoubleHistogram:");
        assertEqual(histogram, histogram.copy());

        DoubleHistogram withIntHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits,
                IntCountsHistogram.class);
        withIntHistogram.recordValue(testValueLevel);
        withIntHistogram.recordValue(testValueLevel * 10);
        withIntHistogram.recordValueWithExpectedInterval(withIntHistogram.getCurrentHighestTrackableValue() - 1, 31000);

        System.out.println("Testing copy of DoubleHistogram backed by IntHistogram:");
        assertEqual(withIntHistogram, withIntHistogram.copy());

        DoubleHistogram withShortHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits,
                ShortCountsHistogram.class);
        withShortHistogram.recordValue(testValueLevel);
        withShortHistogram.recordValue(testValueLevel * 10);
        withShortHistogram.recordValueWithExpectedInterval(withShortHistogram.getCurrentHighestTrackableValue() - 1, 31000);

        System.out.println("Testing copy of DoubleHistogram backed by ShortHistogram:");
        assertEqual(withShortHistogram, withShortHistogram.copy());
    }

    private void testCopyInto(final Class<?> histoClass) {
        DoubleHistogram histogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits);
        DoubleHistogram targetHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getCurrentHighestTrackableValue() - 1,
                histogram.getCurrentHighestTrackableValue() / 1000);

        System.out.println("Testing copyInto for DoubleHistogram:");
        histogram.copyInto(targetHistogram);
        assertEqual(histogram, targetHistogram);

        histogram.recordValue(testValueLevel * 20);

        histogram.copyInto(targetHistogram);
        assertEqual(histogram, targetHistogram);

        DoubleHistogram withIntHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits,
                IntCountsHistogram.class);
        DoubleHistogram targetWithIntHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits,
                IntCountsHistogram.class);
        withIntHistogram.recordValue(testValueLevel);
        withIntHistogram.recordValue(testValueLevel * 10);
        withIntHistogram.recordValueWithExpectedInterval(withIntHistogram.getCurrentHighestTrackableValue() - 1,
                withIntHistogram.getCurrentHighestTrackableValue() / 1000);

        System.out.println("Testing copyInto for DoubleHistogram backed by IntHistogram:");
        withIntHistogram.copyInto(targetWithIntHistogram);
        assertEqual(withIntHistogram, targetWithIntHistogram);

        withIntHistogram.recordValue(testValueLevel * 20);

        withIntHistogram.copyInto(targetWithIntHistogram);
        assertEqual(withIntHistogram, targetWithIntHistogram);

        DoubleHistogram withShortHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits,
                ShortCountsHistogram.class);
        DoubleHistogram targetWithShortHistogram = new DoubleHistogram(trackableValueRangeSize, numberOfSignificantValueDigits,
                ShortCountsHistogram.class);
        withShortHistogram.recordValue(testValueLevel);
        withShortHistogram.recordValue(testValueLevel * 10);
        withShortHistogram.recordValueWithExpectedInterval(withShortHistogram.getCurrentHighestTrackableValue() - 1,
                withShortHistogram.getCurrentHighestTrackableValue() / 1000);

        System.out.println("Testing copyInto for DoubleHistogram backed by a ShortHistogram:");
        withShortHistogram.copyInto(targetWithShortHistogram);
        assertEqual(withShortHistogram, targetWithShortHistogram);

        withShortHistogram.recordValue(testValueLevel * 20);

        withShortHistogram.copyInto(targetWithShortHistogram);
        assertEqual(withShortHistogram, targetWithShortHistogram);
    }

    private void testDataRange(final Class<?> histoClass) {
        // A trackableValueRangeSize histogram
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(0.0);  // Include a zero value to make sure things are handled right.
        assertEquals(1L, histogram.getCountAtValue(0.0));

        double topValue = 1.0;
        try {
            while (true) {
                histogram.recordValue(topValue);
                topValue *= 2.0;
            }
        } catch (CairoException ignore) {
        }
        assertEquals(1L << 33, topValue, 0.00001);
        assertEquals(1L, histogram.getCountAtValue(0.0));

        histogram = constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(0.0); // Include a zero value to make sure things are handled right.

        double bottomValue = 1L << 33;
        try {
            while (true) {
                histogram.recordValue(bottomValue);
                bottomValue /= 2.0;
            }
        } catch (CairoException ex) {
            System.out.println("Bottom value at exception point = " + bottomValue);
        }
        assertEquals(1.0, bottomValue, 0.00001);

        long expectedRange = 1L << (findContainingBinaryOrderOfMagnitude(trackableValueRangeSize) + 1);
        assertEquals(expectedRange, (topValue / bottomValue), 0.00001);
        assertEquals(1L, histogram.getCountAtValue(0.0));
    }

    private void testHighestEquivalentValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        assertEquals("The highest equivalent value to 8180 is 8183",
                8183.99999, histogram.highestEquivalentValue(8180), 0.001);
        assertEquals("The highest equivalent value to 8187 is 8191",
                8191.99999, histogram.highestEquivalentValue(8191), 0.001);
        assertEquals("The highest equivalent value to 8193 is 8199",
                8199.99999, histogram.highestEquivalentValue(8193), 0.001);
        assertEquals("The highest equivalent value to 9995 is 9999",
                9999.99999, histogram.highestEquivalentValue(9995), 0.001);
        assertEquals("The highest equivalent value to 10007 is 10007",
                10007.99999, histogram.highestEquivalentValue(10007), 0.001);
        assertEquals("The highest equivalent value to 10008 is 10015",
                10015.99999, histogram.highestEquivalentValue(10008), 0.001);
    }

    private void testLowestEquivalentValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        assertEquals("The lowest equivalent value to 10007 is 10000",
                10000, histogram.lowestEquivalentValue(10007), 0.001);
        assertEquals("The lowest equivalent value to 10009 is 10008",
                10008, histogram.lowestEquivalentValue(10009), 0.001);
    }

    private void testMedianEquivalentValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        assertEquals("The median equivalent value to 4 is 4",
                4.002, histogram.medianEquivalentValue(4), 0.001);
        assertEquals("The median equivalent value to 5 is 5",
                5.002, histogram.medianEquivalentValue(5), 0.001);
        assertEquals("The median equivalent value to 4000 is 4001",
                4001, histogram.medianEquivalentValue(4000), 0.001);
        assertEquals("The median equivalent value to 8000 is 8002",
                8002, histogram.medianEquivalentValue(8000), 0.001);
        assertEquals("The median equivalent value to 10007 is 10004",
                10004, histogram.medianEquivalentValue(10007), 0.001);
    }

    private void testNextNonEquivalentValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        assertNotSame(null, histogram);
    }

    private void testNumberOfSignificantValueDigitsMustBeLessThanSix(final Class<?> histoClass) {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> constructDoubleHistogram(histoClass, trackableValueRangeSize, 6));
    }

    private void testNumberOfSignificantValueDigitsMustBePositive(final Class<?> histoClass) {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> constructDoubleHistogram(histoClass, trackableValueRangeSize, -1));
    }

    private void testRecordValue(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        assertEquals(1L, histogram.getCountAtValue(testValueLevel));
        assertEquals(1L, histogram.getTotalCount());
    }

    private void testRecordValueWithExpectedInterval(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(0);
        histogram.recordValueWithExpectedInterval(testValueLevel, testValueLevel / 4);
        DoubleHistogram rawHistogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        rawHistogram.recordValue(0);
        rawHistogram.recordValue(testValueLevel);
        // The raw data will not include corrected samples:
        assertEquals(1L, rawHistogram.getCountAtValue(0));
        assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 1) / 4));
        assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 2) / 4));
        assertEquals(0L, rawHistogram.getCountAtValue((testValueLevel * 3) / 4));
        assertEquals(1L, rawHistogram.getCountAtValue((testValueLevel * 4) / 4));
        assertEquals(2L, rawHistogram.getTotalCount());
        // The data will include corrected samples:
        assertEquals(1L, histogram.getCountAtValue(0));
        assertEquals(1L, histogram.getCountAtValue((testValueLevel * 1) / 4));
        assertEquals(1L, histogram.getCountAtValue((testValueLevel * 2) / 4));
        assertEquals(1L, histogram.getCountAtValue((testValueLevel * 3) / 4));
        assertEquals(1L, histogram.getCountAtValue((testValueLevel * 4) / 4));
        assertEquals(5L, histogram.getTotalCount());
    }

    private void testRecordValue_Overflow_ShouldThrowException(final Class<?> histoClass) {
        Assert.assertThrows(CairoException.class,
                () -> {
                    DoubleHistogram histogram =
                            constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
                    histogram.recordValue(trackableValueRangeSize * 3);
                    histogram.recordValue(1.0);
                });
    }

    private void testReset(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(testValueLevel);
        histogram.recordValue(10);
        histogram.recordValue(100);
        Assert.assertEquals(histogram.getMinValue(), Math.min(10.0, testValueLevel), 1.0);
        Assert.assertEquals(histogram.getMaxValue(), Math.max(100.0, testValueLevel), 1.0);
        histogram.reset();
        assertEquals(0L, histogram.getCountAtValue(testValueLevel));
        assertEquals(0L, histogram.getTotalCount());
        histogram.recordValue(20);
        histogram.recordValue(80);
        Assert.assertEquals(20.0, histogram.getMinValue(), 1.0);
        Assert.assertEquals(80.0, histogram.getMaxValue(), 1.0);
    }

    private void testResize(final Class<?> histoClass) {
        // Verify resize behavior for various underlying internal integer histogram implementations:
        genericResizeTest(constructDoubleHistogram(histoClass, 2));
    }

    private void testSerialization(final Class<?> histoClass) throws Exception {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, 3);
        testDoubleHistogramSerialization(histogram);
        histogram = constructDoubleHistogram(histoClass, trackableValueRangeSize, 2);
        testDoubleHistogramSerialization(histogram);
    }

    private void testSizeOfEquivalentValueRange(final Class<?> histoClass) {
        DoubleHistogram histogram =
                constructDoubleHistogram(histoClass, trackableValueRangeSize, numberOfSignificantValueDigits);
        histogram.recordValue(1.0);
        assertEquals("Size of equivalent range for value 1 is 1",
                1.0 / 1024.0, histogram.sizeOfEquivalentValueRange(1), 0.001);
        assertEquals("Size of equivalent range for value 2500 is 2",
                2, histogram.sizeOfEquivalentValueRange(2500), 0.001);
        assertEquals("Size of equivalent range for value 8191 is 4",
                4, histogram.sizeOfEquivalentValueRange(8191), 0.001);
        assertEquals("Size of equivalent range for value 8192 is 8",
                8, histogram.sizeOfEquivalentValueRange(8192), 0.001);
        assertEquals("Size of equivalent range for value 10000 is 8",
                8, histogram.sizeOfEquivalentValueRange(10000), 0.001);
    }

    private void testTrackableValueRangeMustBeGreaterThanTwo(final Class<?> histoClass) {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> constructDoubleHistogram(histoClass, 1, numberOfSignificantValueDigits));
    }

    void testDoubleHistogramSerialization(DoubleHistogram histogram) throws Exception {
        histogram.recordValue(testValueLevel);
        histogram.recordValue(testValueLevel * 10);
        histogram.recordValueWithExpectedInterval(histogram.getCurrentHighestTrackableValue() - 1, histogram.getCurrentHighestTrackableValue() / 1000);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        DoubleHistogram newHistogram;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(histogram);
            Deflater compresser = new Deflater();
            compresser.setInput(bos.toByteArray());
            compresser.finish();
            byte[] compressedOutput = new byte[1024 * 1024];
            int compressedDataLength = compresser.deflate(compressedOutput);
            System.out.println("Serialized form of " + histogram.getClass() + " with internalHighestToLowestValueRatio = " +
                    histogram.getHighestToLowestValueRatio() + "\n and a numberOfSignificantValueDigits = " +
                    histogram.getNumberOfSignificantValueDigits() + " is " + bos.toByteArray().length +
                    " bytes long. Compressed form is " + compressedDataLength + " bytes long.");
            System.out.println("   (estimated footprint was " + histogram.getEstimatedFootprintInBytes() + " bytes)");
            bis = new ByteArrayInputStream(bos.toByteArray());
            in = new ObjectInputStream(bis);
            newHistogram = (DoubleHistogram) in.readObject();
        } finally {
            if (out != null) out.close();
            bos.close();
            if (in != null) in.close();
            if (bis != null) bis.close();
        }
        assertNotNull(newHistogram);
        assertEqual(histogram, newHistogram);
    }
}
