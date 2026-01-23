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

import io.questdb.std.histogram.org.HdrHistogram.AbstractHistogram;
import io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.IntCountsHistogram;
import io.questdb.std.histogram.org.HdrHistogram.PackedDoubleHistogram;
import io.questdb.std.histogram.org.HdrHistogram.PackedHistogram;
import io.questdb.std.histogram.org.HdrHistogram.ShortCountsHistogram;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;

import static io.questdb.test.std.histogram.org.HdrHistogram.HistogramTestUtils.*;

/**
 * JUnit test for {@link io.questdb.std.histogram.org.HdrHistogram.Histogram}
 */
@RunWith(Theories.class)
public class HistogramEncodingTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000; // e.g. for 1 hr in usec units
    @DataPoints
    public static BufferAllocator[] ALLOCATORS = new BufferAllocator[]{BufferAllocator.DIRECT, BufferAllocator.HEAP};

    @Theory
    public void testHistogramEncoding(BufferAllocator allocator) throws Exception {
        ShortCountsHistogram shortCountsHistogram = new ShortCountsHistogram(highestTrackableValue, 3);
        IntCountsHistogram intCountsHistogram = new IntCountsHistogram(highestTrackableValue, 3);
        Histogram histogram = new Histogram(highestTrackableValue, 3);
        PackedHistogram packedHistogram = new PackedHistogram(highestTrackableValue, 3);
        DoubleHistogram doubleHistogram = new DoubleHistogram(highestTrackableValue * 1000, 3);
        PackedDoubleHistogram packedDoubleHistogram = new PackedDoubleHistogram(highestTrackableValue * 1000, 3);

        for (int i = 0; i < 10000; i++) {
            shortCountsHistogram.recordValue(1000 * i);
            intCountsHistogram.recordValue(2000 * i);
            histogram.recordValue(3000 * i);
            packedHistogram.recordValue(3000 * i);
            doubleHistogram.recordValue(5000 * i);
            doubleHistogram.recordValue(0.001); // Makes some internal shifts happen.
            packedDoubleHistogram.recordValue(5000 * i);
            packedDoubleHistogram.recordValue(0.001); // Makes some internal shifts happen.
        }

        System.out.println("Testing encoding of a ShortHistogram:");
        ByteBuffer targetBuffer = allocator.allocate(shortCountsHistogram.getNeededByteBufferCapacity());
        shortCountsHistogram.encodeIntoByteBuffer(targetBuffer);
        targetBuffer.rewind();

        ShortCountsHistogram shortCountsHistogram2 = ShortCountsHistogram.decodeFromByteBuffer(targetBuffer, 0);
        Assert.assertEquals(shortCountsHistogram, shortCountsHistogram2);

        ByteBuffer targetCompressedBuffer = allocator.allocate(shortCountsHistogram.getNeededByteBufferCapacity());
        shortCountsHistogram.encodeIntoCompressedByteBuffer(targetCompressedBuffer);
        targetCompressedBuffer.rewind();

        ShortCountsHistogram shortCountsHistogram3 = ShortCountsHistogram.decodeFromCompressedByteBuffer(targetCompressedBuffer, 0);
        Assert.assertEquals(shortCountsHistogram, shortCountsHistogram3);

        System.out.println("Testing encoding of a IntHistogram:");
        targetBuffer = allocator.allocate(intCountsHistogram.getNeededByteBufferCapacity());
        intCountsHistogram.encodeIntoByteBuffer(targetBuffer);
        targetBuffer.rewind();

        IntCountsHistogram intCountsHistogram2 = IntCountsHistogram.decodeFromByteBuffer(targetBuffer, 0);
        Assert.assertEquals(intCountsHistogram, intCountsHistogram2);

        targetCompressedBuffer = allocator.allocate(intCountsHistogram.getNeededByteBufferCapacity());
        intCountsHistogram.encodeIntoCompressedByteBuffer(targetCompressedBuffer);
        targetCompressedBuffer.rewind();

        IntCountsHistogram intCountsHistogram3 = IntCountsHistogram.decodeFromCompressedByteBuffer(targetCompressedBuffer, 0);
        Assert.assertEquals(intCountsHistogram, intCountsHistogram3);

        System.out.println("Testing encoding of a Histogram:");
        targetBuffer = allocator.allocate(histogram.getNeededByteBufferCapacity());
        histogram.encodeIntoByteBuffer(targetBuffer);
        targetBuffer.rewind();

        Histogram histogram2 = Histogram.decodeFromByteBuffer(targetBuffer, 0);
        Assert.assertEquals(histogram, histogram2);

        targetCompressedBuffer = allocator.allocate(histogram.getNeededByteBufferCapacity());
        histogram.encodeIntoCompressedByteBuffer(targetCompressedBuffer);
        targetCompressedBuffer.rewind();

        Histogram histogram3 = Histogram.decodeFromCompressedByteBuffer(targetCompressedBuffer, 0);
        Assert.assertEquals(histogram, histogram3);

        System.out.println("Testing encoding of a PackedHistogram:");
        targetBuffer = allocator.allocate(packedHistogram.getNeededByteBufferCapacity());
        packedHistogram.encodeIntoByteBuffer(targetBuffer);
        targetBuffer.rewind();

        PackedHistogram packedHistogram2 = PackedHistogram.decodeFromByteBuffer(targetBuffer, 0);
        Assert.assertEquals(packedHistogram, packedHistogram2);

        targetCompressedBuffer = allocator.allocate(packedHistogram.getNeededByteBufferCapacity());
        packedHistogram.encodeIntoCompressedByteBuffer(targetCompressedBuffer);
        targetCompressedBuffer.rewind();

        PackedHistogram packedHistogram3 = PackedHistogram.decodeFromCompressedByteBuffer(targetCompressedBuffer, 0);
        Assert.assertEquals(packedHistogram, packedHistogram3);

        System.out.println("Testing encoding of a DoubleHistogram:");
        targetBuffer = allocator.allocate(doubleHistogram.getNeededByteBufferCapacity());
        doubleHistogram.encodeIntoByteBuffer(targetBuffer);
        targetBuffer.rewind();

        DoubleHistogram doubleHistogram2 = DoubleHistogram.decodeFromByteBuffer(targetBuffer, 0);
        Assert.assertEquals(doubleHistogram, doubleHistogram2);

        targetCompressedBuffer = allocator.allocate(doubleHistogram.getNeededByteBufferCapacity());
        doubleHistogram.encodeIntoCompressedByteBuffer(targetCompressedBuffer);
        targetCompressedBuffer.rewind();

        DoubleHistogram doubleHistogram3 = DoubleHistogram.decodeFromCompressedByteBuffer(targetCompressedBuffer, 0);
        Assert.assertEquals(doubleHistogram, doubleHistogram3);

        System.out.println("Testing encoding of a PackedDoubleHistogram:");
        targetBuffer = allocator.allocate(packedDoubleHistogram.getNeededByteBufferCapacity());
        packedDoubleHistogram.encodeIntoByteBuffer(targetBuffer);
        targetBuffer.rewind();

        PackedDoubleHistogram packedDoubleHistogram2 = PackedDoubleHistogram.decodeFromByteBuffer(targetBuffer, 0);
        Assert.assertEquals(packedDoubleHistogram, packedDoubleHistogram2);

        targetCompressedBuffer = allocator.allocate(packedDoubleHistogram.getNeededByteBufferCapacity());
        packedDoubleHistogram.encodeIntoCompressedByteBuffer(targetCompressedBuffer);
        targetCompressedBuffer.rewind();

        PackedDoubleHistogram packedDoubleHistogram3 = PackedDoubleHistogram.decodeFromCompressedByteBuffer(targetCompressedBuffer, 0);
        Assert.assertEquals(packedDoubleHistogram, packedDoubleHistogram3);
    }

    @Test
    public void testHistogramEncoding_ByteBufferHasCorrectPositionSetAfterEncoding() {
        Histogram histogram = new Histogram(highestTrackableValue, 3);
        int size = histogram.getNeededByteBufferCapacity();
        ByteBuffer buffer = ByteBuffer.allocate(size);

        int bytesWritten = histogram.encodeIntoCompressedByteBuffer(buffer);
        Assert.assertEquals(bytesWritten, buffer.position());
        buffer.rewind();

        bytesWritten = histogram.encodeIntoByteBuffer(buffer);
        Assert.assertEquals(bytesWritten, buffer.position());
    }

    @Test
    public void testResizingHistogramBetweenCompressedEncodings() {
        Class<?>[] testClasses = new Class[]{
                Histogram.class,
                PackedHistogram.class,
                IntCountsHistogram.class,
                ShortCountsHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            AbstractHistogram histogram = constructHistogram(histoClass, 3);

            histogram.recordValue(1);

            ByteBuffer targetCompressedBuffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
            histogram.encodeIntoCompressedByteBuffer(targetCompressedBuffer);

            histogram.recordValue(10000);

            targetCompressedBuffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
            histogram.encodeIntoCompressedByteBuffer(targetCompressedBuffer);
            targetCompressedBuffer.rewind();

            AbstractHistogram histogram2 = decodeFromCompressedByteBuffer(histoClass, targetCompressedBuffer, 0);
            Assert.assertEquals(histogram, histogram2);
        }
    }

    @Test
    public void testSimpleDoubleHistogramEncoding() {
        Class<?>[] testClasses = new Class[]{
                DoubleHistogram.class,
                PackedDoubleHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            DoubleHistogram histogram = constructDoubleHistogram(histoClass, 100000000L, 3);
            histogram.recordValue(6.0);
            histogram.recordValue(1.0);
            histogram.recordValue(0.0);

            ByteBuffer targetBuffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
            histogram.encodeIntoCompressedByteBuffer(targetBuffer);
            targetBuffer.rewind();

            DoubleHistogram decodedHistogram = decodeDoubleHistogramFromCompressedByteBuffer(histoClass, targetBuffer, 0);

            Assert.assertEquals(histogram, decodedHistogram);
        }
    }

    @Test
    public void testSimpleIntegerHistogramEncoding() {
        Class<?>[] testClasses = new Class[]{
                Histogram.class,
                PackedHistogram.class,
                IntCountsHistogram.class,
                ShortCountsHistogram.class
        };

        for (Class<?> histoClass : testClasses) {
            AbstractHistogram histogram = constructHistogram(histoClass, 274877906943L, 3);
            histogram.recordValue(6147);
            histogram.recordValue(1024);
            histogram.recordValue(0);

            ByteBuffer targetBuffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());

            histogram.encodeIntoCompressedByteBuffer(targetBuffer);
            targetBuffer.rewind();
            AbstractHistogram decodedHistogram = decodeFromCompressedByteBuffer(histoClass, targetBuffer, 0);
            Assert.assertEquals(histogram, decodedHistogram);

            histogram.recordValueWithCount(100, 1L << 4); // Make total count > 2^4

            targetBuffer.clear();
            histogram.encodeIntoCompressedByteBuffer(targetBuffer);
            targetBuffer.rewind();
            decodedHistogram = decodeFromCompressedByteBuffer(histoClass, targetBuffer, 0);
            Assert.assertEquals(histogram, decodedHistogram);

            if (histoClass.equals(ShortCountsHistogram.class)) {
                return; // Going farther will overflow short counts histogram
            }
            histogram.recordValueWithCount(200, 1L << 16); // Make total count > 2^16

            targetBuffer.clear();
            histogram.encodeIntoCompressedByteBuffer(targetBuffer);
            targetBuffer.rewind();
            decodedHistogram = decodeFromCompressedByteBuffer(histoClass, targetBuffer, 0);
            Assert.assertEquals(histogram, decodedHistogram);

            histogram.recordValueWithCount(300, 1L << 20); // Make total count > 2^20

            targetBuffer.clear();
            histogram.encodeIntoCompressedByteBuffer(targetBuffer);
            targetBuffer.rewind();
            decodedHistogram = decodeFromCompressedByteBuffer(histoClass, targetBuffer, 0);
            Assert.assertEquals(histogram, decodedHistogram);

            if (histoClass.equals(IntCountsHistogram.class)) {
                return; // Going farther will overflow int counts histogram
            }
            histogram.recordValueWithCount(400, 1L << 32); // Make total count > 2^32

            targetBuffer.clear();
            histogram.encodeIntoCompressedByteBuffer(targetBuffer);
            targetBuffer.rewind();
            decodedHistogram = decodeFromCompressedByteBuffer(histoClass, targetBuffer, 0);
            Assert.assertEquals(histogram, decodedHistogram);

            histogram.recordValueWithCount(500, 1L << 52); // Make total count > 2^52

            targetBuffer.clear();
            histogram.encodeIntoCompressedByteBuffer(targetBuffer);
            targetBuffer.rewind();
            decodedHistogram = decodeFromCompressedByteBuffer(histoClass, targetBuffer, 0);
            Assert.assertEquals(histogram, decodedHistogram);
        }
    }

    public enum BufferAllocator {
        DIRECT {
            @Override
            public ByteBuffer allocate(final int size) {
                return ByteBuffer.allocateDirect(size);
            }
        },
        HEAP {
            @Override
            public ByteBuffer allocate(final int size) {
                return ByteBuffer.allocate(size);
            }
        };

        public abstract ByteBuffer allocate(int size);
    }
}
