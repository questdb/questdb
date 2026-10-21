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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewCheckpointDataSegmentWriter;
import io.questdb.cairo.lv.LiveViewCheckpointLayout;
import io.questdb.cairo.lv.LiveViewCheckpointPageRef;
import io.questdb.cairo.lv.LiveViewCheckpointPartitionMapEntry;
import io.questdb.cairo.lv.LiveViewCheckpointRangeRingStateReader;
import io.questdb.cairo.lv.LiveViewCheckpointRingSeal;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSink;
import io.questdb.cairo.lv.LiveViewCheckpointRingStateSource;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryReader;
import io.questdb.cairo.lv.LiveViewCheckpointSegmentDirectoryWriter;
import io.questdb.cairo.lv.LiveViewCheckpointStatePageRef;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.LongList;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;

/**
 * Pins the RANGE ring checkpoint convention of the bounded RANGE statistical window
 * functions at the production seal ({@link LiveViewCheckpointRingSeal}) and the
 * production reader ({@link LiveViewCheckpointRangeRingStateReader}). Two stub
 * functions make exactly the sink and source calls each family's convention prescribes:
 * <ul>
 *     <li>stddev/variance carry {@code (sum, sumSq)} by raw bits in the two-word scalar
 *     and ride the in-frame count on the {@code frameSize} slot, over a one-word DOUBLE
 *     ring. A restore continues bit-exactly.</li>
 *     <li>covar/corr hold five accumulators, more than the four-word scalar limit, so
 *     they store {@code (x, y)} per row in a two-word ring, leave the one-word scalar
 *     unused, ride the in-frame count on {@code frameSize}, and re-accumulate the sums
 *     from the in-frame ring prefix on restore.</li>
 * </ul>
 * The stubs model the runtime arithmetic of {@code StdDevOverPartitionRangeFrameFunction}
 * and {@code BivarStatOverPartitionRangeFrameFunction}: the ring holds finite rows only,
 * and the in-frame rows are its first {@code count} rows. The rest are rows an
 * {@code N PRECEDING AND M PRECEDING} frame has not reached yet.
 */
public class LiveViewCheckpointRangeScalarConventionTest extends AbstractCairoTest {

    private static final long FIRST_BOUNDARY = 201_000;
    private static final byte[] KEY = new byte[]{7, 1, 9};
    private static final String LV_DIR = "lv_range_scalar_convention";
    private static final int SEEDED_ROWS = 200;

    @Before
    public void setUp() {
        super.setUp();
        try (Path path = new Path()) {
            checkpointsDir(path).concat(LiveViewCheckpointLayout.DATA_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
            checkpointsDir(path).concat(LiveViewCheckpointLayout.META_DIR_NAME).slash();
            configuration.getFilesFacade().mkdirs(path, configuration.getMkDirMode());
        }
    }

    @Test
    public void testBivariateConventionRecomputesTheSumsFromThePairRing() throws Exception {
        assertMemoryLeak(() -> {
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointRingSeal ringSeal = new LiveViewCheckpointRingSeal(configuration, null);
                 LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys();
                 LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                 Path dir = new Path()) {
                key.of(KEY);
                final BivariateRing live = new BivariateRing(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128);
                for (int i = 0; i < SEEDED_ROWS; i++) {
                    live.admit(i * 1_000L, pairX(i), pairY(i));
                }
                for (int i = 0; i < 3; i++) {
                    live.expireHead();
                }
                // Two rows the frame has not reached: a recompute that walked the whole ring
                // instead of the in-frame prefix would fold them into every sum.
                live.buffer(200_000, 4_096.5, -8_192.25);
                live.buffer(FIRST_BOUNDARY, -1_024.75, 512.125);
                Assert.assertEquals(197, live.count);
                Assert.assertEquals(199, live.size());

                seal(ringSeal, key, live, 11, null, 0, first, directory);
                assertPageKinds(first, LiveViewCheckpointRangeRingStateReader.DECIMAL128_VALUE_PAGE_KIND);

                reader.of(checkpointsDir(dir), directory.reader, first);
                Assert.assertEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128, reader.getValueKind());
                Assert.assertEquals(199, reader.getRowCount());
                Assert.assertEquals(197, reader.getFrameSize());
                // No scalar word carries an accumulator: the one word the format requires
                // stays zero.
                Assert.assertEquals(1, reader.getScalarWordCount());
                Assert.assertEquals(0, reader.getScalarWord(0));
                final BivariateRing restored = new BivariateRing(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128);
                restored.restoreCheckpointRingState(reader, null);
                // Every x and y comes back by its exact IEEE-754 bits through the two-word
                // value page, -0.0, subnormals and negative values included.
                assertSameRing(live, restored);
                assertRecomputedFromPrefix(live, restored);

                // The restored partition keeps running from its recomputed sums; the live one
                // from its incremental sums. The rings stay identical, the sums need not.
                continueAfterFirstBoundary(live);
                continueAfterFirstBoundary(restored);
                assertSameRing(live, restored);

                seal(ringSeal, key, restored, 12, first, FIRST_BOUNDARY, second, directory);
                // The second root references the first root's chunk verbatim and pays only
                // for the rows appended since, so the pair ring shares pages like any other.
                Assert.assertEquals(4, second.getStatePageCount());
                assertRefEquals(first.getStatePageRef(0), second.getStatePageRef(0));
                assertRefEquals(first.getStatePageRef(1), second.getStatePageRef(1));
                Assert.assertEquals(12, second.getStatePageRef(2).getSegmentId());
                Assert.assertEquals(4, second.getStatePageRef(2).getRowCount());
                assertPageKinds(second, LiveViewCheckpointRangeRingStateReader.DECIMAL128_VALUE_PAGE_KIND);

                reader.of(checkpointsDir(dir), directory.reader, second);
                Assert.assertEquals(5, reader.getHeadOffset());
                Assert.assertEquals(198, reader.getRowCount());
                Assert.assertEquals(197, reader.getFrameSize());
                final BivariateRing restoredAgain = new BivariateRing(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128);
                restoredAgain.restoreCheckpointRingState(reader, null);
                assertSameRing(live, restoredAgain);
                assertRecomputedFromPrefix(live, restoredAgain);
            }
        });
    }

    @Test
    public void testConventionWidthsAreEnforcedAtTheSealAndTheReader() throws Exception {
        assertMemoryLeak(() -> {
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointRingSeal ringSeal = new LiveViewCheckpointRingSeal(configuration, null);
                 LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys();
                 LiveViewCheckpointPartitionMapEntry root = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                 Path dir = new Path()) {
                key.of(KEY);

                // A stddev/variance function that keeps the one-word default while calling
                // the two-word scalar overload is a wiring error the seal refuses outright.
                final StdDevRing undeclaredScalar = new StdDevRing(1);
                undeclaredScalar.admit(1_000, 1.5);
                try {
                    seal(ringSeal, key, undeclaredScalar, 21, null, 0, root, directory);
                    Assert.fail("expected a scalar width mismatch");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "scalar width does not match the declared width [declared=1, actual=2]"
                    );
                }

                // A covar/corr function that keeps the one-word DOUBLE default while
                // streaming (x, y) pairs is refused at its first row.
                final BivariateRing undeclaredPair = new BivariateRing(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE);
                undeclaredPair.admit(1_000, 1.5, -2.5);
                try {
                    seal(ringSeal, key, undeclaredPair, 22, null, 0, root, directory);
                    Assert.fail("expected a value width mismatch");
                } catch (CairoException e) {
                    TestUtils.assertContains(
                            e.getFlyweightMessage(),
                            "value width does not match the declared kind [declared=1, actual=2]"
                    );
                }

                // A pair root read back through the one-word row form, or asked for a
                // scalar word it never carried, fails fast instead of handing back half
                // of the state.
                final BivariateRing pair = new BivariateRing(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DECIMAL128);
                pair.admit(1_000, 1.5, -2.5);
                pair.admit(2_000, -0.0, 3.25);
                seal(ringSeal, key, pair, 23, null, 0, root, directory);
                reader.of(checkpointsDir(dir), directory.reader, root);
                try {
                    reader.forEachRow((timestamp, valueBits) -> Assert.fail("a pair ring must not replay one word per row"));
                    Assert.fail("expected a value width mismatch");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "value width mismatch [expected=1, actual=2]");
                }
                try {
                    reader.getScalarWord(1);
                    Assert.fail("expected a scalar word out of bounds");
                } catch (CairoException e) {
                    TestUtils.assertContains(e.getFlyweightMessage(), "scalar word out of bounds [index=1, words=1]");
                }
            }
        });
    }

    @Test
    public void testStdDevConventionCarriesSumAndSumSqBitExactly() throws Exception {
        assertMemoryLeak(() -> {
            try (Catalogue directory = new Catalogue();
                 LiveViewCheckpointRingSeal ringSeal = new LiveViewCheckpointRingSeal(configuration, null);
                 LiveViewCheckpointTestKeys key = new LiveViewCheckpointTestKeys();
                 LiveViewCheckpointPartitionMapEntry first = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointPartitionMapEntry second = new LiveViewCheckpointPartitionMapEntry();
                 LiveViewCheckpointRangeRingStateReader reader = new LiveViewCheckpointRangeRingStateReader(configuration);
                 Path dir = new Path()) {
                key.of(KEY);
                final StdDevRing live = new StdDevRing(2);
                // The oldest row dwarfs the rest, so expiring it leaves rounding residue in
                // both running sums.
                live.admit(0, 98_765.4321);
                for (int i = 1; i < SEEDED_ROWS; i++) {
                    live.admit(i * 1_000L, (i + 1) * 0.1);
                }
                for (int i = 0; i < 3; i++) {
                    live.expireHead();
                }
                // Two rows the frame has not reached, so the count the scalar covers is not
                // the ring's row count.
                live.buffer(200_000, 7.25);
                live.buffer(FIRST_BOUNDARY, -3.5);
                Assert.assertEquals(197, live.count);
                Assert.assertEquals(199, live.size());
                // The add-then-expire history leaves the incremental sums off a fresh
                // summation of the same rows. Without that, carrying and recomputing would
                // restore the same bits and the assertions below could not tell them apart.
                Assert.assertNotEquals(bits(live.freshSum()), bits(live.sum));
                Assert.assertNotEquals(bits(live.freshSumSq()), bits(live.sumSq));

                seal(ringSeal, key, live, 1, null, 0, first, directory);
                assertPageKinds(first, LiveViewCheckpointRangeRingStateReader.DOUBLE_VALUE_PAGE_KIND);

                reader.of(checkpointsDir(dir), directory.reader, first);
                Assert.assertEquals(LiveViewCheckpointRangeRingStateReader.VALUE_KIND_DOUBLE, reader.getValueKind());
                Assert.assertEquals(2, reader.getScalarWordCount());
                Assert.assertEquals(bits(live.sum), reader.getScalarWord(0));
                Assert.assertEquals(bits(live.sumSq), reader.getScalarWord(1));
                Assert.assertEquals(197, reader.getFrameSize());
                Assert.assertEquals(199, reader.getRowCount());
                final StdDevRing restored = new StdDevRing(2);
                restored.restoreCheckpointRingState(reader, null);
                assertSameState(live, restored);

                // A restored partition and one that never stopped stay bit-identical as both
                // keep running: the frame reaches the two buffered rows, expires five, admits
                // three and buffers one more.
                continueAfterFirstBoundary(live);
                continueAfterFirstBoundary(restored);
                assertSameState(live, restored);

                seal(ringSeal, key, restored, 2, first, FIRST_BOUNDARY, second, directory);
                Assert.assertEquals(4, second.getStatePageCount());
                assertRefEquals(first.getStatePageRef(0), second.getStatePageRef(0));
                assertRefEquals(first.getStatePageRef(1), second.getStatePageRef(1));
                Assert.assertEquals(2, second.getStatePageRef(2).getSegmentId());
                Assert.assertEquals(4, second.getStatePageRef(2).getRowCount());
                assertPageKinds(second, LiveViewCheckpointRangeRingStateReader.DOUBLE_VALUE_PAGE_KIND);

                reader.of(checkpointsDir(dir), directory.reader, second);
                Assert.assertEquals(5, reader.getHeadOffset());
                Assert.assertEquals(198, reader.getRowCount());
                Assert.assertEquals(197, reader.getFrameSize());
                final StdDevRing restoredAgain = new StdDevRing(2);
                restoredAgain.restoreCheckpointRingState(reader, null);
                assertSameState(live, restoredAgain);
            }
        });
    }

    private static void assertBits(String what, double expected, double actual) {
        Assert.assertEquals(what, bits(expected), bits(actual));
    }

    /**
     * Asserts every timestamp page and value page of {@code entry} carries the expected kind.
     */
    private static void assertPageKinds(LiveViewCheckpointPartitionMapEntry entry, int expectedValuePageKind) {
        for (int i = 0, n = entry.getStatePageCount(); i < n; i++) {
            Assert.assertEquals(
                    "page " + i + " kind",
                    (i & 1) == 0 ? LiveViewCheckpointRangeRingStateReader.TIMESTAMP_PAGE_KIND : expectedValuePageKind,
                    entry.getStatePageRef(i).getPageKind()
            );
        }
    }

    /**
     * Asserts {@code restored} re-accumulated its five sums over exactly the in-frame prefix
     * of the ring it read back, in ring order, which is the arithmetic a fresh summation of
     * {@code live}'s in-frame rows performs.
     */
    private static void assertRecomputedFromPrefix(BivariateRing live, BivariateRing restored) {
        Assert.assertEquals(live.count, restored.count);
        final double[] expected = live.freshSums();
        assertBits("sumX", expected[0], restored.sumX);
        assertBits("sumXX", expected[1], restored.sumXX);
        assertBits("sumY", expected[2], restored.sumY);
        assertBits("sumYY", expected[3], restored.sumYY);
        assertBits("sumXY", expected[4], restored.sumXY);
    }

    private static void assertRefEquals(LiveViewCheckpointStatePageRef expected, LiveViewCheckpointStatePageRef actual) {
        Assert.assertEquals(expected.getSegmentId(), actual.getSegmentId());
        Assert.assertEquals(expected.getOffset(), actual.getOffset());
        Assert.assertEquals(expected.getStoredLength(), actual.getStoredLength());
        Assert.assertEquals(expected.getDecodedLength(), actual.getDecodedLength());
        Assert.assertEquals(expected.getPageKind(), actual.getPageKind());
        Assert.assertEquals(expected.getCodec(), actual.getCodec());
        Assert.assertEquals(expected.getRowCount(), actual.getRowCount());
    }

    private static void assertSameRing(BivariateRing expected, BivariateRing actual) {
        TestUtils.assertEquals(expected.timestamps, actual.timestamps);
        TestUtils.assertEquals(expected.xBits, actual.xBits);
        TestUtils.assertEquals(expected.yBits, actual.yBits);
    }

    private static void assertSameState(StdDevRing expected, StdDevRing actual) {
        TestUtils.assertEquals(expected.timestamps, actual.timestamps);
        TestUtils.assertEquals(expected.valueBits, actual.valueBits);
        Assert.assertEquals(expected.count, actual.count);
        assertBits("sum", expected.sum, actual.sum);
        assertBits("sumSq", expected.sumSq, actual.sumSq);
    }

    private static long bits(double value) {
        return Double.doubleToRawLongBits(value);
    }

    private static Path checkpointsDir(Path path) {
        return path.of(configuration.getDbRoot()).concat(LV_DIR).concat("_checkpoints");
    }

    /**
     * The frame reaches the two buffered rows, expires the five oldest, admits three new rows
     * and buffers one more past its reach. The two buffered rows sit at the first boundary, so
     * a seal against it keeps every surviving row as shared pages and appends four.
     */
    private static void continueAfterFirstBoundary(StdDevRing ring) {
        ring.enterFrame();
        ring.enterFrame();
        for (int i = 0; i < 5; i++) {
            ring.expireHead();
        }
        ring.admit(202_000, 0.7);
        ring.admit(203_000, -1.1);
        ring.admit(204_000, 2.3);
        ring.buffer(205_000, 9.5);
    }

    private static void continueAfterFirstBoundary(BivariateRing ring) {
        ring.enterFrame();
        ring.enterFrame();
        for (int i = 0; i < 5; i++) {
            ring.expireHead();
        }
        ring.admit(202_000, 0.7, -0.0);
        ring.admit(203_000, -1.1, Double.MIN_VALUE);
        ring.admit(204_000, 2.3, -Math.PI);
        ring.buffer(205_000, 9.5, 19.75);
    }

    /**
     * An x that puts -0.0 and negative values, whose raw bits are negative longs, through the
     * two-word value page.
     */
    private static double pairX(int i) {
        return i % 50 == 0 ? -0.0 : (i - 100) * 0.37;
    }

    /**
     * A y that puts positive and negative subnormals beside ordinary negative values through
     * the two-word value page.
     */
    private static double pairY(int i) {
        return switch (i % 3) {
            case 0 -> Double.MIN_VALUE * (i + 1);
            case 1 -> -Double.MIN_VALUE * i;
            default -> -Math.E * i;
        };
    }

    /**
     * Seals {@code function}'s one partition through the production ring seal into data
     * segment {@code segmentId} and publishes the segment. A seal that throws leaves no
     * published segment behind.
     */
    private static void seal(
            LiveViewCheckpointRingSeal ringSeal,
            LiveViewCheckpointTestKeys key,
            WindowFunction function,
            long segmentId,
            LiveViewCheckpointPartitionMapEntry previous,
            long previousBoundaryMaxTimestamp,
            LiveViewCheckpointPartitionMapEntry out,
            Catalogue directory
    ) {
        try (LiveViewCheckpointDataSegmentWriter writer = new LiveViewCheckpointDataSegmentWriter(configuration);
             Path dir = new Path()) {
            writer.of(checkpointsDir(dir), segmentId);
            try {
                ringSeal.seal(writer, function, null, key.address(), key.length(), previous, previousBoundaryMaxTimestamp, out);
                directory.addSegment(segmentId, writer.commit());
            } catch (Throwable th) {
                writer.discard();
                throw th;
            }
        }
    }

    /**
     * The checkpoint seam of {@code BivarStatOverPartitionRangeFrameFunction} under the
     * covar/corr convention, over the runtime arithmetic that class's {@code computeNext}
     * performs on its five sums. The partition's state is the function's own fields; the
     * stub ignores the map value.
     */
    private static final class BivariateRing implements WindowFunction, LiveViewCheckpointRingStateSource.Decimal128RowConsumer {
        private final int declaredValueKind;
        private final LongList timestamps = new LongList();
        private final LongList xBits = new LongList();
        private final LongList yBits = new LongList();
        private long count;
        private double sumX;
        private double sumXX;
        private double sumXY;
        private double sumY;
        private double sumYY;

        private BivariateRing(int declaredValueKind) {
            this.declaredValueKind = declaredValueKind;
        }

        @Override
        public void accept(long timestamp, long xBits, long yBits) {
            timestamps.add(timestamp);
            this.xBits.add(xBits);
            this.yBits.add(yBits);
        }

        @Override
        public int checkpointRingValueKind() {
            return declaredValueKind;
        }

        @Override
        public void freezeCheckpointRingState(LiveViewCheckpointRingStateSink sink, MapValue value) {
            // Five accumulators do not fit the four-word scalar: the one word the format
            // requires carries nothing, and frameSize carries the in-frame count.
            sink.putScalarState(0L, count);
            for (int i = 0, n = timestamps.size(); i < n; i++) {
                sink.putRow(timestamps.getQuick(i), xBits.getQuick(i), yBits.getQuick(i));
            }
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
        }

        @Override
        public void restoreCheckpointRingState(LiveViewCheckpointRingStateSource source, MapValue value) {
            timestamps.clear();
            xBits.clear();
            yBits.clear();
            source.forEachRow(this);
            Assert.assertEquals(source.getRowCount(), timestamps.size());
            count = source.getFrameSize();
            // The reader checks only the sign of frameSize. The in-frame rows are a prefix of
            // the ring, so a count past the ring is a corrupt root the function must refuse.
            Assert.assertTrue(count <= timestamps.size());
            final double[] sums = freshSums();
            sumX = sums[0];
            sumXX = sums[1];
            sumY = sums[2];
            sumYY = sums[3];
            sumXY = sums[4];
        }

        @Override
        public void setColumnIndex(int columnIndex) {
        }

        @Override
        public void toPlan(PlanSink sink) {
        }

        /**
         * Buffers a row and lets the frame reach it, which is only right while no buffered row
         * is waiting ahead of it.
         */
        private void admit(long timestamp, double x, double y) {
            Assert.assertEquals(count, size());
            buffer(timestamp, x, y);
            enterFrame();
        }

        private void buffer(long timestamp, double x, double y) {
            timestamps.add(timestamp);
            xBits.add(bits(x));
            yBits.add(bits(y));
        }

        private void enterFrame() {
            final double x = x((int) count);
            final double y = y((int) count);
            sumX += x;
            sumXX += x * x;
            sumY += y;
            sumYY += y * y;
            sumXY += x * y;
            count++;
        }

        private void expireHead() {
            final double x = x(0);
            final double y = y(0);
            timestamps.removeIndex(0);
            xBits.removeIndex(0);
            yBits.removeIndex(0);
            if (count > 0) {
                sumX -= x;
                sumXX -= x * x;
                sumY -= y;
                sumYY -= y * y;
                sumXY -= x * y;
                count--;
            }
        }

        /**
         * @return sumX, sumXX, sumY, sumYY and sumXY re-accumulated over the in-frame prefix in
         * ring order, with the operations {@link #enterFrame()} uses
         */
        private double[] freshSums() {
            final double[] sums = new double[5];
            for (int i = 0; i < count; i++) {
                final double x = x(i);
                final double y = y(i);
                sums[0] += x;
                sums[1] += x * x;
                sums[2] += y;
                sums[3] += y * y;
                sums[4] += x * y;
            }
            return sums;
        }

        private int size() {
            return timestamps.size();
        }

        private double x(int index) {
            return Double.longBitsToDouble(xBits.getQuick(index));
        }

        private double y(int index) {
            return Double.longBitsToDouble(yBits.getQuick(index));
        }
    }

    /**
     * Publishes each added data segment into a fresh copy-on-write directory generation and
     * keeps a reader bound to the newest root, which is the bounds source a chunk read
     * validates its page references against.
     */
    private static final class Catalogue implements Closeable {
        // Static: a published metadata segment is immutable, and the catalogues of every case
        // write into the same checkpoints directory.
        private static long nextMetaSegmentId = 1_000;
        private final LiveViewCheckpointSegmentDirectoryReader reader =
                new LiveViewCheckpointSegmentDirectoryReader(configuration);
        private final LiveViewCheckpointPageRef root = new LiveViewCheckpointPageRef();

        @Override
        public void close() {
            reader.close();
        }

        private void addSegment(long segmentId, long fileLength) {
            try (LiveViewCheckpointSegmentDirectoryWriter writer = new LiveViewCheckpointSegmentDirectoryWriter(configuration);
                 Path dir = new Path()) {
                writer.of(checkpointsDir(dir));
                writer.begin(root);
                writer.addSegment(segmentId, fileLength, 1);
                writer.publish(nextMetaSegmentId++, 1, root);
                reader.of(checkpointsDir(dir), root);
            }
        }
    }

    /**
     * The checkpoint seam of {@code StdDevOverPartitionRangeFrameFunction} under the
     * stddev/variance convention, over the runtime arithmetic that class's
     * {@code computeNext} performs on {@code (sum, sumSq, count)}. The partition's state is
     * the function's own fields; the stub ignores the map value.
     */
    private static final class StdDevRing implements WindowFunction, LiveViewCheckpointRingStateSource.RowConsumer {
        private final int declaredScalarWords;
        private final LongList timestamps = new LongList();
        private final LongList valueBits = new LongList();
        private long count;
        private double sum;
        private double sumSq;

        private StdDevRing(int declaredScalarWords) {
            this.declaredScalarWords = declaredScalarWords;
        }

        @Override
        public void accept(long timestamp, long valueBits) {
            timestamps.add(timestamp);
            this.valueBits.add(valueBits);
        }

        @Override
        public int checkpointRingScalarWords() {
            return declaredScalarWords;
        }

        @Override
        public void freezeCheckpointRingState(LiveViewCheckpointRingStateSink sink, MapValue value) {
            sink.putScalarState(bits(sum), bits(sumSq), count);
            for (int i = 0, n = timestamps.size(); i < n; i++) {
                sink.putRow(timestamps.getQuick(i), valueBits.getQuick(i));
            }
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
        }

        @Override
        public void restoreCheckpointRingState(LiveViewCheckpointRingStateSource source, MapValue value) {
            timestamps.clear();
            valueBits.clear();
            source.forEachRow(this);
            Assert.assertEquals(source.getRowCount(), timestamps.size());
            sum = Double.longBitsToDouble(source.getScalarWord(0));
            sumSq = Double.longBitsToDouble(source.getScalarWord(1));
            count = source.getFrameSize();
            Assert.assertTrue(count <= timestamps.size());
        }

        @Override
        public void setColumnIndex(int columnIndex) {
        }

        @Override
        public void toPlan(PlanSink sink) {
        }

        /**
         * Buffers a row and lets the frame reach it, which is only right while no buffered row
         * is waiting ahead of it.
         */
        private void admit(long timestamp, double value) {
            Assert.assertEquals(count, size());
            buffer(timestamp, value);
            enterFrame();
        }

        private void buffer(long timestamp, double value) {
            timestamps.add(timestamp);
            valueBits.add(bits(value));
        }

        private void enterFrame() {
            final double value = value((int) count);
            sum += value;
            sumSq += value * value;
            count++;
        }

        private void expireHead() {
            final double value = value(0);
            timestamps.removeIndex(0);
            valueBits.removeIndex(0);
            if (count > 0) {
                sum -= value;
                sumSq -= value * value;
                count--;
            }
        }

        private double freshSum() {
            double fresh = 0.0;
            for (int i = 0; i < count; i++) {
                fresh += value(i);
            }
            return fresh;
        }

        private double freshSumSq() {
            double fresh = 0.0;
            for (int i = 0; i < count; i++) {
                final double value = value(i);
                fresh += value * value;
            }
            return fresh;
        }

        private int size() {
            return timestamps.size();
        }

        private double value(int index) {
            return Double.longBitsToDouble(valueBits.getQuick(index));
        }
    }
}
