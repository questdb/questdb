/**
 * HistogramTest.java
 * Written by Gil Tene of Azul Systems, and released to the public domain,
 * as explained at http://creativecommons.org/publicdomain/zero/1.0/
 *
 * @author Gil Tene
 */

package io.questdb.test.std.histogram.org.HdrHistogram;
import io.questdb.std.histogram.org.HdrHistogram.ConcurrentHistogram;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JUnit test for Histogram
 */
public class ConcurrentHistogramTest {
    static final long highestTrackableValue = 3600L * 1000 * 1000 * 1000; // e.g. for 1 hr in usec units
    volatile boolean doRun = true;
    volatile boolean waitToGo = true;

    @Test
    public void testConcurrentAutoSizedRecording() throws Exception {
        ConcurrentHistogram histogram = new ConcurrentHistogram(2);
        ValueRecorder[] valueRecorders = new ValueRecorder[64];
        doRun = true;
        waitToGo = true;
        for (int i = 0; i < valueRecorders.length; i++) {
            valueRecorders[i] = new ValueRecorder(histogram);
            valueRecorders[i].start();
        }

        long sumOfCounts;

        for (int i = 0; i < 1000; i++) {

            // Ready:
            sumOfCounts = 0;
            for (ValueRecorder v : valueRecorders) {
                v.readySem.acquire();
                sumOfCounts += v.count;
            }

            Assert.assertEquals("totalCount must be equal to sum of counts",
                    sumOfCounts,
                    histogram.getTotalCount());

            // Set:
            waitToGo = true;
            histogram = new ConcurrentHistogram(2);
            for (ValueRecorder v : valueRecorders) {
                v.histogram = histogram;
                v.count = 0;
                v.setSem.release();
            }

            Thread.sleep(2);

            // Go! :
            waitToGo = false;
        }
        doRun = false;
    }

    static AtomicLong valueRecorderId = new AtomicLong(42);

    class ValueRecorder extends Thread {
        ConcurrentHistogram histogram;
        long count = 0;
        Semaphore readySem = new Semaphore(0);
        Semaphore setSem = new Semaphore(0);

        long id = valueRecorderId.getAndIncrement();
        Random random = new Random(id);

        ValueRecorder(ConcurrentHistogram histogram) {
            this.histogram = histogram;
        }

        public void run() {
            try {
                long nextValue = 0;
                for (int i = 0; i < id; i++) {
                    nextValue = (long) (highestTrackableValue * random.nextDouble());
                }
                while (doRun) {
                    readySem.release();
                    setSem.acquire();
                    while (waitToGo) {
                        // wait for doRun to be set.
                    }
                    histogram.resize(nextValue);
                    histogram.recordValue(nextValue);
                    count++;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
