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

package io.questdb.test.std.histogram.org.HdrHistogram;

import io.questdb.std.histogram.org.HdrHistogram.EncodableHistogram;
import io.questdb.std.histogram.org.HdrHistogram.Histogram;
import io.questdb.std.histogram.org.HdrHistogram.HistogramLogReader;
import io.questdb.std.histogram.org.HdrHistogram.HistogramLogWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

public class HistogramLogReaderWriterTest {

    @Test
    public void emptyHistogramsInLog() throws Exception {
        File temp = File.createTempFile("hdrhistogramtesting", "hlog");
        FileOutputStream writerStream = new FileOutputStream(temp);
        HistogramLogWriter writer = new HistogramLogWriter(writerStream);
        writer.outputLogFormatVersion();
        long startTimeWritten = 11000;
        writer.outputStartTime(startTimeWritten);
        writer.outputLogFormatVersion();
        writer.outputLegend();
        Histogram empty = new Histogram(2);
        empty.setStartTimeStamp(11100);
        empty.setEndTimeStamp(12100);
        writer.outputIntervalHistogram(empty);
        empty.setStartTimeStamp(12100);
        empty.setEndTimeStamp(13100);
        writer.outputIntervalHistogram(empty);
        writerStream.close();

        FileInputStream readerStream = new FileInputStream(temp);
        HistogramLogReader reader = new HistogramLogReader(readerStream);
        Histogram histogram = (Histogram) reader.nextIntervalHistogram();
        Assert.assertEquals(11.0, reader.getStartTimeSec(), 0.000001);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(0, histogram.getTotalCount());
        Assert.assertEquals(11100, histogram.getStartTimeStamp());
        Assert.assertEquals(12100, histogram.getEndTimeStamp());
        histogram = (Histogram) reader.nextIntervalHistogram();
        Assert.assertNotNull(histogram);
        Assert.assertEquals(0, histogram.getTotalCount());
        Assert.assertEquals(12100, histogram.getStartTimeStamp());
        Assert.assertEquals(13100, histogram.getEndTimeStamp());
        Assert.assertNull(reader.nextIntervalHistogram());
        readerStream.close();

        readerStream = new FileInputStream(temp);
        reader = new HistogramLogReader(readerStream);
        // relative read from the file, should include both histograms
        histogram = (Histogram) reader.nextIntervalHistogram(0.0, 4.0);
        Assert.assertEquals(11.0, reader.getStartTimeSec(), 0.000001);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(0, histogram.getTotalCount());
        Assert.assertEquals(11100, histogram.getStartTimeStamp());
        Assert.assertEquals(12100, histogram.getEndTimeStamp());
        histogram = (Histogram) reader.nextIntervalHistogram(0.0, 4.0);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(0, histogram.getTotalCount());
        Assert.assertEquals(12100, histogram.getStartTimeStamp());
        Assert.assertEquals(13100, histogram.getEndTimeStamp());
        Assert.assertNull(reader.nextIntervalHistogram());
        readerStream.close();

        readerStream = new FileInputStream(temp);
        reader = new HistogramLogReader(readerStream);
        // relative read from the file, should skip first histogram
        histogram = (Histogram) reader.nextIntervalHistogram(1.0, 4.0);
        Assert.assertEquals(11.0, reader.getStartTimeSec(), 0.000001);
        Assert.assertNotNull(histogram);
        Assert.assertEquals(0, histogram.getTotalCount());
        Assert.assertEquals(12100, histogram.getStartTimeStamp());
        Assert.assertEquals(13100, histogram.getEndTimeStamp());
        Assert.assertNull(reader.nextIntervalHistogram());
        readerStream.close();
    }

    @Test
    public void emptyLog() throws Exception {
        File temp = File.createTempFile("hdrhistogramtesting", "hist");
        temp.deleteOnExit();
        FileOutputStream writerStream = new FileOutputStream(temp);
        HistogramLogWriter writer = new HistogramLogWriter(writerStream);
        writer.outputLogFormatVersion();
        long startTimeWritten = 1000;
        writer.outputStartTime(startTimeWritten);
        writer.outputLogFormatVersion();
        writer.outputLegend();
        writerStream.close();

        FileInputStream readerStream = new FileInputStream(temp);
        HistogramLogReader reader = new HistogramLogReader(readerStream);
        EncodableHistogram histogram = reader.nextIntervalHistogram();
        Assert.assertNull(histogram);
        Assert.assertEquals(1.0, reader.getStartTimeSec(), 0.000001);
    }

    @Test
    public void jHiccupV0Log() {
        InputStream readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.1.logV0.hlog");
        HistogramLogReader reader = new HistogramLogReader(readerStream);
        int histogramCount = 0;
        long totalCount = 0;
        EncodableHistogram encodeableHistogram;
        Histogram accumulatedHistogram = new Histogram(3);
        while ((encodeableHistogram = reader.nextIntervalHistogram()) != null) {
            histogramCount++;
            Assert.assertTrue("Expected integer value histograms in log file", encodeableHistogram instanceof Histogram);
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(81, histogramCount);
        Assert.assertEquals(61256, totalCount);
        Assert.assertEquals(1510998015, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1569718271, accumulatedHistogram.getMaxValue());
        Assert.assertEquals(1438869961.225, reader.getStartTimeSec(), 0.000001);

        readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.1.logV0.hlog");
        reader = new HistogramLogReader(readerStream);
        histogramCount = 0;
        totalCount = 0;
        accumulatedHistogram.reset();
        while ((encodeableHistogram = reader.nextIntervalHistogram(20, 45)) != null) {
            histogramCount++;
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(25, histogramCount);
        Assert.assertEquals(18492, totalCount);
        Assert.assertEquals(459007, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(623103, accumulatedHistogram.getMaxValue());

        readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.1.logV0.hlog");
        reader = new HistogramLogReader(readerStream);
        histogramCount = 0;
        totalCount = 0;
        accumulatedHistogram.reset();
        while ((encodeableHistogram = reader.nextIntervalHistogram(46, 80)) != null) {
            histogramCount++;
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(34, histogramCount);
        Assert.assertEquals(25439, totalCount);
        Assert.assertEquals(1209008127, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1234173951, accumulatedHistogram.getMaxValue());
    }

    @Test
    public void jHiccupV1Log() {
        InputStream readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.6.logV1.hlog");
        HistogramLogReader reader = new HistogramLogReader(readerStream);
        int histogramCount = 0;
        long totalCount = 0;
        EncodableHistogram encodeableHistogram = null;
        Histogram accumulatedHistogram = new Histogram(3);
        while ((encodeableHistogram = reader.nextIntervalHistogram()) != null) {
            histogramCount++;
            Assert.assertTrue("Expected integer value histograms in log file", encodeableHistogram instanceof Histogram);
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(88, histogramCount);
        Assert.assertEquals(65964, totalCount);
        Assert.assertEquals(1829765119, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1888485375, accumulatedHistogram.getMaxValue());
        Assert.assertEquals(1438867590.285, reader.getStartTimeSec(), 0.000001);

        readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.6.logV1.hlog");
        reader = new HistogramLogReader(readerStream);
        histogramCount = 0;
        totalCount = 0;
        accumulatedHistogram.reset();
        while ((encodeableHistogram = reader.nextIntervalHistogram(5, 20)) != null) {
            histogramCount++;
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(15, histogramCount);
        Assert.assertEquals(11213, totalCount);
        Assert.assertEquals(1019740159, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1032323071, accumulatedHistogram.getMaxValue());

        readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.6.logV1.hlog");
        reader = new HistogramLogReader(readerStream);
        histogramCount = 0;
        totalCount = 0;
        accumulatedHistogram.reset();
        while ((encodeableHistogram = reader.nextIntervalHistogram(50, 80)) != null) {
            histogramCount++;
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(29, histogramCount);
        Assert.assertEquals(22630, totalCount);
        Assert.assertEquals(1871708159, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1888485375, accumulatedHistogram.getMaxValue());
    }

    @Test
    public void jHiccupV2Log() {
        InputStream readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.7S.logV2.hlog");

        HistogramLogReader reader = new HistogramLogReader(readerStream);
        int histogramCount = 0;
        long totalCount = 0;
        EncodableHistogram encodeableHistogram = null;
        Histogram accumulatedHistogram = new Histogram(3);
        while ((encodeableHistogram = reader.nextIntervalHistogram()) != null) {
            histogramCount++;
            Assert.assertTrue("Expected integer value histograms in log file", encodeableHistogram instanceof Histogram);
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(62, histogramCount);
        Assert.assertEquals(48761, totalCount);
        Assert.assertEquals(1745879039, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1796210687, accumulatedHistogram.getMaxValue());
        Assert.assertEquals(1441812279.474, reader.getStartTimeSec(), 0.000001);

        readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.7S.logV2.hlog");
        reader = new HistogramLogReader(readerStream);
        histogramCount = 0;
        totalCount = 0;
        accumulatedHistogram.reset();
        while ((encodeableHistogram = reader.nextIntervalHistogram(5, 20)) != null) {
            histogramCount++;
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(15, histogramCount);
        Assert.assertEquals(11664, totalCount);
        Assert.assertEquals(1536163839, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1544552447, accumulatedHistogram.getMaxValue());

        readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/jHiccup-2.0.7S.logV2.hlog");
        reader = new HistogramLogReader(readerStream);
        histogramCount = 0;
        totalCount = 0;
        accumulatedHistogram.reset();
        while ((encodeableHistogram = reader.nextIntervalHistogram(40, 60)) != null) {
            histogramCount++;
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(20, histogramCount);
        Assert.assertEquals(15830, totalCount);
        Assert.assertEquals(1779433471, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1796210687, accumulatedHistogram.getMaxValue());
    }

    @Test
    public void taggedV2LogTest() {
        InputStream readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/tagged-Log.logV2.hlog");

        HistogramLogReader reader = new HistogramLogReader(readerStream);
        int histogramCount = 0;
        long totalCount = 0;
        EncodableHistogram encodeableHistogram = null;
        Histogram accumulatedHistogramWithNoTag = new Histogram(3);
        Histogram accumulatedHistogramWithTagA = new Histogram(3);
        while ((encodeableHistogram = reader.nextIntervalHistogram()) != null) {
            histogramCount++;
            Assert.assertTrue("Expected integer value histograms in log file", encodeableHistogram instanceof Histogram);
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            if ("A".equals(histogram.getTag())) {
                accumulatedHistogramWithTagA.add(histogram);
            } else {
                accumulatedHistogramWithNoTag.add(histogram);
            }
        }
        Assert.assertEquals(32290, totalCount);
        Assert.assertEquals(accumulatedHistogramWithTagA, accumulatedHistogramWithNoTag);
    }

    @Test
    public void ycsbV1Log() {
        InputStream readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/ycsb.logV1.hlog");
        HistogramLogReader reader = new HistogramLogReader(readerStream);
        int histogramCount = 0;
        long totalCount = 0;
        EncodableHistogram encodeableHistogram = null;
        Histogram accumulatedHistogram = new Histogram(3);
        while ((encodeableHistogram = reader.nextIntervalHistogram()) != null) {
            histogramCount++;
            Assert.assertTrue("Expected integer value histograms in log file", encodeableHistogram instanceof Histogram);
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(602, histogramCount);
        Assert.assertEquals(300056, totalCount);
        Assert.assertEquals(1214463, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1546239, accumulatedHistogram.getMaxValue());
        Assert.assertEquals(1438613579.295, reader.getStartTimeSec(), 0.000001);

        readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/ycsb.logV1.hlog");
        reader = new HistogramLogReader(readerStream);
        histogramCount = 0;
        totalCount = 0;
        accumulatedHistogram.reset();
        while ((encodeableHistogram = reader.nextIntervalHistogram(0, 180)) != null) {
            histogramCount++;
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        // note the first histogram in the log is before 0, so we drop it on the
        // floor
        Assert.assertEquals(180, histogramCount);
        Assert.assertEquals(90033, totalCount);
        Assert.assertEquals(1375231, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(1546239, accumulatedHistogram.getMaxValue());

        readerStream = HistogramLogReaderWriterTest.class.getResourceAsStream("/hdr-histogram/ycsb.logV1.hlog");
        reader = new HistogramLogReader(readerStream);
        histogramCount = 0;
        totalCount = 0;
        accumulatedHistogram.reset();
        while ((encodeableHistogram = reader.nextIntervalHistogram(180, 700)) != null) {
            histogramCount++;
            Histogram histogram = (Histogram) encodeableHistogram;
            totalCount += histogram.getTotalCount();
            accumulatedHistogram.add(histogram);
        }
        Assert.assertEquals(421, histogramCount);
        Assert.assertEquals(209686, totalCount);
        Assert.assertEquals(530, accumulatedHistogram.getValueAtPercentile(99.9));
        Assert.assertEquals(17775, accumulatedHistogram.getMaxValue());
    }

}
