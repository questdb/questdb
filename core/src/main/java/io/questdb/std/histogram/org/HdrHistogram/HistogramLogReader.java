/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.std.histogram.org.HdrHistogram;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.zip.DataFormatException;

/**
 * A histogram log reader.
 * <p>
 * Histogram logs are used to capture full fidelity, per-time-interval
 * histograms of a recorded value.
 * <p>
 * For example, a histogram log can be used to capture high fidelity
 * reaction-time logs for some measured system or subsystem component.
 * Such a log would capture a full reaction time histogram for each
 * logged interval, and could be used to later reconstruct a full
 * HdrHistogram of the measured reaction time behavior for any arbitrary
 * time range within the log, by adding [only] the relevant interval
 * histograms.
 * <h2>Histogram log format:</h2>
 * A histogram log file consists of text lines. Lines beginning with
 * the "#" character are optional and treated as comments. Lines
 * containing the legend (starting with "Timestamp") are also optional
 * and ignored in parsing the histogram log. All other lines must
 * be valid interval description lines. Text fields are delimited by
 * commas, spaces.
 * <p>
 * A valid interval description line contains an optional Tag=tagString
 * text field, followed by an interval description.
 * <p>
 * A valid interval description must contain exactly four text fields:
 * <ul>
 * <li>StartTimestamp: The first field must contain a number parse-able as a Double value,
 * representing the start timestamp of the interval in seconds.</li>
 * <li>intervalLength: The second field must contain a number parse-able as a Double value,
 * representing the length of the interval in seconds.</li>
 * <li>Interval_Max: The third field must contain a number parse-able as a Double value,
 * which generally represents the maximum value of the interval histogram.</li>
 * <li>Interval_Compressed_Histogram: The fourth field must contain a text field
 * parse-able as a Base64 text representation of a compressed HdrHistogram.</li>
 * </ul>
 * The log file may contain an optional indication of a starting time. Starting time
 * is indicated using a special comments starting with "#[StartTime: " and followed
 * by a number parse-able as a double, representing the start time (in seconds)
 * that may be added to timestamps in the file to determine an absolute
 * timestamp (e.g. since the epoch) for each interval.
 */
public class HistogramLogReader implements Closeable {

    private final HistogramLogScanner scanner;
    // scanner handling state
    private boolean absolute;
    private double baseTimeSec = 0.0;
    private EncodableHistogram nextHistogram;
    private boolean observedBaseTime = false;
    private boolean observedStartTime = false;
    private double rangeEndTimeSec;
    private double rangeStartTimeSec;
    private double startTimeSec = 0.0;
    private final HistogramLogScanner.EventHandler handler = new HistogramLogScanner.EventHandler() {
        @Override
        public boolean onBaseTime(double secondsSinceEpoch) {
            baseTimeSec = secondsSinceEpoch; // base time represented as seconds since epoch
            observedBaseTime = true;
            return false;
        }

        @Override
        public boolean onComment(String comment) {
            return false;
        }

        @Override
        public boolean onException(Throwable t) {

            // We ignore NoSuchElementException, but stop processing.
            // Next call to nextIntervalHistogram may return null.
            if (t instanceof java.util.NoSuchElementException) {
                return true;
            }
            // rethrow
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        }

        @Override
        public boolean onHistogram(String tag, double timestamp, double length,
                                   HistogramLogScanner.EncodableHistogramSupplier lazyReader) {
            // Timestamp is expected to be in seconds

            if (!observedStartTime) {
                // No explicit start time noted. Use 1st observed time:
                startTimeSec = timestamp;
                observedStartTime = true;
            }
            if (!observedBaseTime) {
                // No explicit base time noted. Deduce from 1st observed time (compared to start time):
                if (timestamp < startTimeSec - (365 * 24 * 3600.0)) {
                    // Criteria Note: if log timestamp is more than a year in the past (compared to
                    // StartTime), we assume that timestamps in the log are not absolute
                    baseTimeSec = startTimeSec;
                } else {
                    // Micros are absolute
                    baseTimeSec = 0.0;
                }
                observedBaseTime = true;
            }

            final double absoluteStartTimeStampSec = timestamp + baseTimeSec;
            final double offsetStartTimeStampSec = absoluteStartTimeStampSec - startTimeSec;

            // Timestamp length is expect to be in seconds
            final double absoluteEndTimeStampSec = absoluteStartTimeStampSec + length;

            final double startTimeStampToCheckRangeOn = absolute ? absoluteStartTimeStampSec : offsetStartTimeStampSec;

            if (startTimeStampToCheckRangeOn < rangeStartTimeSec) {
                // keep on trucking
                return false;
            }

            if (startTimeStampToCheckRangeOn > rangeEndTimeSec) {
                // after limit we stop on each line
                return true;
            }
            EncodableHistogram histogram;
            try {
                histogram = lazyReader.read();
            } catch (DataFormatException e) {
                // stop after exception
                return true;
            }

            histogram.setStartTimeStamp((long) (absoluteStartTimeStampSec * 1000.0));
            histogram.setEndTimeStamp((long) (absoluteEndTimeStampSec * 1000.0));
            histogram.setTag(tag);
            nextHistogram = histogram;
            return true;
        }

        @Override
        public boolean onStartTime(double secondsSinceEpoch) {
            startTimeSec = secondsSinceEpoch; // start time represented as seconds since epoch
            observedStartTime = true;
            return false;
        }
    };

    /**
     * Constructs a new HistogramLogReader that produces intervals read from the specified file name.
     *
     * @param inputFileName The name of the file to read from
     * @throws java.io.FileNotFoundException when unable to find inputFileName
     */
    public HistogramLogReader(final String inputFileName) throws FileNotFoundException {
        scanner = new HistogramLogScanner(new File(inputFileName));
    }

    /**
     * Constructs a new HistogramLogReader that produces intervals read from the specified InputStream.
     *
     * @param inputStream The InputStream to read from
     */
    public HistogramLogReader(final InputStream inputStream) {
        scanner = new HistogramLogScanner(inputStream);
    }

    /**
     * Constructs a new HistogramLogReader that produces intervals read from the specified file.
     *
     * @param inputFile The File to read from
     * @throws java.io.FileNotFoundException when unable to find inputFile
     */
    public HistogramLogReader(final File inputFile) throws FileNotFoundException {
        scanner = new HistogramLogScanner(inputFile);
    }

    @Override
    public void close() {
        scanner.close();
    }

    /**
     * get the latest start time found in the file so far (or 0.0),
     * per the log file format explained above. Assuming the "#[StartTime:" comment
     * line precedes the actual intervals recorded in the file, getStartTimeSec() can
     * be safely used after each interval is read to determine's the offset of that
     * interval's timestamp from the epoch.
     *
     * @return latest Start Time found in the file (or 0.0 if non found)
     */
    public double getStartTimeSec() {
        return startTimeSec;
    }

    /**
     * Indicates whether or not additional intervals may exist in the log
     *
     * @return true if additional intervals may exist in the log
     */
    public boolean hasNext() {
        return scanner.hasNextLine();
    }

    /**
     * Read the next interval histogram from the log, if interval falls within an absolute time range
     * <p>
     * Returns a histogram object if an interval line was found with an
     * associated absolute start timestamp value that falls between
     * absoluteStartTimeSec and absoluteEndTimeSec, or null if no such
     * interval line is found.
     * <p>
     * Micros are assumed to appear in order in the log file, and as such
     * this method will return a null upon encountering a timestamp larger than
     * rangeEndTimeSec.
     * <p>
     * The histogram returned will have it's timestamp set to the absolute
     * timestamp calculated from adding the interval's indicated timestamp
     * value to the latest [optional] start time found in the log.
     * <p>
     * Absolute timestamps are calculated by adding the timestamp found
     * with the recorded interval to the [latest, optional] start time
     * found in the log. The start time is indicated in the log with
     * a "#[StartTime: " followed by the start time in seconds.
     * <p>
     * Upon encountering any unexpected format errors in reading the next
     * interval from the file, this method will return a null. Use {@link #hasNext} to determine
     * whether or not additional intervals may be available for reading in the log input.
     *
     * @param absoluteStartTimeSec The (absolute time) start of the expected
     *                             time range, in seconds.
     * @param absoluteEndTimeSec   The (absolute time) end of the expected
     *                             time range, in seconds.
     * @return A histogram, or a null if no appropriate interval found
     */
    public EncodableHistogram nextAbsoluteIntervalHistogram(final double absoluteStartTimeSec,
                                                            final double absoluteEndTimeSec) {
        return nextIntervalHistogram(absoluteStartTimeSec, absoluteEndTimeSec, true);
    }

    /**
     * Read the next interval histogram from the log, if interval falls within a time range.
     * <p>
     * Returns a histogram object if an interval line was found with an
     * associated start timestamp value that falls between startTimeSec and
     * endTimeSec, or null if no such interval line is found. Note that
     * the range is assumed to be in seconds relative to the actual
     * timestamp value found in each interval line in the log, and not
     * in absolute time.
     * <p>
     * Micros are assumed to appear in order in the log file, and as such
     * this method will return a null upon encountering a timestamp larger than
     * rangeEndTimeSec.
     * <p>
     * The histogram returned will have it's timestamp set to the absolute
     * timestamp calculated from adding the interval's indicated timestamp
     * value to the latest [optional] start time found in the log.
     * <p>
     * Upon encountering any unexpected format errors in reading the next
     * interval from the file, this method will return a null. Use {@link #hasNext} to determine
     * whether or not additional intervals may be available for reading in the log input.
     *
     * @param startTimeSec The (non-absolute time) start of the expected
     *                     time range, in seconds.
     * @param endTimeSec   The (non-absolute time) end of the expected time
     *                     range, in seconds.
     * @return a histogram, or a null if no appropriate interval found
     */
    public EncodableHistogram nextIntervalHistogram(final double startTimeSec,
                                                    final double endTimeSec) {
        return nextIntervalHistogram(startTimeSec, endTimeSec, false);
    }

    /**
     * Read the next interval histogram from the log. Returns a Histogram object if
     * an interval line was found, or null if not.
     * <p>Upon encountering any unexpected format errors in reading the next interval
     * from the input, this method will return a null. Use {@link #hasNext} to determine
     * whether or not additional intervals may be available for reading in the log input.
     *
     * @return a DecodedInterval, or a null if no appropriately formatted interval was found
     */
    public EncodableHistogram nextIntervalHistogram() {
        return nextIntervalHistogram(0.0, Long.MAX_VALUE * 1.0, true);
    }

    private EncodableHistogram nextIntervalHistogram(final double rangeStartTimeSec,
                                                     final double rangeEndTimeSec, boolean absolute) {
        this.rangeStartTimeSec = rangeStartTimeSec;
        this.rangeEndTimeSec = rangeEndTimeSec;
        this.absolute = absolute;
        scanner.process(handler);
        EncodableHistogram histogram = this.nextHistogram;
        nextHistogram = null;
        return histogram;
    }
}
