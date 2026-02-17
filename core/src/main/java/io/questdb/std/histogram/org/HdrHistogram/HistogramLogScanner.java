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

package io.questdb.std.histogram.org.HdrHistogram;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Scanner;
import java.util.zip.DataFormatException;

public class HistogramLogScanner implements Closeable {

    protected final Scanner scanner;
    private final LazyHistogramReader lazyReader;

    /**
     * Constructs a new HistogramLogReader that produces intervals read from the specified file name.
     *
     * @param inputFileName The name of the file to read from
     * @throws java.io.FileNotFoundException when unable to find inputFileName
     */
    public HistogramLogScanner(final String inputFileName) throws FileNotFoundException {
        this(new Scanner(new File(inputFileName)));
    }

    /**
     * Constructs a new HistogramLogReader that produces intervals read from the specified InputStream. Note that
     * log readers constructed through this constructor do not assume ownership of stream and will not close it on
     * {@link #close()}.
     *
     * @param inputStream The InputStream to read from
     */
    public HistogramLogScanner(final InputStream inputStream) {
        this(new Scanner(inputStream));
    }

    /**
     * Constructs a new HistogramLogReader that produces intervals read from the specified file.
     *
     * @param inputFile The File to read from
     * @throws java.io.FileNotFoundException when unable to find inputFile
     */
    public HistogramLogScanner(final File inputFile) throws FileNotFoundException {
        this(new Scanner(inputFile));
    }

    private HistogramLogScanner(Scanner scanner) {
        this.scanner = scanner;
        this.lazyReader = new LazyHistogramReader(scanner);
        initScanner();
    }

    /**
     * Close underlying scanner.
     */
    @Override
    public void close() {
        scanner.close();
    }

    /**
     * Indicates whether or not additional intervals may exist in the log
     *
     * @return true if additional intervals may exist in the log
     */
    public boolean hasNextLine() {
        return scanner.hasNextLine();
    }

    public void process(EventHandler handler) {
        while (scanner.hasNextLine()) {
            try {
                if (scanner.hasNext("\\#.*")) {
                    // comment line.
                    // Look for explicit start time or base time notes in comments:
                    if (scanner.hasNext("#\\[StartTime:")) {
                        scanner.next("#\\[StartTime:");
                        if (scanner.hasNextDouble()) {
                            double startTimeSec = scanner.nextDouble(); // start time represented as seconds since epoch
                            if (handler.onStartTime(startTimeSec)) {
                                return;
                            }
                        }
                    } else if (scanner.hasNext("#\\[BaseTime:")) {
                        scanner.next("#\\[BaseTime:");
                        if (scanner.hasNextDouble()) {
                            double baseTimeSec = scanner.nextDouble(); // base time represented as seconds since epoch
                            if (handler.onBaseTime(baseTimeSec)) {
                                return;
                            }
                        }
                    } else if (handler.onComment(scanner.next("\\#.*"))) {
                        return;
                    }
                    continue;
                }

                if (scanner.hasNext("\"StartTimestamp\".*")) {
                    // Legend line
                    continue;
                }

                String tagString = null;
                if (scanner.hasNext("Tag\\=.*")) {
                    tagString = scanner.next("Tag\\=.*").substring(4);
                }

                // Decode: startTimestamp, intervalLength, maxTime, histogramPayload
                final double logTimeStampInSec = scanner.nextDouble(); // Timestamp is expected to be in seconds
                final double intervalLengthSec = scanner.nextDouble(); // Timestamp length is expect to be in seconds
                scanner.nextDouble(); // Skip maxTime field, as max time can be deduced from the histogram.

                lazyReader.allowGet();
                if (handler.onHistogram(tagString, logTimeStampInSec, intervalLengthSec, lazyReader)) {
                    return;
                }

            } catch (Throwable ex) {
                if (handler.onException(ex)) {
                    return;
                }
            } finally {
                scanner.nextLine(); // Move to next line.
            }
        }
    }

    private void initScanner() {
        scanner.useLocale(Locale.US);
        scanner.useDelimiter("[ ,\\r\\n]");
    }

    // can't use lambdas, and anyway we need to let the handler take the exception
    public interface EncodableHistogramSupplier {
        EncodableHistogram read() throws DataFormatException;
    }

    /**
     * Handles log events, return true to stop processing.
     */
    public interface EventHandler {
        boolean onBaseTime(double secondsSinceEpoch);

        boolean onComment(String comment);

        boolean onException(Throwable t);

        /**
         * A lazy reader is provided to allow fast skipping of bulk of work where tag or timestamp are to be used as
         * a basis for filtering the {@link EncodableHistogram} anyway. The reader is to be called only once.
         *
         * @param tag        histogram tag or null if none exist
         * @param timestamp  logged timestamp
         * @param length     logged interval length
         * @param lazyReader to be called if the histogram needs to be deserialized, given the tag/timestamp etc.
         * @return true to stop processing, false to continue.
         */
        boolean onHistogram(String tag, double timestamp, double length, EncodableHistogramSupplier lazyReader);

        boolean onStartTime(double secondsSinceEpoch);
    }

    private static class LazyHistogramReader implements EncodableHistogramSupplier {

        private final Scanner scanner;
        private boolean gotIt = true;

        private LazyHistogramReader(Scanner scanner) {
            this.scanner = scanner;
        }

        @Override
        public EncodableHistogram read() throws DataFormatException {
            // prevent double calls to this method
            if (gotIt) {
                throw new IllegalStateException();
            }
            gotIt = true;

            final String compressedPayloadString = scanner.next();
            final ByteBuffer buffer = ByteBuffer.wrap(Base64Helper.parseBase64Binary(compressedPayloadString));

            return EncodableHistogram.decodeFromCompressedByteBuffer(buffer, 0);
        }

        private void allowGet() {
            gotIt = false;
        }
    }
}
