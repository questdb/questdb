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

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;

/**
 * A base class for all encodable (and decodable) histogram classes. Log readers and writers
 * will generally use this base class to provide common log processing across the integer value
 * based AbstractHistogram subclasses and the double value based DoubleHistogram class.
 */
public abstract class EncodableHistogram {

    public abstract int encodeIntoCompressedByteBuffer(final ByteBuffer targetBuffer, int compressionLevel);

    public abstract long getEndTimeStamp();

    public abstract double getMaxValueAsDouble();

    public abstract int getNeededByteBufferCapacity();

    public abstract long getStartTimeStamp();

    public abstract String getTag();

    public abstract void setEndTimeStamp(long endTimestamp);

    public abstract void setStartTimeStamp(long startTimeStamp);

    public abstract void setTag(String tag);

    /**
     * Decode a {@link EncodableHistogram} from a compressed byte buffer. Will return either a
     * {@link io.questdb.std.histogram.org.HdrHistogram.Histogram} or {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram} depending
     * on the format found in the supplied buffer.
     *
     * @param buffer                         The input buffer to decode from.
     * @param minBarForHighestTrackableValue A lower bound either on the highestTrackableValue of
     *                                       the created Histogram, or on the HighestToLowestValueRatio
     *                                       of the created DoubleHistogram.
     * @return The decoded {@link io.questdb.std.histogram.org.HdrHistogram.Histogram} or {@link io.questdb.std.histogram.org.HdrHistogram.DoubleHistogram}
     * @throws DataFormatException on errors in decoding the buffer compression.
     */
    static EncodableHistogram decodeFromCompressedByteBuffer(
            ByteBuffer buffer,
            final long minBarForHighestTrackableValue) throws DataFormatException {
        // Peek iun buffer to see the cookie:
        int cookie = buffer.getInt(buffer.position());
        if (DoubleHistogram.isDoubleHistogramCookie(cookie)) {
            return DoubleHistogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue);
        } else {
            return Histogram.decodeFromCompressedByteBuffer(buffer, minBarForHighestTrackableValue);
        }
    }
}
