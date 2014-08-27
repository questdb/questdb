/*
 * Copyright (c) 2014. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal;

public class BinarySearch {

    public static long indexOf(LongTimeSeriesProvider data, long timestamp, SearchType type) {
        return indexOf(data, timestamp, type, 0, data.size() - 1);
    }

    /**
     * Finds index of timestamp closest to searched value.
     *
     * @param data      data set
     * @param timestamp search value
     * @param type      type of search (NEWER or OLDER)
     * @param lo        lower bound
     * @param hi        higher bound
     * @return index or -2 if searching for NEWER and entire data set is OLDER then search value. Or -1 is searching for OLDER and entire data set is NEWER than search value.
     */
    public static long indexOf(LongTimeSeriesProvider data, long timestamp, SearchType type, long lo, long hi) {
        if (hi == -1) {
            return -1;
        }

        long result = indexOf(data, lo, hi, timestamp, type);

        if (type == SearchType.NEWER_OR_SAME) {
            while (result > lo) {
                long ts = data.readLong(result - 1);
                if (ts < timestamp) {
                    break;
                } else {
                    result--;
                }
            }
        } else if (type == SearchType.OLDER_OR_SAME) {
            while (result < hi) {
                long ts = data.readLong(result + 1);
                if (ts > timestamp) {
                    break;
                } else {
                    result++;
                }
            }
        }
        return result;
    }

    private static long indexOf(LongTimeSeriesProvider data, long startIndex, long endIndex, long timestamp, SearchType type) {

        long minTime = data.readLong(startIndex);

        if (minTime == timestamp) {
            return startIndex;
        }

        long maxTime = data.readLong(endIndex);

        if (maxTime == timestamp) {
            return endIndex;
        }

        if (endIndex - startIndex == 1) {
            if (type == SearchType.NEWER_OR_SAME) {
                if (maxTime >= timestamp) {
                    return endIndex;
                } else {
                    return -2;
                }
            } else {
                if (minTime <= timestamp) {
                    return startIndex;
                } else {
                    return -1;
                }
            }
        }

        if (timestamp > minTime && timestamp < maxTime) {
            long median = startIndex + (endIndex - startIndex) / 2;

            long medianTime = data.readLong(median);

            if (timestamp <= medianTime) {
                return indexOf(data, startIndex, median, timestamp, type);
            } else {
                return indexOf(data, median, endIndex, timestamp, type);
            }
        } else if (timestamp > maxTime && type == SearchType.OLDER_OR_SAME) {
            return endIndex;
        } else if (timestamp > maxTime) {
            return -2;
        } else if (timestamp < minTime && type == SearchType.NEWER_OR_SAME) {
            return startIndex;
        } else {
            return -1;
        }
    }

    public enum SearchType {
        NEWER_OR_SAME, OLDER_OR_SAME
    }

    public static interface LongTimeSeriesProvider {
        long readLong(long index);

        long size();
    }
}
