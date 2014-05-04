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

    public static long indexOf(LongTimeSeriesProvider data, long searchValue, SearchType type) {
        long startIndex = 0;
        long endIndex = data.size() - 1;

        if (endIndex == -1) {
            return -1;
        }

        long result = indexOf(data, startIndex, endIndex, searchValue, type);

        if (type == SearchType.GREATER_OR_EQUAL) {
            while (result > startIndex) {
                long ts = data.readLong(result - 1);
                if (ts < searchValue) {
                    break;
                } else {
                    result--;
                }
            }
        } else if (type == SearchType.LESS_OR_EQUAL) {
            while (result < endIndex) {
                long ts = data.readLong(result + 1);
                if (ts > searchValue) {
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
            if (type == SearchType.GREATER_OR_EQUAL) {
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
        } else if (timestamp > maxTime && type == SearchType.LESS_OR_EQUAL) {
            return endIndex;
        } else if (timestamp > maxTime) {
            return -2;
        } else if (timestamp < minTime && type == SearchType.GREATER_OR_EQUAL) {
            return startIndex;
        } else {
            return -1;
        }
    }

    public enum SearchType {
        GREATER_OR_EQUAL, LESS_OR_EQUAL
    }

    public static interface LongTimeSeriesProvider {
        long readLong(long index);

        long size();
    }
}
