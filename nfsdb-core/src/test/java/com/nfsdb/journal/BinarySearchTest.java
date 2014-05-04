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

import org.junit.Assert;
import org.junit.Test;

public class BinarySearchTest {

    @Test
    public void testSearchGreaterOrEquals() throws Exception {
        Assert.assertEquals(6, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{1, 2, 3, 4, 5, 6, 9, 12, 17, 23}),
                7, BinarySearch.SearchType.GREATER_OR_EQUAL));

        Assert.assertEquals(5, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{1, 2, 3, 4, 5, 6, 6, 6, 7, 12, 17, 23}),
                6, BinarySearch.SearchType.GREATER_OR_EQUAL));

        Assert.assertEquals(0, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{2, 2, 3, 4, 5, 6, 6, 6, 7, 12, 17, 23}),
                1, BinarySearch.SearchType.GREATER_OR_EQUAL));

        Assert.assertEquals(0, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{2, 2, 3, 4, 5, 6, 6, 6, 7, 12, 17, 23}),
                2, BinarySearch.SearchType.GREATER_OR_EQUAL));

        Assert.assertEquals(-2, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{2, 2, 3, 4, 5, 6, 6, 6, 7, 12, 17, 23}),
                25, BinarySearch.SearchType.GREATER_OR_EQUAL));

        Assert.assertEquals(-2, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{2, 2}),
                25, BinarySearch.SearchType.GREATER_OR_EQUAL));
    }

    @Test
    public void testSearchLessOrEquals() throws Exception {
        Assert.assertEquals(6, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{1, 2, 3, 4, 5, 6, 9, 12, 17, 23}),
                11, BinarySearch.SearchType.LESS_OR_EQUAL));

        Assert.assertEquals(6, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{1, 2, 3, 4, 9, 9, 9, 12, 17, 23}),
                9, BinarySearch.SearchType.LESS_OR_EQUAL));

        Assert.assertEquals(9, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{1, 2, 3, 4, 9, 9, 9, 12, 17, 23}),
                25, BinarySearch.SearchType.LESS_OR_EQUAL));

        Assert.assertEquals(-1, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{3, 3, 3, 4, 9, 9, 9, 12, 17, 23}),
                1, BinarySearch.SearchType.LESS_OR_EQUAL));

        Assert.assertEquals(-1, BinarySearch.indexOf(new ArraySeriesProvider(new long[]{3, 3}),
                1, BinarySearch.SearchType.LESS_OR_EQUAL));
    }

    private static class ArraySeriesProvider implements BinarySearch.LongTimeSeriesProvider {
        private final long data[];

        @Override
        public long readLong(long index) {
            return data[((int) index)];
        }

        @Override
        public long size() {
            return data.length;
        }

        private ArraySeriesProvider(long[] data) {
            this.data = data;
        }
    }
}
