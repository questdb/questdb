/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.impl.aggregation;

import com.nfsdb.utils.Dates;

public class MonthsSampler implements TimestampSampler {
    private final int bucket;

    public MonthsSampler(int bucket) {
        this.bucket = bucket;
    }

    @Override
    public long resample(long value) {
        int y = Dates.getYear(value);
        boolean l = Dates.isLeapYear(y);
        int m = Dates.getMonthOfYear(value, y, l);
        // target month
        int n = ((m - 1) / bucket) * bucket + 1;
        return Dates.yearMillis(y, l) +
                Dates.monthOfYearMillis(n, l);
    }
}
