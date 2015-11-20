/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.ql.impl.aggregation;

import com.nfsdb.exceptions.JournalRuntimeException;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.misc.Dates;
import com.nfsdb.misc.Numbers;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public final class SamplerFactory {

    /**
     * Parses strings such as '10m', '3M', '5d', '12h', 'y', '35s'
     *
     * @param cs the key
     * @return instance of appropriate TimestampSampler
     */
    @SuppressFBWarnings({"LEST_LOST_EXCEPTION_STACK_TRACE", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS"})
    public static TimestampSampler from(CharSequence cs) {
        int k = -1;

        // look for end of digits
        for (int i = 0; i < cs.length(); i++) {
            char c = cs.charAt(i);
            if (c < '0' || c > '9') {
                k = i;
                break;
            }
        }

        if (k == -1) {
            // no qualifier
            return null;
        }

        // expect 1 letter qualifier
        if (k + 1 < cs.length()) {
            return null;
        }

        if (k == 0) {
            if (cs.charAt(k) == 'Y') {
                return YearSampler.INSTANCE;
            } else {
                return null;
            }
        }

        try {
            int n = Numbers.parseInt(cs, 0, k);
            switch (cs.charAt(k)) {
                case 's':
                    // seconds
                    return new MillisSampler(Dates.SECOND_MILLIS * n);
                case 'm':
                    // minutes
                    return new MillisSampler(Dates.MINUTE_MILLIS * n);
                case 'h':
                    // hours
                    return new MillisSampler(Dates.HOUR_MILLIS * n);
                case 'd':
                    // days
                    return new MillisSampler(Dates.DAY_MILLIS * n);
                case 'M':
                    // months
                    return new MonthsSampler(n);
                default:
                    return null;

            }
        } catch (NumericException e) {
            throw new JournalRuntimeException("Internal error");
        }
    }
}
