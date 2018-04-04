/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.ql.aggregation;

import com.questdb.common.JournalRuntimeException;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.time.Dates;

public final class SamplerFactory {

    /**
     * Parses strings such as '10m', '3M', '5d', '12h', 'y', '35s'
     *
     * @param cs the key
     * @return instance of appropriate TimestampSampler
     */
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
