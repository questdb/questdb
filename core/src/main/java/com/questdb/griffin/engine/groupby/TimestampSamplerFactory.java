/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

package com.questdb.griffin.engine.groupby;

import com.questdb.griffin.SqlException;
import com.questdb.std.Numbers;
import com.questdb.std.NumericException;
import com.questdb.std.microtime.Dates;

public final class TimestampSamplerFactory {

    /**
     * Parses strings such as '10m', '3M', '5d', '12h', 'y', '35s'
     *
     * @param cs       the key
     * @param position position in SQL text to report error against
     * @return instance of appropriate TimestampSampler
     * @throws SqlException when input string is invalid
     */
    public static TimestampSampler getInstance(CharSequence cs, int position) throws SqlException {
        int k = -1;

        if (cs == null) {
            throw SqlException.$(position, "missing interval");
        }

        final int len = cs.length();

        // look for end of digits
        for (int i = 0; i < len; i++) {
            char c = cs.charAt(i);
            if (c < '0' || c > '9') {
                k = i;
                break;
            }
        }

        if (k == -1) {
            throw SqlException.$(position + len, "expected interval qualifier");
        }

        // expect 1 letter qualifier
        if (k + 1 < len) {
            throw SqlException.$(position + k, "expected single letter qualifier");
        }

        try {
            final int n;
            if (k == 0) {
                n = 1;
            } else {
                n = Numbers.parseInt(cs, 0, k);
                if (n == 0) {
                    throw SqlException.$(position, "zero is not a valid sample value");
                }
            }

            switch (cs.charAt(k)) {
                case 's':
                    // seconds
                    return new MicroTimestampSampler(Dates.SECOND_MICROS * n);
                case 'm':
                    // minutes
                    return new MicroTimestampSampler(Dates.MINUTE_MICROS * n);
                case 'h':
                    // hours
                    return new MicroTimestampSampler(Dates.HOUR_MICROS * n);
                case 'd':
                    // days
                    return new MicroTimestampSampler(Dates.DAY_MICROS * n);
                case 'M':
                    // months
                    return new MonthTimestampSampler(n);
                case 'y':
                    return new YearTimestampSampler(n);
                default:
                    break;

            }
        } catch (NumericException ignore) {
            // we are parsing a pre-validated number
            // but we have to deal with checked exception anyway
            assert false;
        }

        throw SqlException.$(position + k, "unsupported interval qualifier");
    }
}
