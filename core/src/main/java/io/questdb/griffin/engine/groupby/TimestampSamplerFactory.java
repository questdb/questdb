/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.groupby;

import io.questdb.griffin.SqlException;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.datetime.microtime.Timestamps;

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
                case 'T':
                    // millis
                    return new MicroTimestampSampler(Timestamps.MILLI_MICROS * n);
                case 's':
                    // seconds
                    return new MicroTimestampSampler(Timestamps.SECOND_MICROS * n);
                case 'm':
                    // minutes
                    return new MicroTimestampSampler(Timestamps.MINUTE_MICROS * n);
                case 'h':
                    // hours
                    return new MicroTimestampSampler(Timestamps.HOUR_MICROS * n);
                case 'd':
                    // days
                    return new MicroTimestampSampler(Timestamps.DAY_MICROS * n);
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
