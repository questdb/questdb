/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

public final class SampleByIntervalExpressionParser {

    // call it only after SampleByIntervalExpressionParser::parseIntervalValue(), this method does not validate cs
    public static char parseIntervalQualifier(CharSequence cs, int position) throws SqlException {
        final char c = cs.charAt(cs.length() - 1);
        switch (c) {
            case 'U': // micros
            case 'T': // millis
            case 's': // seconds
            case 'm': // minutes
            case 'h': // hours
            case 'd': // days
            case 'w': // weeks
            case 'M': // months
            case 'y': // year
                return c;
            default:
                throw SqlException.$(position, "unsupported interval qualifier");
        }
    }

    /**
     * Parses strings such as '10m', '3M', '5d', '12h', 'y', '35s'
     *
     * @param cs       interval expression
     * @param position position in SQL text to report error against
     * @return interval value
     * @throws SqlException when interval expression is invalid
     */
    public static int parseIntervalValue(CharSequence cs, int position) throws SqlException {
        if (cs == null) {
            throw SqlException.$(position, "missing interval expression");
        }

        final int len = cs.length();

        // look for end of digits
        int k = -1;
        for (int i = 0; i < len; i++) {
            final char c = cs.charAt(i);
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
                    throw SqlException.$(position, "zero is not a valid interval value");
                }
            }
            return n;
        } catch (NumericException e) {
            // parsing a pre-validated number, but we have to deal with checked exception anyway
            throw SqlException.$(position + k, "cannot parse interval value");
        }
    }
}
