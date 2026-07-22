/*+*****************************************************************************
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

package io.questdb.griffin.engine.functions.date;

final class VarcharDateFunctionUtils {
    private VarcharDateFunctionUtils() {
    }

    /**
     * Returns true when every value accepted by the pattern is necessarily ASCII.
     * This is deliberately conservative: locale text fields and time zones are
     * treated as Unicode-capable even when their current locale data is ASCII.
     */
    static boolean isAsciiOnlyPattern(CharSequence pattern) {
        int consecutiveMonthChars = 0;
        for (int i = 0, n = pattern.length(); i < n; i++) {
            final char c = pattern.charAt(i);
            if (c > 0x7f) {
                return false;
            }
            switch (c) {
                case 'M':
                    if (++consecutiveMonthChars > 2) {
                        return false;
                    }
                    break;
                case 'E':
                case 'G':
                case 'Z':
                case 'a':
                case 'x':
                case 'z':
                    return false;
                default:
                    consecutiveMonthChars = 0;
                    break;
            }
        }
        return true;
    }
}
