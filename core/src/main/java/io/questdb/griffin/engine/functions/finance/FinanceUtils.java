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

package io.questdb.griffin.engine.functions.finance;

import io.questdb.std.Numbers;

public class FinanceUtils {

    public static double mid(double bid, double ask) {
        if (Numbers.isNull(bid) || Numbers.isNull(ask)) {
            return Double.NaN;
        }
        return ((ask + bid) / 2.0);
    }

    public static double spread(double bid, double ask) {
        if (Numbers.isNull(bid) || Numbers.isNull(ask)) {
            return Double.NaN;
        } else {
            return (ask - bid);
        }
    }

    public static double imbalance(double bidSize, double askSize) {
        if (Numbers.isNull(bidSize) || Numbers.isNull(askSize)) {
            return Double.NaN;
        }
        double sum = bidSize + askSize;
        if (sum == 0.0d) {
            return Double.NaN;
        }
        return (bidSize - askSize) / sum;
    }

    public static double trueRange(double high, double low, double prevClose) {
        if (Numbers.isNull(high) || Numbers.isNull(low) || Numbers.isNull(prevClose)) {
            return Double.NaN;
        }
        double hl = high - low;
        double hpc = Math.abs(high - prevClose);
        double lpc = Math.abs(low - prevClose);
        return Math.max(hl, Math.max(hpc, lpc));
    }

}
