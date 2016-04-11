/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.nfsdb.ql.impl.aggregation;

import com.nfsdb.ex.JournalRuntimeException;
import com.nfsdb.ex.NumericException;
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
