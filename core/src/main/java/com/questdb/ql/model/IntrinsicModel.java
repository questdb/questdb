/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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
 ******************************************************************************/

package com.questdb.ql.model;

import com.questdb.misc.Dates;
import com.questdb.ql.impl.interval.IntervalSource;
import com.questdb.std.CharSequenceHashSet;
import com.questdb.std.IntList;
import com.questdb.std.Mutable;
import com.questdb.std.ObjectFactory;

public class IntrinsicModel implements Mutable {
    public static final IntrinsicModelFactory FACTORY = new IntrinsicModelFactory();
    public final CharSequenceHashSet keyValues = new CharSequenceHashSet();
    public final IntList keyValuePositions = new IntList();
    public String keyColumn;
    public long intervalLo = Long.MIN_VALUE;
    public long intervalHi = Long.MAX_VALUE;
    public ExprNode filter;
    public long millis = Long.MIN_VALUE;
    public IntervalSource intervalSource;
    public int intrinsicValue = IntrinsicValue.UNDEFINED;
    public boolean keyValuesIsLambda = false;

    private IntrinsicModel() {
    }

    @Override
    public void clear() {
        keyColumn = null;
        keyValues.clear();
        keyValuePositions.clear();
        clearInterval();
        filter = null;
        millis = Long.MIN_VALUE;
        intervalSource = null;
        intrinsicValue = IntrinsicValue.UNDEFINED;
        keyValuesIsLambda = false;
    }

    public void clearInterval() {
        this.intervalLo = Long.MIN_VALUE;
        this.intervalHi = Long.MAX_VALUE;
    }

    public void overlapInterval(long lo, long hi) {
        if (hi < intervalLo || lo > intervalHi) {
            intrinsicValue = IntrinsicValue.FALSE;
        } else {
            if (lo > intervalLo) {
                intervalLo = lo;
            }

            if (hi < intervalHi) {
                intervalHi = hi;
            }
        }
    }

    @Override
    public String toString() {
        return "IntrinsicModel{" +
                "keyValues=" + keyValues +
                ", keyColumn='" + keyColumn + '\'' +
                ", intervalLo=" + Dates.toString(intervalLo) +
                ", intervalHi=" + Dates.toString(intervalHi) +
                ", filter=" + filter +
                ", millis=" + millis +
                '}';
    }

    public static final class IntrinsicModelFactory implements ObjectFactory<IntrinsicModel> {
        @Override
        public IntrinsicModel newInstance() {
            return new IntrinsicModel();
        }
    }

}
