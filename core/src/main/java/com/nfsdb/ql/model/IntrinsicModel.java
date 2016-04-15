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

package com.nfsdb.ql.model;

import com.nfsdb.misc.Dates;
import com.nfsdb.ql.impl.interval.IntervalSource;
import com.nfsdb.std.CharSequenceHashSet;
import com.nfsdb.std.IntList;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjectFactory;

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
    public IntrinsicValue intrinsicValue = IntrinsicValue.UNDEFINED;
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
