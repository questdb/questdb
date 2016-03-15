/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.ql.impl.map;

import com.nfsdb.misc.Chars;
import com.nfsdb.ql.Record;

class Comparer implements RecordComparator {
    public static final int f1Idx = 0;
    public static final int f2Idx = 1;
    public static final int f3Idx = 11;
    private CharSequence f1;
    private int f2;
    private byte f3;

    @Override
    public void setLeft(Record record) {
        f1 = record.getFlyweightStr(f1Idx);
        f2 = record.getInt(f2Idx);
        f3 = record.get(f3Idx);
    }

    @Override
    public int compare(Record record) {
        int n = Chars.compare(f1, record.getFlyweightStr(f1Idx));
        if (n == 0) {
            n = Integer.compare(f2, record.getInt(f2Idx));
            if (n == 0) {
                n = Byte.compare(f3, record.get(f3Idx));
            }
        }
        return n;
    }
}
