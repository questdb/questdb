/*******************************************************************************
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

package io.questdb.std;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ImplicitCastException;

public abstract class Long256FromCharSequenceDecoder implements Long256Acceptor {

    public static void decode(
            final CharSequence hexString,
            final int startPos,
            final int endPos,
            final Long256Acceptor acceptor
    ) {
        try {
            final int minPos = startPos - 16;
            int lim = endPos;
            int p = lim - 16;
            long l0 = parse64BitGroup(startPos, minPos, hexString, p, lim);
            lim = p;
            p = lim - 16;
            long l1 = parse64BitGroup(startPos, minPos, hexString, p, lim);
            lim = p;
            p -= 16;
            long l2 = parse64BitGroup(startPos, minPos, hexString, p, lim);
            lim = p;
            p -= 16;
            if (p > startPos) {
                // hex string too long
                throw NumericException.INSTANCE;
            }
            long l3 = parse64BitGroup(startPos, minPos, hexString, p, lim);
            acceptor.setAll(l0, l1, l2, l3);
        } catch (NumericException e) {
            throw ImplicitCastException.inconvertibleValue(hexString, ColumnType.STRING, ColumnType.LONG256);
        }
    }

    public abstract void setAll(long l0, long l1, long l2, long l3);

    private static long parse64BitGroup(int startPos, int minPos, CharSequence hexString, int p, int lim) throws NumericException {
        assert minPos == startPos - 16;
        if (p >= startPos) {
            return Numbers.parseHexLong(hexString, p, lim);
        }
        if (p > minPos) {
            return Numbers.parseHexLong(hexString, startPos, lim);
        }
        return 0;
    }
}
