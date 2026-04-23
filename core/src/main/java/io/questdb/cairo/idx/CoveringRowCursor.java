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

package io.questdb.cairo.idx;

import io.questdb.cairo.arr.ArrayView;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.BinarySequence;
import io.questdb.std.str.Utf8Sequence;

public interface CoveringRowCursor extends RowCursor {

    ArrayView getCoveredArray(int includeIdx, int columnType);

    BinarySequence getCoveredBin(int includeIdx);

    long getCoveredBinLen(int includeIdx);

    byte getCoveredByte(int includeIdx);

    double getCoveredDouble(int includeIdx);

    float getCoveredFloat(int includeIdx);

    int getCoveredInt(int includeIdx);

    long getCoveredLong(int includeIdx);

    long getCoveredLong128Hi(int includeIdx);

    long getCoveredLong128Lo(int includeIdx);

    long getCoveredLong256_0(int includeIdx);

    long getCoveredLong256_1(int includeIdx);

    long getCoveredLong256_2(int includeIdx);

    long getCoveredLong256_3(int includeIdx);

    short getCoveredShort(int includeIdx);

    CharSequence getCoveredStrA(int includeIdx);

    CharSequence getCoveredStrB(int includeIdx);

    Utf8Sequence getCoveredVarcharA(int includeIdx);

    Utf8Sequence getCoveredVarcharB(int includeIdx);

    boolean hasCovering();

    boolean isCoveredAvailable(int includeIdx);

    long seekToLast();
}
