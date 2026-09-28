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

package io.questdb.test.griffin.engine.table;

import io.questdb.griffin.engine.table.ExtraNullColumnRecord;
import io.questdb.std.Long256;
import io.questdb.std.Long256Impl;
import org.junit.Assert;
import org.junit.Test;

public class ExtraNullColumnRecordTest {

    // A constant-false WINDOW JOIN drops the join and splices one NULL aggregate column per window
    // function through ExtraNullColumnRecord (SqlCodeGenerator wraps the master in
    // ExtraNullColumnCursorFactory). For a spliced LONG256 column, both CountLong256GroupByFunction
    // and PGUtils.calculateColumnBinSize (the pgwire binary path) read the value and call
    // Long256Impl.isNull(value), whose Long256Impl.equals does an unchecked (Long256Impl) cast.
    // The record returned Long256NullConstant.INSTANCE - a Long256 (and a Function) but NOT a
    // Long256Impl - so that cast threw ClassCastException instead of reading NULL. The sibling
    // HorizonJoinRecord / MultiHorizonJoinRecord return Long256Impl.NULL_LONG256; this record was
    // missed. A columnSplit of 0 makes every column spliced, so the base record is never consulted.
    @Test
    public void testGetLong256SentinelSurvivesLong256ImplIsNull() {
        final ExtraNullColumnRecord record = new ExtraNullColumnRecord(0);
        final Long256 a = record.getLong256A(0);
        final Long256 b = record.getLong256B(0);
        // The exact call the production callers make. Pre-fix this threw ClassCastException from the
        // (Long256Impl) cast in Long256Impl.equals.
        Assert.assertTrue(Long256Impl.isNull(a));
        Assert.assertTrue(Long256Impl.isNull(b));
        // And it is the canonical NULL sentinel the sibling join records hand out.
        Assert.assertSame(Long256Impl.NULL_LONG256, a);
        Assert.assertSame(Long256Impl.NULL_LONG256, b);
    }
}
