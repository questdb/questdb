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

package io.questdb.test.cairo.sql;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.std.IntList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8SplitString;
import io.questdb.std.str.Utf8String;
import org.junit.Assert;
import org.junit.Test;

public class PageFrameMemoryRecordTest {

    @Test
    public void testMalformedVarcharToStringIsNull() {
        TestRecord record = new TestRecord();
        Assert.assertNull(record.getStrA(0));
        Assert.assertEquals(TableUtils.NULL_LEN, record.getStrLen(0));
    }

    private static class TestRecord extends PageFrameMemoryRecord {
        private final Utf8String value = new Utf8String(new byte[]{'1', (byte) 0xC3}, false);

        private TestRecord() {
            hasTypeCasts = true;
            sourceColumnTypes = new IntList();
            sourceColumnTypes.add(-ColumnType.VARCHAR);
        }

        @Override
        protected Utf8Sequence getVarchar(int columnIndex, Utf8SplitString utf8View) {
            return value;
        }
    }
}
