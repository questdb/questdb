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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.EmptyRecordMetadata;
import io.questdb.griffin.engine.EmptyTableRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class EmptyTableRecordCursorFactoryTest extends AbstractCairoTest {

    @Test
    public void testCursorIsRandomAccessAndExposesNoRows() throws Exception {
        // Random access on an empty cursor is trivially correct: no row is ever fetched,
        // so recordAt never runs. Splice-style joins rely on this signal to admit an
        // empty side as the master factory rather than failing compilation.
        try (RecordCursorFactory factory = new EmptyTableRecordCursorFactory(EmptyRecordMetadata.INSTANCE)) {
            Assert.assertTrue(factory.recordCursorSupportsRandomAccess());
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                Assert.assertFalse(cursor.hasNext());
                Assert.assertEquals(0, cursor.size());
                // recordAt is a no-op on the empty random cursor; it must not throw.
                cursor.recordAt(cursor.getRecord(), 0);
                cursor.toTop();
                Assert.assertFalse(cursor.hasNext());
            }
        }
    }

}
