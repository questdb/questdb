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

import io.questdb.cairo.sql.ColumnMapping;
import org.junit.Assert;
import org.junit.Test;

public class ColumnMappingTest {

    @Test
    public void testParquetLookupKeyKeepsTheTwoHalvesApart() {
        // A non-negative field id is its own key; a negative one is keyed by
        // position, in a region no writer index reaches. The maps keyed by this
        // read absence off the value side, so the only forbidden key is
        // Integer.MIN_VALUE, their empty-slot marker.
        Assert.assertEquals(0, ColumnMapping.parquetLookupKey(0, 7));
        Assert.assertEquals(3, ColumnMapping.parquetLookupKey(3, 0));
        Assert.assertEquals(Integer.MAX_VALUE, ColumnMapping.parquetLookupKey(Integer.MAX_VALUE, 0));

        Assert.assertEquals(-1, ColumnMapping.parquetLookupKey(-1, 0));
        Assert.assertEquals(-2, ColumnMapping.parquetLookupKey(-1, 1));
        Assert.assertEquals(
                "a hostile field id is discarded for the position, not carried into the key",
                -1,
                ColumnMapping.parquetLookupKey(Integer.MIN_VALUE, 0)
        );
        Assert.assertEquals(Integer.MIN_VALUE + 1, ColumnMapping.parquetLookupKey(-1, Integer.MAX_VALUE - 1));
    }

    @Test
    public void testParquetLookupKeyRejectsAPositionThatWouldKeyOnTheMarker() {
        Assert.assertTrue("this test requires -ea", ColumnMappingTest.class.desiredAssertionStatus());
        try {
            ColumnMapping.parquetLookupKey(-1, Integer.MAX_VALUE);
            Assert.fail("a position keying on Integer.MIN_VALUE must not pass silently");
        } catch (AssertionError e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("parquetIndex=2147483647"));
        }
        try {
            ColumnMapping.parquetLookupKey(-1, -1);
            Assert.fail("a negative position keys back into the writer-index space");
        } catch (AssertionError e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("parquetIndex=-1"));
        }
    }
}
