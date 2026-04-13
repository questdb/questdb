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

package io.questdb.test.cairo;

import io.questdb.cairo.IndexType;
import io.questdb.std.str.StringSink;
import org.junit.Assert;
import org.junit.Test;

public class IndexTypeTest {

    @Test
    public void testIsIndexed() {
        Assert.assertFalse(IndexType.isIndexed(IndexType.NONE));
        Assert.assertTrue(IndexType.isIndexed(IndexType.BITMAP));
        Assert.assertTrue(IndexType.isIndexed(IndexType.POSTING));
        Assert.assertTrue(IndexType.isIndexed(IndexType.POSTING_DELTA));
    }

    @Test
    public void testIsPosting() {
        Assert.assertFalse(IndexType.isPosting(IndexType.NONE));
        Assert.assertFalse(IndexType.isPosting(IndexType.BITMAP));
        Assert.assertTrue(IndexType.isPosting(IndexType.POSTING));
        Assert.assertTrue(IndexType.isPosting(IndexType.POSTING_DELTA));
    }

    @Test
    public void testNameOfAll() {
        Assert.assertEquals("NONE", IndexType.nameOf(IndexType.NONE));
        Assert.assertEquals("BITMAP", IndexType.nameOf(IndexType.BITMAP));
        Assert.assertEquals("POSTING", IndexType.nameOf(IndexType.POSTING));
        Assert.assertEquals("POSTING DELTA", IndexType.nameOf(IndexType.POSTING_DELTA));
        Assert.assertEquals("UNKNOWN", IndexType.nameOf((byte) 99));
    }

    @Test
    public void testPutNameSink() {
        StringSink sink = new StringSink();
        IndexType.putName(sink, IndexType.NONE);
        Assert.assertEquals("NONE", sink.toString());
        sink.clear();
        IndexType.putName(sink, IndexType.BITMAP);
        Assert.assertEquals("BITMAP", sink.toString());
        sink.clear();
        IndexType.putName(sink, IndexType.POSTING);
        Assert.assertEquals("POSTING", sink.toString());
        sink.clear();
        IndexType.putName(sink, IndexType.POSTING_DELTA);
        Assert.assertEquals("POSTING DELTA", sink.toString());
        sink.clear();
        IndexType.putName(sink, (byte) 99);
        Assert.assertEquals("UNKNOWN(99)", sink.toString());
    }

    @Test
    public void testValueOf() {
        Assert.assertEquals(IndexType.BITMAP, IndexType.valueOf("BITMAP"));
        Assert.assertEquals(IndexType.BITMAP, IndexType.valueOf("bitmap"));
        Assert.assertEquals(IndexType.POSTING, IndexType.valueOf("POSTING"));
        Assert.assertEquals(IndexType.POSTING, IndexType.valueOf("posting"));
        Assert.assertEquals(IndexType.NONE, IndexType.valueOf("NONE"));
        Assert.assertEquals(IndexType.NONE, IndexType.valueOf("unknown_type"));
        Assert.assertEquals(IndexType.NONE, IndexType.valueOf(null));
        Assert.assertEquals(IndexType.NONE, IndexType.valueOf(""));
    }
}
