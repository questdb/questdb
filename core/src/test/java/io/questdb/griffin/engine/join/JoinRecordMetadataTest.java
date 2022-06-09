/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin.engine.join;

import io.questdb.cairo.AbstractCairoTest;
import io.questdb.cairo.ColumnType;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JoinRecordMetadataTest extends AbstractCairoTest {
    @Test
    public void testSimple() {
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 10);
        metadata.add("a", "x", 1, ColumnType.INT, false, 0, false, null);
        metadata.add("a", "y", 2, ColumnType.DOUBLE, false, 0, false, null);
        metadata.add("a", "m", 3, ColumnType.DOUBLE, false, 0, false, null);
        metadata.add("b", "x", 4, ColumnType.DOUBLE, false, 0, false, null);
        metadata.add("b", "y", 5, ColumnType.BINARY, false, 0, false, null);
        metadata.add("b", "z", 6, ColumnType.FLOAT, false, 0, false, null);
        try {
            metadata.add("b", "y", 7,ColumnType.FLOAT, false, 0, false, null);
            Assert.fail();
        } catch (Exception ignored) {
            TestUtils.assertContains(ignored.getMessage(), "Duplicate column [name=y, alias=b]");
        }

        metadata.add(null, "c.x", 8, ColumnType.STRING, false, 0, false, null);

        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.x"));
        Assert.assertEquals(1, metadata.getColumnIndexQuiet("a.y"));
        Assert.assertEquals(2, metadata.getColumnIndexQuiet("m"));

        Assert.assertEquals(3, metadata.getColumnIndexQuiet("b.x"));
        Assert.assertEquals(4, metadata.getColumnIndexQuiet("b.y"));
        Assert.assertEquals(5, metadata.getColumnIndexQuiet("b.z"));

        Assert.assertEquals(5, metadata.getColumnIndexQuiet("z"));

        // this one shouldn't exist
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("b.k"));

        // add ambiguity to column names without aliases
        metadata.add(null, "z.m", 9, ColumnType.STRING, false, 0, false, null);
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("m"));

        Assert.assertEquals(ColumnType.BINARY, metadata.getColumnType("b.y"));
        Assert.assertEquals(ColumnType.INT, metadata.getColumnType("a.x"));

        String expected = "a.x:INT\n" +
                "a.y:DOUBLE\n" +
                "a.m:DOUBLE\n" +
                "b.x:DOUBLE\n" +
                "b.y:BINARY\n" +
                "b.z:FLOAT\n" +
                "c.x:STRING\n" +
                "z.m:STRING\n";

        StringSink sink = new StringSink();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            sink.put(metadata.getColumnName(i));
            sink.put(':');
            sink.put(ColumnType.nameOf(metadata.getColumnType(i)));
            sink.put('\n');
        }

        TestUtils.assertEquals(expected, sink);
    }

    @Test
    public void testDuplicateColumnAlias() {
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 3);
        metadata.add("A", "x", 1, ColumnType.INT, false, 0, false, null);
        try {
            metadata.add("a", "X", 1, ColumnType.FLOAT, false, 0, false, null);
            Assert.fail();
        } catch (Exception ignored) {
            TestUtils.assertContains(ignored.getMessage(), "Duplicate column [name=X, alias=a]");
        }

        try {
            metadata.add("A", "X", 7, ColumnType.FLOAT, false, 0, false, null);
            Assert.fail();
        } catch (Exception ignored) {
            TestUtils.assertContains(ignored.getMessage(), "Duplicate column [name=X, alias=A]");
        }

        Assert.assertEquals(0, metadata.getColumnIndexQuiet("x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.X"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("A.x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("A.X"));
    }
}