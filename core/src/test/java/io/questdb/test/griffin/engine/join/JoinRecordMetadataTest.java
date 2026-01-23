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

package io.questdb.test.griffin.engine.join;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JoinRecordMetadataTest extends AbstractCairoTest {
    @Test
    public void testDuplicateColumnAlias() {
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 3);
        metadata.add("A", "x", ColumnType.INT, false, 0, false, null);
        try {
            metadata.add("a", "X", ColumnType.FLOAT, false, 0, false, null);
            Assert.fail();
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), "duplicate column [name=X, alias=a]");
        }

        try {
            metadata.add("A", "X", ColumnType.FLOAT, false, 0, false, null);
            Assert.fail();
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), "duplicate column [name=X, alias=A]");
        }

        Assert.assertEquals(0, metadata.getColumnIndexQuiet("x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("a.X"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("A.x"));
        Assert.assertEquals(0, metadata.getColumnIndexQuiet("A.X"));
    }

    @Test
    public void testSimple() {
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 10);
        metadata.add("a", "x", ColumnType.INT, false, 0, false, null);
        metadata.add("a", "y", ColumnType.DOUBLE, false, 0, false, null);
        metadata.add("a", "m", ColumnType.DOUBLE, false, 0, false, null);
        metadata.add("b", "x", ColumnType.DOUBLE, false, 0, false, null);
        metadata.add("b", "y", ColumnType.BINARY, false, 0, false, null);
        metadata.add("b", "z", ColumnType.FLOAT, false, 0, false, null);
        try {
            metadata.add("b", "y", ColumnType.FLOAT, false, 0, false, null);
            Assert.fail();
        } catch (Exception e) {
            TestUtils.assertContains(e.getMessage(), "duplicate column [name=y, alias=b]");
        }

        metadata.add(null, "c.x", ColumnType.STRING, false, 0, false, null);
        metadata.add(null, "c.vch", ColumnType.VARCHAR, false, 0, false, null);

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
        metadata.add(null, "z.m", ColumnType.STRING, false, 0, false, null);
        Assert.assertEquals(-1, metadata.getColumnIndexQuiet("m"));

        Assert.assertEquals(ColumnType.BINARY, metadata.getColumnType("b.y"));
        Assert.assertEquals(ColumnType.INT, metadata.getColumnType("a.x"));
        Assert.assertEquals(ColumnType.VARCHAR, metadata.getColumnType("c.vch"));

        String varcharType = ColumnType.nameOf(ColumnType.VARCHAR);
        String stringType = ColumnType.nameOf(ColumnType.STRING);
        String expected = "a.x:INT\n" +
                "a.y:DOUBLE\n" +
                "a.m:DOUBLE\n" +
                "b.x:DOUBLE\n" +
                "b.y:BINARY\n" +
                "b.z:FLOAT\n" +
                "c.x:" + stringType + "\n" +
                "c.vch:" + varcharType + "\n" +
                "z.m:" + stringType + "\n";

        StringSink sink = new StringSink();
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            sink.put(metadata.getColumnName(i));
            sink.put(':');
            sink.put(ColumnType.nameOf(metadata.getColumnType(i)));
            sink.put('\n');
        }

        TestUtils.assertEquals(expected, sink);
    }
}
