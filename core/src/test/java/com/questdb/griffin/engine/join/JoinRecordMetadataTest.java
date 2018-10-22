/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.griffin.engine.join;

import com.questdb.cairo.AbstractCairoTest;
import com.questdb.cairo.ColumnType;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class JoinRecordMetadataTest extends AbstractCairoTest {
    @Test
    public void testSimple() {
        JoinRecordMetadata metadata = new JoinRecordMetadata(configuration, 10);
        metadata.add("a", "x", ColumnType.INT);
        metadata.add("a", "y", ColumnType.DOUBLE);
        metadata.add("a", "m", ColumnType.DOUBLE);
        metadata.add("b", "x", ColumnType.DOUBLE);
        metadata.add("b", "y", ColumnType.BINARY);
        metadata.add("b", "z", ColumnType.FLOAT);
        try {
            metadata.add("b", "y", ColumnType.FLOAT);
            Assert.fail();
        } catch (Exception ignored) {
        }

        metadata.add(null, "c.x", ColumnType.STRING);

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
        metadata.add(null, "z.m", ColumnType.STRING);
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
}