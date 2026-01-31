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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.InvalidColumnException;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GenericRecordMetadataTest {
    private static final StringSink sink = new StringSink();

    @Test
    public void testBaseInterfaceDefaults() {
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("abc", ColumnType.INT));
        metadata.add(new TableColumnMetadata("cde", ColumnType.SYMBOL, IndexType.SYMBOL, 1024, true, null));
        metadata.add(new TableColumnMetadata("timestamp", ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(2);

        Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType("timestamp"));

        try {
            metadata.getColumnIndex("xyz");
            Assert.fail();
        } catch (InvalidColumnException ignore) {
        }

        Assert.assertEquals(1024, metadata.getIndexValueBlockCapacity("cde"));
        Assert.assertTrue(metadata.isSymbolTableStatic("cde"));
    }

    @Test
    public void testDuplicateColumn() {
        final String expected = "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"abc\",\"type\":\"INT\"},{\"index\":1,\"name\":\"cde\",\"type\":\"INT\"}],\"timestampIndex\":-1}";

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("abc", ColumnType.INT));
        metadata.add(new TableColumnMetadata("cde", ColumnType.INT));

        sink.clear();
        metadata.toJson(sink);
        TestUtils.assertEquals(expected, sink);

        try {
            metadata.add(new TableColumnMetadata("abc", ColumnType.FLOAT));
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "duplicate column [name=abc]");
        }

        sink.clear();
        metadata.toJson(sink);
        TestUtils.assertEquals(expected, sink);
    }

    @Test
    public void testDuplicateColumn2() {
        final String expected = "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"abc\",\"type\":\"INT\"},{\"index\":1,\"name\":\"cde\",\"type\":\"INT\"}],\"timestampIndex\":-1}";

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("abc", ColumnType.INT));
        metadata.add(new TableColumnMetadata("cde", ColumnType.INT));

        sink.clear();
        metadata.toJson(sink);
        TestUtils.assertEquals(expected, sink);

        try {
            metadata.add(new TableColumnMetadata("ABC", ColumnType.FLOAT));
            Assert.fail();
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "duplicate column [name=ABC]");
        }

        sink.clear();
        metadata.toJson(sink);
        TestUtils.assertEquals(expected, sink);
    }


    @Test
    public void testReuse() {
        String expected1 = "{\"columnCount\":3,\"columns\":[{\"index\":0,\"name\":\"abc\",\"type\":\"INT\"},{\"index\":1,\"name\":\"cde\",\"type\":\"INT\"},{\"index\":2,\"name\":\"timestamp\",\"type\":\"TIMESTAMP\"}],\"timestampIndex\":2}";
        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("abc", ColumnType.INT));
        metadata.add(new TableColumnMetadata("cde", ColumnType.INT));
        metadata.add(new TableColumnMetadata("timestamp", ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(2);

        sink.clear();
        metadata.toJson(sink);
        TestUtils.assertEquals(expected1, sink);

        String expected2 = "{\"columnCount\":2,\"columns\":[{\"index\":0,\"name\":\"x\",\"type\":\"SYMBOL\"},{\"index\":1,\"name\":\"z\",\"type\":\"DATE\"}],\"timestampIndex\":-1}";
        metadata.clear();
        metadata.add(new TableColumnMetadata("x", ColumnType.SYMBOL, IndexType.NONE, 0, false, null));
        metadata.add(new TableColumnMetadata("z", ColumnType.DATE));
        sink.clear();
        metadata.toJson(sink);
        TestUtils.assertEquals(expected2, sink);
    }
}