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

package com.questdb.cairo;

import com.questdb.cairo.sql.InvalidColumnException;
import com.questdb.common.ColumnType;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class GenericRecordMetadataTest {
    private static final StringSink sink = new StringSink();

    @Test
    public void testBaseInterfaceDefaults() {

        GenericRecordMetadata metadata = new GenericRecordMetadata();
        metadata.add(new TableColumnMetadata("abc", ColumnType.INT));
        metadata.add(new TableColumnMetadata("cde", ColumnType.SYMBOL, true, 1024));
        metadata.add(new TableColumnMetadata("timestamp", ColumnType.TIMESTAMP));
        metadata.setTimestampIndex(2);

        Assert.assertEquals(ColumnType.TIMESTAMP, metadata.getColumnType("timestamp"));

        try {
            metadata.getColumnIndex("xyz");
            Assert.fail();
        } catch (InvalidColumnException ignore) {
        }

        Assert.assertEquals(1024, metadata.getIndexValueBlockCapacity("cde"));
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
            TestUtils.assertContains(e.getMessage(), "Duplicate column");
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
        metadata.add(new TableColumnMetadata("x", ColumnType.SYMBOL));
        metadata.add(new TableColumnMetadata("z", ColumnType.DATE));
        sink.clear();
        metadata.toJson(sink);
        TestUtils.assertEquals(expected2, sink);
    }
}