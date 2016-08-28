/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2016 Appsicle
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

package com.questdb.ql;

import com.questdb.Journal;
import com.questdb.PartitionBy;
import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.misc.Files;
import com.questdb.model.configuration.ModelConfiguration;
import com.questdb.ql.parser.QueryCompiler;
import com.questdb.ql.parser.QueryError;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.JournalTestFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class DDLTests {

    public static final JournalTestFactory factory = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));
    private static final QueryCompiler compiler = new QueryCompiler();

    @After
    public void tearDown() throws Exception {
        Files.deleteOrException(new File(factory.getConfiguration().getJournalBase(), "x"));
    }

    @Test
    public void testBadIntBuckets() throws Exception {
        try {
            compiler.execute(factory, "create journal x (a INT index buckets -1, b BYTE, t TIMESTAMP, x SYMBOL) partition by MONTH");
            Assert.fail();
        } catch (ParserException ignore) {
            // good, pass
        }
    }

    @Test
    public void testCreateAllFieldTypes() throws Exception {
        compiler.execute(factory, "create journal x (a INT, b BYTE, c SHORT, d LONG, e FLOAT, f DOUBLE, g DATE, h BINARY, t TIMESTAMP, x SYMBOL, z STRING) partition by MONTH");

        // validate journal
        try (Journal r = factory.reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(11, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());

            Assert.assertEquals(ColumnType.SHORT, m.getColumn("c").getType());
            Assert.assertEquals(ColumnType.LONG, m.getColumn("d").getType());
            Assert.assertEquals(ColumnType.FLOAT, m.getColumn("e").getType());
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn("f").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("g").getType());
            Assert.assertEquals(ColumnType.BINARY, m.getColumn("h").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());

            Assert.assertEquals(8, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }

        // test query execute fails

        try {
            compiler.execute(factory, "x");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(0, QueryError.getPosition());
        }
    }

    @Test
    public void testCreateIndexedInt() throws Exception {
        compiler.execute(factory, "create journal x (a INT index buckets 25, b BYTE, t TIMESTAMP, x SYMBOL) partition by MONTH");
        // validate journal
        try (Journal r = factory.reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(31, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateIndexedIntDefaultBuckets() throws Exception {
        compiler.execute(factory, "create journal x (a INT index, b BYTE, t TIMESTAMP, x SYMBOL) partition by MONTH");
        // validate journal
        try (Journal r = factory.reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(1023, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateIndexedString() throws Exception {
        compiler.execute(factory, "create journal x (a INT index, b BYTE, t TIMESTAMP, z STRING index buckets 40) partition by MONTH");
        // validate journal
        try (Journal r = factory.reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(1023, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.STRING, m.getColumn("z").getType());
            Assert.assertTrue(m.getColumn("z").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(63, m.getColumn("z").getBucketCount());

            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }

    @Test
    public void testCreateIndexedSymbol() throws Exception {
        compiler.execute(factory, "create journal x (a INT index buckets 25, b BYTE, t TIMESTAMP, x SYMBOL index) partition by MONTH");
        // validate journal
        try (Journal r = factory.reader("x")) {
            Assert.assertNotNull(r);
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(4, m.getColumnCount());
            Assert.assertEquals(ColumnType.INT, m.getColumn("a").getType());
            Assert.assertTrue(m.getColumn("a").isIndexed());
            // bucket is ceilPow2(value) - 1
            Assert.assertEquals(31, m.getColumn("a").getBucketCount());

            Assert.assertEquals(ColumnType.BYTE, m.getColumn("b").getType());
            Assert.assertEquals(ColumnType.DATE, m.getColumn("t").getType());
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumn("x").getType());
            Assert.assertTrue(m.getColumn("x").isIndexed());
            Assert.assertEquals(2, m.getTimestampIndex());
            Assert.assertEquals(PartitionBy.MONTH, m.getPartitionBy());
        }
    }
}
