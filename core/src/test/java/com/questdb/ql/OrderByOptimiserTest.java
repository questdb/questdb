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

import com.questdb.JournalWriter;
import com.questdb.ex.ParserException;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.ql.parser.QueryError;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class OrderByOptimiserTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $sym("id").index().valueCountHint(128).
                        $double("x").
                        $double("y").
                        $int("i1").
                        $int("i2").
                        $ts()
        );
        w.close();

        w = factory.writer(
                new JournalStructure("tex").
                        $sym("id").index().valueCountHint(128).
                        $double("amount").
                        $ts()
        );

        w.close();

        factory.getConfiguration().exists("");
    }

    @Before
    public void setUp() throws Exception {
        sink.clear();
    }

    @Test
    public void testLiteralAnalysis() throws Exception {
        try {
            expectFailure("select x,count() from tab order by timestamp");
        } catch (ParserException e) {
            Assert.assertEquals(35, QueryError.getPosition());
            TestUtils.assertEquals("Invalid column: timestamp", QueryError.getMessage());
        }
    }

    @Test
    public void testOrderOnOneLevelSubQuery() throws Exception {
        sink.put(compileSource("select x,count() from (tab order by timestamp)"));
        TestUtils.assertEquals("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"AggregatedRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}}", sink);
    }

    @Test
    public void testOrderOverride() throws Exception {
        sink.put(compileSource("select x,count() from ((tab order by y) order by timestamp)"));
        TestUtils.assertEquals("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"AggregatedRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}}", sink);
    }

    @Test
    public void testRegularOrder() throws Exception {
        sink.put(compileSource("select x,y from ((tab order by y) order by timestamp)"));
        TestUtils.assertEquals("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}}", sink);
    }

    @Test
    public void testSampleByBackout() throws Exception {
        sink.put(compileSource("(select x,count() from (select y, x, count() from (tab order by timestamp) sample by 1M order by y)) where x = 100"));
        TestUtils.assertEquals("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"AggregatedRecordSource\",\"src\":{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":false,\"src\":{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"ResampledRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}},\"sampler\":{\"op\":\"MonthsSampler\",\"buckets\":1}}}}}}", sink);
    }
}
