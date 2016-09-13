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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SubqueryOptimiserTest extends AbstractOptimiserTest {

    @BeforeClass
    public static void setUpClass() throws Exception {
        JournalWriter w = factory.writer(
                new JournalStructure("tab").
                        $sym("id").index().buckets(128).
                        $double("x").
                        $double("y").
                        $int("i1").
                        $int("i2").
                        $ts()
        );
        w.close();

        w = factory.writer(
                new JournalStructure("tex").
                        $sym("id").index().buckets(128).
                        $double("amount").
                        $ts()
        );

        w.close();

    }

    @Before
    public void setUp() throws Exception {
        sink.clear();
    }

    @Test
    public void testAmbiguousColumn() throws Exception {
        try {
            expectFailure("(((tab order by y) where y = 5) a join tex b on a.id = b.id) a where a.x = 10 and id > 100");
        } catch (ParserException e) {
            TestUtils.assertEquals("Ambiguous column name", QueryError.getMessage());
        }
    }

    @Test
    public void testJoinRecursiveJoinSubQueries() throws Exception {
        sink.put(compileSource("(((tab order by y) where y = 5) a join tex b on a.id = b.id) a where a.x = 10 and a.amount > 100"));
        TestUtils.assertEquals("{\"op\":\"HashJoinRecordSource\",\"master\":{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}},\"slave\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tex\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}},\"joinOn\":[[\"id\"],[\"id\"]]}",
                sink);
    }

    @Test
    public void testJoinSubQueries() throws Exception {
        sink.put(compileSource("((tab order by y) a join tex b on a.id = b.id) a where a.x = 10 and a.amount > 100"));
        TestUtils.assertEquals("{\"op\":\"HashJoinRecordSource\",\"master\":{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}},\"slave\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tex\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}},\"joinOn\":[[\"id\"],[\"id\"]]}",
                sink);
    }

    @Test
    public void testJoinSubQueryFilter() throws Exception {
        sink.put(compileSource("(tab a join tex b on a.id = b.id) a where a.x = 10"));
        TestUtils.assertEquals("{\"op\":\"HashJoinRecordSource\",\"master\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}},\"slave\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tex\"},\"rsrc\":{\"op\":\"AllRowSource\"}},\"joinOn\":[[\"id\"],[\"id\"]]}",
                sink);
    }

    @Test
    public void testJoinSubQueryFilter2() throws Exception {
        sink.put(compileSource("(tab a join tex b on a.id = b.id) a where a.amount = 10"));
        TestUtils.assertEquals("{\"op\":\"HashJoinRecordSource\",\"master\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}},\"slave\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tex\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}},\"joinOn\":[[\"id\"],[\"id\"]]}",
                sink);
    }

    @Test
    public void testOneLevelAliasedSelectedSubQuery() throws Exception {
        sink.put(compileSource("(select x from tab order by x) a where a.x = 10"));
        TestUtils.assertEquals("{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}}",
                sink);
    }

    @Test
    public void testOneLevelAliasedSubQuery() throws Exception {
        sink.put(compileSource("(tab order by x) a where a.x = 10"));
        TestUtils.assertEquals("{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}",
                sink);
    }

    @Test
    public void testOneLevelSimpleSubQuery() throws Exception {
        sink.put(compileSource("(tab order by x) where x = 10"));
        TestUtils.assertEquals("{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}",
                sink);
    }

    @Test
    public void testRecursiveAliasedMixedSubQuery() throws Exception {
        sink.put(compileSource("(select y from (select 1+1 y, x from tab order by x) a where a.x = 10) b where b.y > 100"));
        TestUtils.assertEquals("{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"FilteredRecordSource\",\"src\":{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"VirtualColumnRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}}},\"filter\":\"y > 100\"}}",
                sink);
    }

    @Test
    public void testRecursiveAliasedSubQuery() throws Exception {
        sink.put(compileSource("((tab order by x) a where a.x = 10) b where b.y > 100"));
        TestUtils.assertEquals("{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}",
                sink);
    }

    @Test
    public void testSubQueryFilterOnAggregate() throws Exception {
        sink.put(compileSource("(select sum(x) k from tab) a where a.k = 10"));
        TestUtils.assertEquals("{\"op\":\"FilteredRecordSource\",\"src\":{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"AggregatedRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}},\"filter\":\"a.k = 10\"}",
                sink);
    }

    @Test
    public void testSubQueryFilterOnConstant() throws Exception {
        sink.put(compileSource("(select 1 k from tab) a where a.k = 10"));
        TestUtils.assertEquals("{\"op\":\"FilteredRecordSource\",\"src\":{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"VirtualColumnRecordSource\",\"src\":{\"op\":\"JournalRecordSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}},\"filter\":\"a.k = 10\"}",
                sink);
    }
}
