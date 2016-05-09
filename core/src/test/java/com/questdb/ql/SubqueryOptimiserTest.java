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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
 ******************************************************************************/

package com.questdb.ql;

import com.questdb.JournalWriter;
import com.questdb.factory.configuration.JournalStructure;
import com.questdb.ql.parser.AbstractOptimiserTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SubqueryOptimiserTest extends AbstractOptimiserTest {

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

    }

    @Before
    public void setUp() throws Exception {
        sink.clear();
    }

    @Test
    public void testJoinSubQueryFilter() throws Exception {
        sink.put(compiler.compileSource(factory, "(tab a join tex b on a.id = b.id) a where a.x = 10"));
        TestUtils.assertEquals("{\"op\":\"HashJoinRecordSource\",\"master\":{\"op\":\"JournalSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}},\"slave\":{\"op\":\"JournalSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tex\"},\"rsrc\":{\"op\":\"AllRowSource\"}},\"joinOn\":[id],[id]}",
                sink);
    }

    @Test
    public void testOneLevelAliasedSelectedSubQuery() throws Exception {
        sink.put(compiler.compileSource(factory, "(select x from tab order by x) a where a.x = 10"));
        TestUtils.assertEquals("{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"JournalSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}}",
                sink);
    }

    @Test
    public void testOneLevelAliasedSubQuery() throws Exception {
        sink.put(compiler.compileSource(factory, "(tab order by x) a where a.x = 10"));
        TestUtils.assertEquals("{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}",
                sink);
    }

    @Test
    public void testOneLevelSimpleSubQuery() throws Exception {
        sink.put(compiler.compileSource(factory, "(tab order by x) where x = 10"));
        TestUtils.assertEquals("{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}",
                sink);
    }

    @Test
    public void testRecursiveAliasedSubQuery() throws Exception {
        sink.put(compiler.compileSource(factory, "((tab order by x) a where a.x = 10) b where b.y > 100"));
        TestUtils.assertEquals("{\"op\":\"RBTreeSortedRecordSource\",\"byRowId\":true,\"src\":{\"op\":\"JournalSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"FilteredRowSource\",\"rsrc\":{\"op\":\"AllRowSource\"}}}}",
                sink);
    }

    @Test
    public void testSubQueryFilterOnAggregate() throws Exception {
        sink.put(compiler.compileSource(factory, "(select sum(x) k from tab) a where a.k = 10"));
        TestUtils.assertEquals("{\"op\":\"FilteredJournalRecordSource\",\"src\":{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"AggregatedRecordSource\",\"src\":{\"op\":\"JournalSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}},\"filter\":\"a.k = 10\"}",
                sink);
    }

    @Test
    public void testSubQueryFilterOnConstant() throws Exception {
        sink.put(compiler.compileSource(factory, "(select 1 k from tab) a where a.k = 10"));
        TestUtils.assertEquals("{\"op\":\"FilteredJournalRecordSource\",\"src\":{\"op\":\"SelectedColumnsRecordSource\",\"src\":{\"op\":\"VirtualColumnRecordSource\",\"src\":{\"op\":\"JournalSource\",\"psrc\":{\"op\":\"JournalPartitionSource\",\"journal\":\"tab\"},\"rsrc\":{\"op\":\"AllRowSource\"}}}},\"filter\":\"a.k = 10\"}",
                sink);
    }
}
