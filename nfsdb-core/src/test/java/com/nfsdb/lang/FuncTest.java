/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.lang;

import com.nfsdb.JournalEntryWriter;
import com.nfsdb.JournalWriter;
import com.nfsdb.factory.configuration.JournalStructure;
import com.nfsdb.lang.cst.Choice;
import com.nfsdb.lang.cst.PartitionSlice;
import com.nfsdb.lang.cst.RowAcceptor;
import com.nfsdb.lang.cst.impl.fltr.DoubleGreaterThanRowFilter;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.utils.Rnd;
import org.junit.Assert;
import org.junit.Test;


public class FuncTest extends AbstractTest {

    @Test
    public void testGreaterThan() throws Exception {

        // configure journal "xxx" with single double column "price"
        JournalWriter w = factory.writer(new JournalStructure("xxx").$double("price"));

        // populate 500 rows with pseudo-random double values
        Rnd rnd = new Rnd();
        for (int i = 0; i < 500; i++) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putDouble(0, rnd.nextDouble());
            ew.append();
        }
        w.commit();

        // create test slice that covers entire journal
        // our journal has only one partition
        PartitionSlice slice = new PartitionSlice();
        slice.lo = 0;
        slice.partition = w.getLastPartition();
        slice.hi = slice.partition.size() - 1;

        // create filter and acceptor
        DoubleGreaterThanRowFilter filter = new DoubleGreaterThanRowFilter("price", 10);
        RowAcceptor acceptor = filter.acceptor(slice);

        for (long row = slice.lo; row <= slice.hi; row++) {
            Choice choice = acceptor.accept(row);
            switch (choice) {
                case PICK:
                    Assert.assertTrue(slice.partition.getDouble(row, 0) > 10);
                    break;
                default:
                    Assert.assertTrue(slice.partition.getDouble(row, 0) <= 10);
            }
        }
    }
}
