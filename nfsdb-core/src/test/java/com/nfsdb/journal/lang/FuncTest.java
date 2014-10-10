/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.lang;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.Partition;
import com.nfsdb.journal.column.AbstractColumn;
import com.nfsdb.journal.column.FixedColumn;
import com.nfsdb.journal.factory.configuration.JournalMetadata;
import com.nfsdb.journal.lang.cst.Choice;
import com.nfsdb.journal.lang.cst.PartitionSlice;
import com.nfsdb.journal.lang.cst.RowAcceptor;
import com.nfsdb.journal.lang.cst.impl.fltr.DoubleGreaterThanRowFilter;
import org.junit.Assert;
import org.junit.Test;

import static org.easymock.EasyMock.*;

public class FuncTest {

    @Test
    public void testGreaterThan() throws Exception {
        DoubleGreaterThanRowFilter filter = new DoubleGreaterThanRowFilter("test", 10);

        FixedColumn column = createMock(FixedColumn.class);
        // let row 100 have value 5.0
        // let row 101 have value 11.0
        // let row 102 have value 10
        expect(column.getDouble(100)).andReturn(5d);
        expect(column.getDouble(101)).andReturn(11d);
        expect(column.getDouble(102)).andReturn(10d);
        replay(column);

        PartitionSlice slice = mockPartitionColumn("test", 5, column);
        RowAcceptor acceptor = filter.acceptor(slice);
        Assert.assertEquals(Choice.SKIP, acceptor.accept(100));
        Assert.assertEquals(Choice.PICK, acceptor.accept(101));
        Assert.assertEquals(Choice.SKIP, acceptor.accept(102));
    }

    private PartitionSlice mockPartitionColumn(String name, int index, AbstractColumn column) {
        JournalMetadata metadata = createMock(JournalMetadata.class);
        expect(metadata.getColumnIndex(name)).andReturn(index);
        replay(metadata);

        Journal journal = createMock(Journal.class);
        expect(journal.getMetadata()).andReturn(metadata);
        replay(journal);

        Partition partition = createMock(Partition.class);
        expect(partition.getJournal()).andReturn(journal);
        expect(partition.getAbstractColumn(index)).andReturn(column);
        replay(partition);

        PartitionSlice slice = new PartitionSlice();
        slice.partition = partition;

        return slice;
    }
}
