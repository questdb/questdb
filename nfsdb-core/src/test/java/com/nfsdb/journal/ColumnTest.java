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

package com.nfsdb.journal;

import com.nfsdb.journal.column.FixedWidthColumn;
import com.nfsdb.journal.column.MappedFile;
import com.nfsdb.journal.column.MappedFileImpl;
import com.nfsdb.journal.column.VarcharColumn;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalMetadata;
import com.nfsdb.journal.utils.Files;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class ColumnTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private File dataFile;
    private File indexFile;

    @Before
    public void setUp() throws JournalException {
        dataFile = new File(temporaryFolder.getRoot(), "col.d");
        indexFile = new File(temporaryFolder.getRoot(), "col.i");
    }

    @After
    public void tearDown() throws Exception {
        Files.deleteOrException(dataFile);
        Files.deleteOrException(indexFile);
    }

    @Test
    public void testFixedWidthColumns() throws JournalException {


        MappedFile mf = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);

        try (FixedWidthColumn pcc = new FixedWidthColumn(mf, 4)) {
            for (int i = 0; i < 10000; i++) {
                pcc.putInt(i);
                pcc.commit();
            }
        }

        MappedFile mf2 = new MappedFileImpl(dataFile, 22, JournalMode.READ);

        try (FixedWidthColumn pcc2 = new FixedWidthColumn(mf2, 4)) {
            Assert.assertEquals(66, pcc2.getInt(66));
            Assert.assertEquals(4597, pcc2.getInt(4597));
            Assert.assertEquals(120, pcc2.getInt(120));
            Assert.assertEquals(4599, pcc2.getInt(4599));
        }

        MappedFile mf3 = new MappedFileImpl(dataFile, 22, JournalMode.READ);
        try (FixedWidthColumn pcc3 = new FixedWidthColumn(mf3, 4)) {
            Assert.assertEquals(4598, pcc3.getInt(4598));
        }
    }

    @Test
    public void testVarcharColumn() throws JournalException {
        final int recordCount = 10000;

        MappedFile df1 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
        MappedFile idxFile1 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

        try (VarcharColumn varchar1 = new VarcharColumn(df1, idxFile1, JournalMetadata.BYTE_LIMIT)) {
            for (int i = 0; i < recordCount; i++) {
                varchar1.putString("s" + i);
                varchar1.commit();
            }
        }

        MappedFile df2 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
        MappedFile idxFile2 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

        try (VarcharColumn varchar2 = new VarcharColumn(df2, idxFile2, JournalMetadata.BYTE_LIMIT)) {
            Assert.assertEquals(recordCount, varchar2.size());
            for (int i = 0; i < varchar2.size(); i++) {
                String s = varchar2.getString(i);
                Assert.assertEquals("s" + i, s);
            }
        }
    }

    @Test
    public void testVarcharNulls() throws JournalException {
        MappedFile df1 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
        MappedFile idxFile1 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

        try (VarcharColumn varchar1 = new VarcharColumn(df1, idxFile1, JournalMetadata.BYTE_LIMIT)) {
            varchar1.putString("string1");
            varchar1.commit();
            varchar1.putString("string2");
            varchar1.commit();
            varchar1.putNull();
            varchar1.commit();
            varchar1.putString("string3");
            varchar1.commit();
            varchar1.putNull();
            varchar1.commit();
            varchar1.putString("string4");
            varchar1.commit();
        }

        MappedFile df2 = new MappedFileImpl(dataFile, 22, JournalMode.READ);
        MappedFile idxFile2 = new MappedFileImpl(indexFile, 22, JournalMode.READ);

        try (VarcharColumn varchar2 = new VarcharColumn(df2, idxFile2, JournalMetadata.BYTE_LIMIT)) {
            Assert.assertEquals("string1", varchar2.getString(0));
            Assert.assertEquals("string2", varchar2.getString(1));
//            Assert.assertNull(varchar2.getString(2));
            Assert.assertEquals("string3", varchar2.getString(3));
//            Assert.assertNull(varchar2.getString(4));
            Assert.assertEquals("string4", varchar2.getString(5));
        }
    }

    @Test
    public void testTruncate() throws JournalException {

        MappedFile df1 = new MappedFileImpl(dataFile, 22, JournalMode.APPEND);
        MappedFile idxFile1 = new MappedFileImpl(indexFile, 22, JournalMode.APPEND);

        try (VarcharColumn varchar1 = new VarcharColumn(df1, idxFile1, JournalMetadata.BYTE_LIMIT)) {
            varchar1.putString("string1");
            varchar1.commit();
            varchar1.putString("string2");
            varchar1.commit();
            varchar1.putNull();
            varchar1.commit();
            varchar1.putString("string3");
            varchar1.commit();
            varchar1.putNull();
            varchar1.commit();
            varchar1.putString("string4");
            varchar1.commit();

            Assert.assertEquals(6, varchar1.size());
            varchar1.truncate(4);
            varchar1.commit();
            Assert.assertEquals(4, varchar1.size());
            Assert.assertEquals("string1", varchar1.getString(0));
            Assert.assertEquals("string2", varchar1.getString(1));
//            Assert.assertNull(varchar1.getString(2));
            Assert.assertEquals("string3", varchar1.getString(3));

        }

        MappedFile df2 = new MappedFileImpl(dataFile, 22, JournalMode.READ);
        MappedFile idxFile12 = new MappedFileImpl(indexFile, 22, JournalMode.READ);

        try (VarcharColumn varchar2 = new VarcharColumn(df2, idxFile12, JournalMetadata.BYTE_LIMIT)) {
            Assert.assertEquals("string1", varchar2.getString(0));
            Assert.assertEquals("string2", varchar2.getString(1));
//            Assert.assertNull(varchar2.getString(2));
            Assert.assertEquals("string3", varchar2.getString(3));
        }
    }

    @Test
    public void testFixedWidthFloat() throws Exception {
        try (FixedWidthColumn col = new FixedWidthColumn(new MappedFileImpl(dataFile, 22, JournalMode.APPEND), 4)) {
            int max = 150;
            for (int i = 0; i < max; i++) {
                col.putFloat((max - i) + 0.33f);
                col.commit();
            }

            for (long l = 0; l < col.size(); l++) {
                Assert.assertEquals(max - l + 0.33f, col.getFloat(l), 0);
            }
        }
    }

    @Test
    public void testFixedWidthNull() throws Exception {
        try (FixedWidthColumn col = new FixedWidthColumn(new MappedFileImpl(dataFile, 22, JournalMode.APPEND), 4)) {
            int max = 150;
            for (int i = 0; i < max; i++) {
                col.putNull();
                col.commit();
            }

            Assert.assertEquals(max, col.size());
        }
    }
}
