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

package com.nfsdb.impexp;

import com.nfsdb.Journal;
import com.nfsdb.factory.JournalReaderFactory;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.io.ExportManager;
import com.nfsdb.io.ImportManager;
import com.nfsdb.io.TextFileFormat;
import com.nfsdb.storage.ColumnType;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class ImportCsvTest extends AbstractTest {

    @Test
    public void testImport() throws Exception {
        String file = this.getClass().getResource("/csv/test-import.csv").getFile();
        ImportManager.importFile(factory, file, TextFileFormat.CSV, null);

        String location = "test-import.csv";


        Assert.assertEquals(JournalReaderFactory.JournalExistenceCheck.EXISTS, factory.exists(location));

        Journal r = factory.reader(location);
        JournalMetadata m = r.getMetadata();
        Assert.assertEquals(10, m.getColumnCount());
        Assert.assertEquals(ColumnType.SYMBOL, m.getColumnMetadata(0).type);
        Assert.assertEquals(ColumnType.SYMBOL, m.getColumnMetadata(1).type);
        Assert.assertEquals(ColumnType.INT, m.getColumnMetadata(2).type);
        Assert.assertEquals(ColumnType.DOUBLE, m.getColumnMetadata(3).type);
        Assert.assertEquals(ColumnType.DATE, m.getColumnMetadata(4).type);
        Assert.assertEquals(ColumnType.DATE, m.getColumnMetadata(5).type);
        Assert.assertEquals(ColumnType.DATE, m.getColumnMetadata(6).type);
        Assert.assertEquals(ColumnType.STRING, m.getColumnMetadata(7).type);
        Assert.assertEquals(ColumnType.BOOLEAN, m.getColumnMetadata(8).type);
        Assert.assertEquals(ColumnType.LONG, m.getColumnMetadata(9).type);

        File actual = new File(factory.getConfiguration().getJournalBase(), "exp.csv");
        File expected = new File(this.getClass().getResource("/csv/test-export-expected.csv").getFile());

        ExportManager.export(factory, location, actual.getAbsolutePath(), TextFileFormat.CSV);
        TestUtils.assertEquals(actual, expected);

        System.out.println(factory.getConfiguration().getJournalBase());
        System.out.println(m);
    }
}
