/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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
 ******************************************************************************/

package com.nfsdb.io;

import com.nfsdb.Journal;
import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.ParserException;
import com.nfsdb.factory.configuration.JournalConfiguration;
import com.nfsdb.factory.configuration.JournalMetadata;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.store.ColumnType;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ImportCsvTest extends AbstractTest {

    @Test
    public void testImport() throws Exception {
        String file = this.getClass().getResource("/csv/test-import.csv").getFile();
        ImportManager.importFile(factory, file, TextFileFormat.CSV, null);

        String location = "test-import.csv";


        Assert.assertEquals(JournalConfiguration.JournalExistenceCheck.EXISTS, factory.getConfiguration().exists(location));

        try (Journal r = factory.reader(location)) {
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(10, m.getColumnCount());
            Assert.assertEquals(ColumnType.STRING, m.getColumn(0).type);
            Assert.assertEquals(ColumnType.INT, m.getColumn(1).type);
            Assert.assertEquals(ColumnType.INT, m.getColumn(2).type);
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn(3).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumn(4).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumn(5).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumn(6).type);
            Assert.assertEquals(ColumnType.STRING, m.getColumn(7).type);
            Assert.assertEquals(ColumnType.BOOLEAN, m.getColumn(8).type);
            Assert.assertEquals(ColumnType.LONG, m.getColumn(9).type);
        }

        File actual = new File(factory.getConfiguration().getJournalBase(), "exp.csv");
        File expected = new File(this.getClass().getResource("/csv/test-export-expected.csv").getFile());

        ExportManager.export(compiler.compile(factory, "'" + location + "'"), actual, TextFileFormat.CSV);
        TestUtils.assertEquals(expected, actual);
    }

    @Test
    public void testImportNan() throws Exception {
        imp("/csv/test-import-nan.csv");
        final String expected = "CMP1\t7\t4486\tNaN\t2015-02-05T19:15:09.000Z\n" +
                "CMP2\t8\t5256\tNaN\t2015-05-05T19:15:09.000Z\n" +
                "CMP2\t2\t6675\tNaN\t2015-05-07T19:15:09.000Z\n";
        assertThat(expected, "select StrSym, IntSym, IntCol, DoubleCol, IsoDate from 'test-import-nan.csv' where DoubleCol = NaN");
    }

    @Test
    public void testImportSchema() throws Exception {
        String file = this.getClass().getResource("/csv/test-import.csv").getFile();
        ImportManager.importFile(factory, file, TextFileFormat.CSV, new ImportSchema("1,INT,\n6,STRING,"));
        String location = "test-import.csv";


        Assert.assertEquals(JournalConfiguration.JournalExistenceCheck.EXISTS, factory.getConfiguration().exists(location));

        Journal r = factory.reader(location);
        JournalMetadata m = r.getMetadata();
        Assert.assertEquals(ColumnType.INT, m.getColumn(1).type);
        Assert.assertEquals(ColumnType.STRING, m.getColumn(6).type);
    }

    @Test
    public void testImportSchemaFile() throws Exception {
        String file = this.getClass().getResource("/csv/test-import.csv").getFile();
        ImportManager.importFile(factory, file, TextFileFormat.CSV, new ImportSchema(
                new File(this.getClass().getResource("/csv/test-import-schema.csv").getFile())
        ));
        String location = "test-import.csv";


        Assert.assertEquals(JournalConfiguration.JournalExistenceCheck.EXISTS, factory.getConfiguration().exists(location));

        Journal r = factory.reader(location);
        JournalMetadata m = r.getMetadata();
        Assert.assertEquals(ColumnType.INT, m.getColumn(1).type);
        Assert.assertEquals(ColumnType.STRING, m.getColumn(6).type);
    }

    private void assertThat(String expected, String query) throws ParserException, JournalException, IOException {
        StringSink sink = new StringSink();
        RecordSourcePrinter p = new RecordSourcePrinter(sink);
        p.printCursor(compiler.compile(factory, query));
        Assert.assertEquals(expected, sink.toString());

    }

    private void imp(String resource) throws IOException {
        String file = this.getClass().getResource(resource).getFile();
        ImportManager.importFile(factory, file, TextFileFormat.CSV, null, 20);
    }
}
