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

package com.questdb.txt;

import com.questdb.Journal;
import com.questdb.factory.configuration.JournalConfiguration;
import com.questdb.factory.configuration.JournalMetadata;
import com.questdb.ql.RecordSource;
import com.questdb.store.ColumnType;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class DelimitedTextParserTest extends AbstractTest {

    @Test
    public void testImport() throws Exception {
        String file = this.getClass().getResource("/csv/test-import.csv").getFile();
        ImportManager.importFile(theFactory.getMegaFactory(), file, TextFileDelimiter.CSV, null);

        String location = "test-import.csv";


        Assert.assertEquals(JournalConfiguration.EXISTS, getReaderFactory().getConfiguration().exists(location));

        try (Journal r = getReaderFactory().reader(location)) {
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

        File actual = new File(getReaderFactory().getConfiguration().getJournalBase(), "exp.csv");
        File expected = new File(this.getClass().getResource("/csv/test-export-expected.csv").getFile());

        try (RecordSource rs = compile("'" + location + "'")) {
            ExportManager.export(rs, theFactory.getMegaFactory(), actual, TextFileDelimiter.CSV);
            TestUtils.assertEquals(expected, actual);
        }
    }

    @Test
    public void testImportMalformedQuote() throws Exception {
        String file = this.getClass().getResource("/csv/test-import-malformed.csv").getFile();
        ImportManager.importFile(theFactory.getMegaFactory(), file, TextFileDelimiter.CSV, null);

        String location = "test-import-malformed.csv";

        Assert.assertEquals(JournalConfiguration.EXISTS, theFactory.getConfiguration().exists(location));

        try (Journal r = theFactory.getMegaFactory().reader(location)) {
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(10, m.getColumnCount());
            Assert.assertEquals(ColumnType.STRING, m.getColumn(0).type);
            Assert.assertEquals(ColumnType.STRING, m.getColumn(1).type);
            Assert.assertEquals(ColumnType.INT, m.getColumn(2).type);
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumn(3).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumn(4).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumn(5).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumn(6).type);
            Assert.assertEquals(ColumnType.STRING, m.getColumn(7).type);
            Assert.assertEquals(ColumnType.BOOLEAN, m.getColumn(8).type);
            Assert.assertEquals(ColumnType.LONG, m.getColumn(9).type);
        }
        File actual = new File(theFactory.getConfiguration().getJournalBase(), "exp.csv");
        File expected = new File(this.getClass().getResource("/csv/test-import-malformed-expected.csv").getFile());

        try (RecordSource rs = compile("'" + location + "'")) {
            ExportManager.export(rs, theFactory.getMegaFactory(), actual, TextFileDelimiter.CSV);
            TestUtils.assertEquals(expected, actual);
        }
    }

    @Test
    public void testImportNan() throws Exception {
        String file = this.getClass().getResource("/csv/test-import-nan.csv").getFile();
        ImportManager.importFile(theFactory.getMegaFactory(), file, TextFileDelimiter.CSV, null, 20);

        final String expected = "CMP1\t7\t4486\tNaN\t2015-02-05T19:15:09.000Z\n" +
                "CMP2\t8\t5256\tNaN\t2015-05-05T19:15:09.000Z\n" +
                "CMP2\t2\t6675\tNaN\t2015-05-07T19:15:09.000Z\n";
        assertThat(expected, "select StrSym, IntSym, IntCol, DoubleCol, IsoDate from 'test-import-nan.csv' where DoubleCol = NaN");
    }

    @Test
    public void testImportSchema() throws Exception {
        String file = this.getClass().getResource("/csv/test-import.csv").getFile();
        ImportManager.importFile(theFactory.getMegaFactory(), file, TextFileDelimiter.CSV, "IntSym=INT&Fmt2Date=STRING");
        String location = "test-import.csv";

        Assert.assertEquals(JournalConfiguration.EXISTS, getReaderFactory().getConfiguration().exists(location));

        try (Journal r = getReaderFactory().reader(location)) {
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(ColumnType.INT, m.getColumn(1).type);
            Assert.assertEquals(ColumnType.STRING, m.getColumn(6).type);
        }
    }


}
