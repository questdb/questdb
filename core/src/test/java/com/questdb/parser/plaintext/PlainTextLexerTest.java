/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.parser.plaintext;

import com.questdb.ql.RecordSource;
import com.questdb.store.ColumnType;
import com.questdb.store.Journal;
import com.questdb.store.factory.configuration.JournalConfiguration;
import com.questdb.store.factory.configuration.JournalMetadata;
import com.questdb.store.util.ExportManager;
import com.questdb.store.util.ImportManager;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class PlainTextLexerTest extends AbstractTest {

    @Test
    public void testForcedHeader() throws Exception {
        String file = this.getClass().getResource("/csv/test-explicit-headers.csv").getFile();
        ImportManager.importFile(getFactory(), file, PlainTextDelimiter.CSV, null, 20, true);

        final String expected = "ABC\txy\ta\tbrown fox jumped over the fence\tGROUP1\n" +
                "CDE\tbb\tb\tsentence 1\n" +
                "sentence 2\tGROUP1\n";
        assertThat(expected, "'test-explicit-headers.csv'", false);
    }

    @Test
    public void testImport() throws Exception {
        String file = this.getClass().getResource("/csv/test-import.csv").getFile();
        ImportManager.importFile(getFactory(), file, PlainTextDelimiter.CSV, null, false);

        String location = "test-import.csv";


        Assert.assertEquals(JournalConfiguration.EXISTS, getFactory().getConfiguration().exists(location));

        try (Journal r = getFactory().reader(location)) {
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(10, m.getColumnCount());
            Assert.assertEquals(ColumnType.STRING, m.getColumnQuick(0).type);
            Assert.assertEquals(ColumnType.INT, m.getColumnQuick(1).type);
            Assert.assertEquals(ColumnType.INT, m.getColumnQuick(2).type);
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumnQuick(3).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumnQuick(4).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumnQuick(5).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumnQuick(6).type);
            Assert.assertEquals(ColumnType.STRING, m.getColumnQuick(7).type);
            Assert.assertEquals(ColumnType.BOOLEAN, m.getColumnQuick(8).type);
            Assert.assertEquals(ColumnType.LONG, m.getColumnQuick(9).type);
        }

        File actual = new File(getFactory().getConfiguration().getJournalBase(), "exp.csv");
        File expected = new File(this.getClass().getResource("/csv/test-export-expected.csv").getFile());

        try (RecordSource rs = compile("'" + location + "'")) {
            ExportManager.export(rs, getFactory(), actual, PlainTextDelimiter.CSV);
            TestUtils.assertEquals(expected, actual);
        }
    }

    @Test
    public void testImportMalformedQuote() throws Exception {
        String file = this.getClass().getResource("/csv/test-import-malformed.csv").getFile();
        ImportManager.importFile(getFactory(), file, PlainTextDelimiter.CSV, null, false);

        String location = "test-import-malformed.csv";

        Assert.assertEquals(JournalConfiguration.EXISTS, factoryContainer.getConfiguration().exists(location));

        try (Journal r = getFactory().reader(location)) {
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(10, m.getColumnCount());
            Assert.assertEquals(ColumnType.STRING, m.getColumnQuick(0).type);
            Assert.assertEquals(ColumnType.STRING, m.getColumnQuick(1).type);
            Assert.assertEquals(ColumnType.INT, m.getColumnQuick(2).type);
            Assert.assertEquals(ColumnType.DOUBLE, m.getColumnQuick(3).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumnQuick(4).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumnQuick(5).type);
            Assert.assertEquals(ColumnType.DATE, m.getColumnQuick(6).type);
            Assert.assertEquals(ColumnType.STRING, m.getColumnQuick(7).type);
            Assert.assertEquals(ColumnType.BOOLEAN, m.getColumnQuick(8).type);
            Assert.assertEquals(ColumnType.LONG, m.getColumnQuick(9).type);
        }
        File actual = new File(factoryContainer.getConfiguration().getJournalBase(), "exp.csv");
        File expected = new File(this.getClass().getResource("/csv/test-import-malformed-expected.csv").getFile());

        try (RecordSource rs = compile("'" + location + "'")) {
            ExportManager.export(rs, getFactory(), actual, PlainTextDelimiter.CSV);
            TestUtils.assertEquals(expected, actual);
        }
    }

    @Test
    public void testImportNan() throws Exception {
        String file = this.getClass().getResource("/csv/test-import-nan.csv").getFile();
        ImportManager.importFile(getFactory(), file, PlainTextDelimiter.CSV, null, 20, false);

        final String expected = "CMP1\t7\t4486\tNaN\t2015-02-05T19:15:09.000Z\n" +
                "CMP2\t8\t5256\tNaN\t2015-05-05T19:15:09.000Z\n" +
                "CMP2\t2\t6675\tNaN\t2015-05-07T19:15:09.000Z\n";
        assertThat(expected, "select StrSym, IntSym, IntCol, DoubleCol, IsoDate from 'test-import-nan.csv' where DoubleCol = NaN");
    }

    @Test
    public void testImportSchema() throws Exception {
        String file = this.getClass().getResource("/csv/test-import.csv").getFile();
        String schema = "[{\"name\":\"IntSym\", \"type\":\"SYMBOL\"}, {\"name\":\"Fmt2Date\", \"type\":\"STRING\"}]";
        ImportManager.importFile(getFactory(), file, PlainTextDelimiter.CSV, schema, false);
        String location = "test-import.csv";

        Assert.assertEquals(JournalConfiguration.EXISTS, getFactory().getConfiguration().exists(location));

        try (Journal r = getFactory().reader(location)) {
            JournalMetadata m = r.getMetadata();
            Assert.assertEquals(ColumnType.SYMBOL, m.getColumnQuick(1).type);
            Assert.assertEquals(ColumnType.STRING, m.getColumnQuick(6).type);
        }
    }


}
