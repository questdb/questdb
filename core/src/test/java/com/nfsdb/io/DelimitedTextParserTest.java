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

public class DelimitedTextParserTest extends AbstractTest {

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
        ImportManager.importFile(factory, file, TextFileFormat.CSV, "IntSym=INT&Fmt2Date=STRING");
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
