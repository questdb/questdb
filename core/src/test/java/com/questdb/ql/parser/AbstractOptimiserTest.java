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

package com.questdb.ql.parser;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.questdb.ex.ParserException;
import com.questdb.io.RecordSourcePrinter;
import com.questdb.io.sink.StringSink;
import com.questdb.misc.Files;
import com.questdb.misc.Misc;
import com.questdb.misc.Unsafe;
import com.questdb.model.configuration.ModelConfiguration;
import com.questdb.ql.Record;
import com.questdb.ql.RecordCursor;
import com.questdb.ql.RecordSource;
import com.questdb.std.AssociativeCache;
import com.questdb.store.SymbolTable;
import com.questdb.test.tools.JournalTestFactory;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractOptimiserTest {

    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));
    protected static final StringSink sink = new StringSink();
    protected static final RecordSourcePrinter printer = new RecordSourcePrinter(sink);
    protected static final QueryCompiler compiler = new QueryCompiler();
    private static final AssociativeCache<RecordSource> cache = new AssociativeCache<>(8, 16);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final JsonParser jp = new JsonParser();

    protected static void assertRowId(String query, String longColumn) throws ParserException {
        RecordSource src = compiler.compile(factory, query);
        try {
            RecordCursor cursor = src.prepareCursor(factory);

            int dateIndex = src.getMetadata().getColumnIndex(longColumn);
            HashMap<Long, Long> map = new HashMap<>();

            long count = 0;
            while (cursor.hasNext()) {
                Record record = cursor.next();
                map.put(record.getRowId(), record.getLong(dateIndex));
                count++;
            }

            Assert.assertTrue(count > 0);
            Record record = cursor.newRecord();
            for (Map.Entry<Long, Long> e : map.entrySet()) {
                Assert.assertEquals((long) e.getValue(), cursor.recordAt(e.getKey()).getLong(dateIndex));
                cursor.recordAt(record, e.getKey());
                Assert.assertEquals((long) e.getValue(), record.getLong(dateIndex));
            }

        } finally {
            Misc.free(src);
        }
    }

    protected void assertEmpty(String query) throws ParserException {
        try (RecordSource src = compiler.compile(factory, query)) {
            Assert.assertFalse(src.prepareCursor(factory).hasNext());
        }
    }

    protected void assertPlan(String expected, String query) throws ParserException {
        TestUtils.assertEquals(expected, compiler.plan(factory, query));
    }

    protected void assertPlan2(CharSequence expected, CharSequence query) throws ParserException {
        sink.clear();
        try (RecordSource recordSource = compiler.compile(factory, query)) {
            recordSource.toSink(sink);
            String s = gson.toJson(jp.parse(sink.toString()));
            TestUtils.assertEquals(expected, s);
        }
    }

    protected void assertString(String query, int columnIndex) throws ParserException {
        try (RecordSource rs = compiler.compile(factory, query)) {
            RecordCursor cursor = rs.prepareCursor(factory);
            while (cursor.hasNext()) {
                Record r = cursor.next();
                int len = r.getStrLen(columnIndex);
                CharSequence s = r.getStr(columnIndex);
                if (s != null) {
                    CharSequence csA = r.getFlyweightStr(columnIndex);
                    CharSequence csB = r.getFlyweightStrB(columnIndex);
                    TestUtils.assertEquals(s, csA);
                    TestUtils.assertEquals(s, csB);
                    Assert.assertEquals(len, s.length());
                    Assert.assertFalse(csA == csB);
                } else {
                    Assert.assertEquals(-1, len);
                    Assert.assertNull(r.getFlyweightStr(columnIndex));
                    Assert.assertNull(r.getFlyweightStrB(columnIndex));
                }
            }
        }
    }

    protected void assertSymbol(String query, int columnIndex) throws ParserException {
        try (RecordSource src = compiler.compile(factory, query)) {
            RecordCursor cursor = src.prepareCursor(factory);
            SymbolTable tab = cursor.getStorageFacade().getSymbolTable(columnIndex);
            Assert.assertNotNull(factory);
            while (cursor.hasNext()) {
                Record r = cursor.next();
                TestUtils.assertEquals(r.getSym(columnIndex), tab.value(r.getInt(columnIndex)));
            }
        }
    }

    protected void assertThat(String expected, String query) throws ParserException, IOException {
        assertThat(expected, query, false);
    }

    protected void assertThat(String expected, String query, boolean header) throws ParserException, IOException {
        long allocated = Unsafe.getMemUsed();
        assertThat0(expected, query, header);
        assertThat0(expected, query, header);
        Misc.free(cache.poll(query));
        Assert.assertEquals(allocated, Unsafe.getMemUsed());
    }

    protected void assertThat(String expected, RecordSource rs, boolean header) throws IOException {
        RecordCursor cursor = rs.prepareCursor(factory);

        sink.clear();
        printer.print(cursor, header, rs.getMetadata());
        TestUtils.assertEquals(expected, sink);

        cursor.toTop();

        sink.clear();
        printer.print(cursor, header, rs.getMetadata());
        TestUtils.assertEquals(expected, sink);

        TestUtils.assertStrings(rs, factory);
    }

    private void assertThat0(String expected, String query, boolean header) throws ParserException, IOException {
        RecordSource rs = cache.peek(query);
        if (rs == null) {
            cache.put(query, rs = compiler.compile(factory, query));
        }
        assertThat(expected, rs, header);
    }

    protected RecordSource compileSource(CharSequence query) throws ParserException {
        return compiler.compile(factory, query);
    }

    protected void expectFailure(CharSequence query) throws ParserException {
        long memUsed = Unsafe.getMemUsed();
        try {
            compiler.compile(factory, query);
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(memUsed, Unsafe.getMemUsed());
            throw e;
        }
    }
}
