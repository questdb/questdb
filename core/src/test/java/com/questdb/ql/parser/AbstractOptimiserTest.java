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
import com.questdb.ex.JournalException;
import com.questdb.ex.ParserException;
import com.questdb.io.RecordSourcePrinter;
import com.questdb.io.sink.StringSink;
import com.questdb.misc.Files;
import com.questdb.misc.Misc;
import com.questdb.model.configuration.ModelConfiguration;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.RecordSource;
import com.questdb.ql.impl.NoOpCancellationHandler;
import com.questdb.std.AssociativeCache;
import com.questdb.test.tools.JournalTestFactory;
import com.questdb.test.tools.TestUtils;
import org.junit.ClassRule;

import java.io.IOException;

public abstract class AbstractOptimiserTest {

    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));
    protected static final QueryCompiler compiler = new QueryCompiler(new ServerConfiguration());
    protected static final StringSink sink = new StringSink();
    protected static final RecordSourcePrinter printer = new RecordSourcePrinter(sink);
    private static final AssociativeCache<RecordSource> cache = new AssociativeCache<>(8, 16);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private static final JsonParser jp = new JsonParser();

    protected void assertPlan(String expected, String query) throws ParserException, JournalException {
        TestUtils.assertEquals(expected, compiler.plan(factory, query));
    }

    protected void assertPlan2(CharSequence expected, CharSequence query) throws JournalException, ParserException {
        sink.clear();
        compiler.compileSource(factory, query).toSink(sink);
        String s = gson.toJson(jp.parse(sink.toString()));
        TestUtils.assertEquals(expected, s);
    }

    protected void assertThat(String expected, String query, boolean header) throws ParserException, JournalException, IOException {
        sink.clear();
        RecordSource rs = cache.peek(query);
        if (rs == null) {
            cache.put(query, rs = compiler.compileSource(factory, query));
        } else {
            rs.reset();
        }
        printer.printCursor(rs.prepareCursor(factory, NoOpCancellationHandler.INSTANCE), header);
        TestUtils.assertEquals(expected, sink);
    }

    protected void assertThat(String expected, String query) throws JournalException, ParserException, IOException {
        assertThat(expected, query, false);
        assertThat(expected, query, false);
        Misc.free(cache.poll(query));
    }
}
