/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/

package com.questdb.test.tools;

import com.questdb.ex.JournalException;
import com.questdb.ex.ParserException;
import com.questdb.io.RecordSourcePrinter;
import com.questdb.io.sink.StringSink;
import com.questdb.misc.Files;
import com.questdb.model.configuration.ModelConfiguration;
import com.questdb.net.http.ServerConfiguration;
import com.questdb.ql.RecordSource;
import com.questdb.ql.parser.QueryCompiler;
import org.junit.Assert;
import org.junit.Rule;

import java.io.IOException;

public abstract class AbstractTest {
    @Rule
    public final JournalTestFactory factory = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));

    protected final StringSink sink = new StringSink();
    protected final QueryCompiler compiler = new QueryCompiler(new ServerConfiguration());
    protected final RecordSourcePrinter printer = new RecordSourcePrinter(sink);

    protected void assertEmpty(String query) throws ParserException, JournalException {
        try (RecordSource src = compiler.compileSource(factory, query)) {
            Assert.assertFalse(src.prepareCursor(factory).hasNext());
        }
    }

    protected void assertThat(String expected, String query) throws JournalException, ParserException, IOException {
        assertThat(expected, query, false);
        assertThat(expected, query, false);
    }

    protected void assertThat(String expected, String query, boolean header) throws ParserException, JournalException, IOException {
        sink.clear();
        try (RecordSource src = compiler.compileSource(factory, query)) {
            printer.print(src, factory, header);
            TestUtils.assertEquals(expected, sink);
            src.reset();
            TestUtils.assertStrings(src, factory);
        }
    }
}
