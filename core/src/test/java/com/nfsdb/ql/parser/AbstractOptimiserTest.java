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

package com.nfsdb.ql.parser;

import com.nfsdb.ex.JournalException;
import com.nfsdb.ex.ParserException;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Misc;
import com.nfsdb.model.configuration.ModelConfiguration;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.std.AssociativeCache;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.test.tools.TestUtils;
import org.junit.ClassRule;

import java.io.IOException;

public abstract class AbstractOptimiserTest {

    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));
    protected static final QueryCompiler compiler = new QueryCompiler();
    protected static final StringSink sink = new StringSink();
    protected static final RecordSourcePrinter printer = new RecordSourcePrinter(sink);
    private static final AssociativeCache<RecordSource> cache = new AssociativeCache<>(8, 16);

    protected void assertPlan(String expected, String query) throws ParserException, JournalException {
        TestUtils.assertEquals(expected, compiler.plan(factory, query));
    }

    protected void assertThat(String expected, String query, boolean header) throws ParserException, JournalException, IOException {
        sink.clear();
        RecordSource rs = cache.peek(query);
        if (rs == null) {
            cache.put(query, rs = compiler.compileSource(factory, query));
        } else {
            rs.reset();
        }
        printer.printCursor(rs.prepareCursor(factory), header);
        TestUtils.assertEquals(expected, sink);
    }

    protected void assertThat(String expected, String query) throws JournalException, ParserException, IOException {
        assertThat(expected, query, false);
        assertThat(expected, query, false);
        Misc.free(cache.poll(query));
    }
}
