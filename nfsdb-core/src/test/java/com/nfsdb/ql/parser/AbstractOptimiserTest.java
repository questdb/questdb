/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.ql.parser;

import com.nfsdb.exceptions.JournalException;
import com.nfsdb.exceptions.ParserException;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.model.configuration.ModelConfiguration;
import com.nfsdb.test.tools.JournalTestFactory;
import com.nfsdb.test.tools.TestUtils;
import com.nfsdb.utils.Files;
import org.junit.ClassRule;

public abstract class AbstractOptimiserTest {

    @ClassRule
    public static final JournalTestFactory factory = new JournalTestFactory(ModelConfiguration.MAIN.build(Files.makeTempDir()));
    protected static final Compiler compiler = new Compiler(factory);
    private static final StringSink sink = new StringSink();
    private static final RecordSourcePrinter printer = new RecordSourcePrinter(sink);

    protected void assertThat(String expected, String query) throws JournalException, ParserException {
        assertThat(expected, query, false);
        assertThat(expected, query, false);
    }

    protected void assertThat(String expected, String query, boolean header) throws ParserException, JournalException {
        sink.clear();
        printer.printCursor(compiler.compile(query), header);
        TestUtils.assertEquals(expected, sink);
    }
}
