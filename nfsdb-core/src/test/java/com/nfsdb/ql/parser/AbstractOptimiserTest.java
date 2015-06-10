/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ql.parser;

import com.nfsdb.collections.ObjHashSet;
import com.nfsdb.exceptions.JournalException;
import com.nfsdb.factory.JournalCachingFactory;
import com.nfsdb.io.RecordSourcePrinter;
import com.nfsdb.io.sink.StringSink;
import com.nfsdb.ql.Compiler;
import com.nfsdb.ql.Record;
import com.nfsdb.ql.RecordSource;
import com.nfsdb.test.tools.AbstractTest;
import com.nfsdb.utils.Rnd;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

public abstract class AbstractOptimiserTest extends AbstractTest {
    private final Compiler compiler = new Compiler();
    private final StringSink sink = new StringSink();
    private final RecordSourcePrinter printer = new RecordSourcePrinter(sink);
    private JournalCachingFactory f;

    @Before
    public void setUp() {
        this.f = new JournalCachingFactory(factory.getConfiguration());
    }

    @After
    public void tearDown() {
        this.f.close();
    }

    protected void assertThat(String expected, String query) throws JournalException, ParserException {
        RecordSource<? extends Record> rs = compile(query);

        sink.clear();
        printer.print(rs.prepareCursor(f), rs.getMetadata());
        Assert.assertEquals(expected, sink.toString());

        rs.reset();
        sink.clear();
        printer.print(rs.prepareCursor(f), rs.getMetadata());
        Assert.assertEquals(expected, sink.toString());
    }

    protected RecordSource<? extends Record> compile(CharSequence query) throws ParserException, JournalException {
        return compiler.compile(query, f);
    }

    protected ObjHashSet<String> getNames(Rnd r, int n) {
        ObjHashSet<String> names = new ObjHashSet<>();
        for (int i = 0; i < n; i++) {
            names.add(r.nextString(15));
        }
        return names;
    }

}
