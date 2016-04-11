/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
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
