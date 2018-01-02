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

package com.questdb.ql;

import com.questdb.ex.ParserException;
import com.questdb.parser.sql.QueryCompiler;
import com.questdb.parser.sql.QueryError;
import com.questdb.std.ex.JournalException;
import com.questdb.std.str.StringSink;
import com.questdb.store.JournalEntryWriter;
import com.questdb.store.JournalKey;
import com.questdb.store.JournalWriter;
import com.questdb.store.factory.Factory;
import com.questdb.test.tools.AbstractTest;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class RenameJournalTest extends AbstractTest {
    private static final QueryCompiler compiler = new QueryCompiler();

    private final StringSink sink = new StringSink();
    private final RecordSourcePrinter printer = new RecordSourcePrinter(sink);

    @Before
    public void setUp() {
        sink.clear();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testJournalAlreadyOpenButIdle() throws Exception {
        createX();

        Factory factory = getFactory();
        assertJournal(factory, "x");
        sink.clear();

        compiler.execute(factory, "rename table x to y");
        assertJournal(factory, "y");

        // make sure caching readerFactory doesn't return old journal
        try {
            factory.reader(new JournalKey("x"));
            Assert.fail();
        } catch (JournalException e) {
            Assert.assertEquals("Journal does not exist", e.getMessage());
        }

        // make sure compile doesn't pick up old journal
        try {
            compiler.compile(factory, "x");
            Assert.fail("still exists");
        } catch (ParserException e) {
            Assert.assertEquals(0, QueryError.getPosition());
            TestUtils.assertEquals("Journal does not exist", QueryError.getMessage());
        }

        sink.clear();
        createX();
        assertJournal(factory, "x");
    }

    @Test
    public void testNonLiteralFrom() throws Exception {
        try {
            compiler.execute(getFactory(), "rename table 1+2 to 'c d'");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(14, QueryError.getPosition());
        }
    }

    @Test
    public void testNonLiteralTo() throws Exception {
        try {
            compiler.execute(getFactory(), "rename table x to 5+5");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(19, QueryError.getPosition());
        }
    }

    @Test
    public void testReleaseOfJournalInPool() throws Exception {
        createX();

        Factory f = getFactory();

        assertJournal(f, "x");

        sink.clear();

        compiler.execute(f, "rename table x to y");
        assertJournal(f, "y");

        sink.clear();
        createX();
        assertJournal(f, "x");
    }

    @Test
    public void testRenameQuoted() throws Exception {
        create("'a b'");
        compiler.execute(getFactory(), "rename table 'a b' to 'c d'");
    }

    @Test
    public void testSimpleNonExisting() throws Exception {
        try {
            compiler.execute(getFactory(), "rename table x to y");
            Assert.fail();
        } catch (ParserException e) {
            Assert.assertEquals(13, QueryError.getPosition());
            TestUtils.assertEquals("Journal does not exist", QueryError.getMessage());
        }
    }

    @Test
    public void testSimpleRename() throws Exception {
        createX();
        compiler.execute(getFactory(), "rename table x to y");
        assertJournal(getFactory(), "y");
    }

    private void assertJournal(Factory f, String dest) throws IOException, ParserException {
        try (RecordSource rs = compiler.compile(f, dest)) {
            printer.print(rs, f);
            TestUtils.assertEquals("999\n", sink);
        }
    }

    private void create(String name) throws JournalException, ParserException {
        try (JournalWriter w = compiler.createWriter(getFactory(), "create table " + name + "(a int) record hint 100")) {
            JournalEntryWriter ew = w.entryWriter();
            ew.putInt(0, 999);
            ew.append();
            w.commit();
        }
    }

    private void createX() throws JournalException, ParserException {
        create("x");
    }
}
