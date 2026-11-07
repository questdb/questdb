/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
 *
 ******************************************************************************/

package io.questdb.test.griffin.engine.functions.bool;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.functions.bool.InSymbolCursorFunctionFactory;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class InSymbolCursorFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testCursorCloneOwnsImmutableState() throws Exception {
        assertMemoryLeak(() -> {
            TestCursorFactory donorRhs = new TestCursorFactory(ColumnType.SYMBOL);
            TestCursorFactory cloneRhs = new TestCursorFactory(ColumnType.SYMBOL);
            donorRhs.values.add(null);
            donorRhs.values.add("old");
            try (Function clone = newInFunction(cloneRhs)) {
                try (Function donor = newInFunction(donorRhs)) {
                    donor.init(null, sqlExecutionContext);
                    donor.offerStateTo(clone);
                    clone.init(null, sqlExecutionContext);
                    Assert.assertEquals(0, cloneRhs.openCount);
                    Assert.assertEquals(1, cloneRhs.initCount);
                    Assert.assertNotSame(valueSet(donor), valueSet(clone));
                    Assert.assertNotSame(spareValueSet(donor), spareValueSet(clone));
                    Assert.assertSame(key(valueSet(donor), "old"), key(valueSet(clone), "old"));
                    donorRhs.values.clear();
                    donorRhs.values.add("new");
                    donor.init(null, sqlExecutionContext);
                    Assert.assertTrue(matches(clone, "old"));
                    Assert.assertTrue(matches(clone, null));
                    Assert.assertFalse(matches(clone, "new"));
                    donor.offerStateTo(clone);
                    clone.init(null, sqlExecutionContext);
                    Assert.assertEquals(0, cloneRhs.openCount);
                    Assert.assertEquals(2, cloneRhs.initCount);
                    Assert.assertFalse(matches(clone, "old"));
                    Assert.assertFalse(matches(clone, null));
                }
                Assert.assertTrue(matches(clone, "new"));
            }
        });
    }

    @Test
    public void testCursorInitFailureClearsPreviousAndRetries() throws Exception {
        assertMemoryLeak(() -> {
            for (int failure = 1; failure <= 4; failure++) {
                TestCursorFactory rhs = new TestCursorFactory(ColumnType.STRING);
                rhs.values.add("old");
                try (Function function = newInFunction(rhs)) {
                    function.init(null, sqlExecutionContext);
                    rhs.values.clear();
                    rhs.values.add("partial");
                    rhs.values.add("other");
                    rhs.failure = failure;
                    try {
                        function.init(null, sqlExecutionContext);
                        Assert.fail("expected failure " + failure);
                    } catch (IllegalStateException e) {
                        Assert.assertEquals("failure " + failure, e.getMessage());
                    }
                    Assert.assertEquals(failure <= 2 ? 1 : 2, rhs.closeCount);
                    Assert.assertEquals(0, spareValueSet(function).size());
                    rhs.failure = 0;
                    rhs.values.clear();
                    rhs.values.add("retry");
                    function.init(null, sqlExecutionContext);
                    Assert.assertTrue(matches(function, "retry"));
                    Assert.assertFalse(matches(function, "old"));
                    Assert.assertFalse(matches(function, "partial"));
                    Assert.assertEquals(0, spareValueSet(function).size());
                }
            }
        });
    }

    @Test
    public void testCursorReusesImmutableValues() throws Exception {
        assertMemoryLeak(() -> {
            for (int type : new int[]{ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR}) {
                TestCursorFactory rhs = new TestCursorFactory(type);
                rhs.values.add(null);
                rhs.values.add("");
                rhs.values.add("alpha");
                rhs.values.add("é🌍");
                rhs.values.add("Aa");
                rhs.values.add("BB");
                rhs.values.add("alpha");
                try (Function function = newInFunction(rhs)) {
                    function.init(null, sqlExecutionContext);
                    CharSequenceHashSet first = new CharSequenceHashSet(valueSet(function));
                    int copies = rhs.flyweight.copyCount;
                    if (type != ColumnType.VARCHAR) {
                        Assert.assertEquals(5, copies);
                    }
                    rhs.flyweight.isCopyForbidden = true;
                    rhs.values.setQuick(2, "BB");
                    rhs.values.setQuick(5, "alpha");
                    for (int i = 0; i < 3; i++) {
                        function.init(null, sqlExecutionContext);
                        Assert.assertEquals(6, valueSet(function).size());
                        Assert.assertTrue(matches(function, null));
                        for (int j = 1; j < first.size(); j++) {
                            CharSequence value = first.get(j);
                            Assert.assertTrue(value instanceof String);
                            Assert.assertSame(value, key(valueSet(function), value));
                            Assert.assertTrue(matches(function, value));
                        }
                        Assert.assertEquals(copies, rhs.flyweight.copyCount);
                        Assert.assertEquals(0, spareValueSet(function).size());
                    }
                    rhs.flyweight.isCopyForbidden = false;
                    rhs.values.clear();
                    rhs.values.add("alpha");
                    rhs.values.add("new");
                    function.init(null, sqlExecutionContext);
                    Assert.assertSame(key(first, "alpha"), key(valueSet(function), "alpha"));
                    Assert.assertEquals(2, valueSet(function).size());
                    Assert.assertFalse(matches(function, null));
                    Assert.assertFalse(matches(function, "BB"));
                    Assert.assertTrue(matches(function, "new"));
                    if (type != ColumnType.VARCHAR) {
                        Assert.assertEquals(copies + 1, rhs.flyweight.copyCount);
                    }
                    rhs.values.clear();
                    function.init(null, sqlExecutionContext);
                    Assert.assertEquals(0, valueSet(function).size());
                    Assert.assertFalse(matches(function, "alpha"));
                    rhs.values.add("alpha");
                    function.init(null, sqlExecutionContext);
                    Assert.assertTrue(matches(function, "alpha"));
                    Assert.assertFalse(matches(function, "new"));
                    Assert.assertNotSame(key(first, "alpha"), key(valueSet(function), "alpha"));
                }
            }
        });
    }

    @Test
    public void testCursorRetainsOnlyCurrentStringsAfterLongValueRotation() throws Exception {
        assertMemoryLeak(() -> {
            TestCursorFactory rhs = new TestCursorFactory(ColumnType.SYMBOL);
            String longValue = "x".repeat(65_536);
            try (Function function = newInFunction(rhs)) {
                CharSequence retained = null;
                for (int position = 0; position < 16; position++) {
                    rhs.values.clear();
                    for (int i = 0; i < 16; i++) {
                        rhs.values.add(i == position ? longValue : "s" + i);
                    }
                    function.init(null, sqlExecutionContext);
                    CharSequence current = key(valueSet(function), longValue);
                    if (retained != null) {
                        Assert.assertSame(retained, current);
                    }
                    retained = current;
                    Assert.assertEquals(16, valueSet(function).size());
                    Assert.assertEquals(0, spareValueSet(function).size());
                    for (int i = 0; i < 16; i++) {
                        Assert.assertTrue(matches(function, rhs.values.getQuick(i)));
                    }
                }
                rhs.values.clear();
                rhs.values.add("small");
                function.init(null, sqlExecutionContext);
                Assert.assertEquals(1, valueSet(function).size());
                Assert.assertEquals("small", valueSet(function).get(0));
                Assert.assertEquals(0, spareValueSet(function).size());
                Assert.assertFalse(matches(function, longValue));
            }
        });
    }

    @Test
    public void testNullBeforeCursorSetGrowth() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE lhs (v STRING)");
            execute("INSERT INTO lhs VALUES (NULL), ('s1'), ('s16'), (''), ('é🌍'), ('absent')");
            execute("CREATE TABLE rhs (s SYMBOL CACHE)");
            execute("INSERT INTO rhs VALUES (NULL)");
            execute("INSERT INTO rhs SELECT 's' || x::STRING FROM long_sequence(16)");
            execute("INSERT INTO rhs VALUES (''), ('é🌍'), ('s1'), (NULL)");
            for (int type : new int[]{ColumnType.SYMBOL, ColumnType.STRING, ColumnType.VARCHAR}) {
                String projection = "s::" + ColumnType.nameOf(type);
                assertQuery("SELECT v::SYMBOL IN (SELECT " + projection + " FROM rhs) AS matched FROM lhs")
                        .noLeakCheck()
                        .expectSize()
                        .returns("matched\ntrue\ntrue\ntrue\ntrue\ntrue\nfalse\n");
            }
            assertQuery("SELECT v::SYMBOL IN (SELECT s FROM rhs GROUP BY s) AS matched FROM lhs")
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("GroupBy vectorized: true")
                    .returns("matched\ntrue\ntrue\ntrue\ntrue\ntrue\nfalse\n");
        });
    }

    @Test
    public void testMalformedVarcharInSymbolCursorMatchesNullNotPrefix() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v VARCHAR)");
            execute("CREATE TABLE y (str STRING, sym SYMBOL)");
            execute("""
                    INSERT INTO y VALUES
                        ('abc', 'abc'),
                        (NULL, NULL)
                    """);
            try (TableWriter writer = getWriter("x")) {
                TableWriter.Row row = writer.newRow();
                row.putVarchar(0, new Utf8String(new byte[]{'a', 'b', 'c', (byte) 0xC3}, false));
                row.append();
                writer.commit();
            }

            assertQuery("SELECT cast(str AS symbol) IN (SELECT v FROM x) matched FROM y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            matched
                            false
                            true
                            """);
            assertQuery("SELECT sym IN (SELECT v FROM x) matched FROM y")
                    .noLeakCheck()
                    .expectSize()
                    .returns("""
                            matched
                            false
                            true
                            """);
        });
    }

    @Test
    public void testNullInCursorInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE NULL IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .returns("x\n");
    }

    @Test
    public void testNullInCursorInSelect() throws Exception {
        assertQuery("SELECT NULL IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testNullInNullCursorInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE NULL IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .returns("x\n");
    }

    @Test
    public void testNullInNullCursorInSelect() throws Exception {
        assertQuery("SELECT NULL IN (SELECT NULL);")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testNullInNullInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE NULL IN NULL;")
                .ddl(null)
                .expectSize()
                .returns("""
                        x
                        1
                        2
                        3
                        """);
    }

    @Test
    public void testNullInNullInSelect() throws Exception {
        assertQuery("SELECT NULL IN NULL;")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testSymbolColumnInCursorInFilter() throws Exception {
        assertQuery("select * from x where b in (select 'RXGZ'::symbol from long_sequence(3))")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .timestamp("k")
                .returns("""
                        a\tb\tk
                        23.90529010846525\tRXGZ\t1970-01-03T07:33:20.000000Z
                        """);
    }

    @Test
    public void testSymbolColumnInCursorInSelect() throws Exception {
        assertQuery("select b in (select 'RXGZ'::symbol from long_sequence(3)), b from x")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .expectSize()
                .returns("""
                        column\tb
                        false\t
                        false\tVTJW
                        true\tRXGZ
                        false\tPEHN
                        false\t
                        """);
    }

    @Test
    public void testSymbolColumnInNullInFilter() throws Exception {
        assertQuery("select * from x where b in (select null from long_sequence(10))")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .timestamp("k")
                .returns("""
                        a\tb\tk
                        11.427984775756228\t\t1970-01-01T00:00:00.000000Z
                        87.99634725391621\t\t1970-01-05T15:06:40.000000Z
                        """);
    }

    @Test
    public void testSymbolColumnInNullInSelect() throws Exception {
        assertQuery("select b in (select null from long_sequence(10)), b from x")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .expectSize()
                .returns("""
                        column\tb
                        true\t
                        false\tVTJW
                        false\tRXGZ
                        false\tPEHN
                        true\t
                        """);
    }

    @Test
    public void testSymbolExprInNullInFilter() throws Exception {
        assertQuery("select * from x where substring(b, 1, 2)::symbol in (select null from long_sequence(10))")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .timestamp("k")
                .returns("""
                        a\tb\tk
                        11.427984775756228\t\t1970-01-01T00:00:00.000000Z
                        87.99634725391621\t\t1970-01-05T15:06:40.000000Z
                        """);
    }

    @Test
    public void testSymbolExprInNullInSelect() throws Exception {
        assertQuery("select substring(b, 1, 2)::symbol in (select null from long_sequence(10)), b from x")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(5)" +
                        ") timestamp(k) partition by DAY")
                .expectSize()
                .returns("""
                        column\tb
                        true\t
                        false\tVTJW
                        false\tRXGZ
                        false\tPEHN
                        true\t
                        """);
    }

    @Test
    public void testSymbolInCursorInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE 'abc'::symbol IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .returns("""
                        x
                        1
                        2
                        3
                        """);
    }

    @Test
    public void testSymbolInCursorInSelect() throws Exception {
        assertQuery("SELECT 'abc'::symbol IN (SELECT 'abc'::symbol);")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        true
                        """);
    }

    @Test
    public void testSymbolInNullCursorInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN (SELECT NULL);")
                .ddl(null)
                .returns("x\n");
    }

    @Test
    public void testSymbolInNullCursorInSelect() throws Exception {
        assertQuery("SELECT 'foobar'::symbol IN (SELECT NULL);")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testSymbolInNullInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN NULL;")
                .ddl(null)
                .returns("x\n");
    }

    @Test
    public void testSymbolInNullInSelect() throws Exception {
        assertQuery("SELECT 'foobar'::symbol IN NULL;")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testSymbolInSymbolInFilter() throws Exception {
        assertQuery("SELECT x FROM long_sequence(3) WHERE 'foobar'::symbol IN 'foobar'::symbol;")
                .ddl(null)
                .expectSize()
                .returns("""
                        x
                        1
                        2
                        3
                        """);
    }

    @Test
    public void testSymbolInSymbolInSelect() throws Exception {
        assertQuery("SELECT 'foo'::symbol IN 'bar'::symbol;")
                .ddl(null)
                .expectSize()
                .returns("""
                        column
                        false
                        """);
    }

    @Test
    public void testUnsupportedColumnType() throws Exception {
        assertQuery("select * from x where b in (select 12, rnd_str('RXGZ', 'HYRX', null) a from long_sequence(10))")
                .ddl("create table x as (" +
                        "select" +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        ") timestamp(k) partition by DAY")
                .fails(24, "supported column types are VARCHAR, SYMBOL and STRING, found: INT");
    }

    private static CharSequence key(CharSequenceHashSet set, CharSequence value) {
        int index = set.keyIndex(value);
        Assert.assertTrue(index < 0);
        return set.keyAt(index);
    }

    private static boolean matches(Function function, CharSequence value) {
        return function.getBool(new Record() {
            @Override
            public CharSequence getSymA(int col) {
                return value;
            }
        });
    }

    private static Function newInFunction(TestCursorFactory rhs) throws SqlException {
        ObjList<Function> args = new ObjList<>();
        args.add(new SymbolFunction() {
            @Override
            public int getInt(Record rec) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence getSymbol(Record rec) {
                return rec.getSymA(0);
            }

            @Override
            public CharSequence getSymbolB(Record rec) {
                return getSymbol(rec);
            }

            @Override
            public boolean isSymbolTableStatic() {
                return false;
            }

            @Override
            public CharSequence valueBOf(int key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CharSequence valueOf(int key) {
                throw new UnsupportedOperationException();
            }
        });
        args.add(new CursorFunction(rhs) {
            @Override
            public RecordCursorFactory getRecordCursorFactory() {
                rhs.fail(1);
                return super.getRecordCursorFactory();
            }

            @Override
            public void init(SymbolTableSource source, SqlExecutionContext context) {
                rhs.initCount++;
            }
        });
        return new InSymbolCursorFunctionFactory().newInstance(0, args, new IntList(), configuration, sqlExecutionContext);
    }

    private static CharSequenceHashSet spareValueSet(Function function) {
        return ((InSymbolCursorFunctionFactory.StrInCursorFunc) function).getSpareValueSet();
    }

    private static CharSequenceHashSet valueSet(Function function) {
        return ((InSymbolCursorFunctionFactory.StrInCursorFunc) function).getValueSet();
    }

    private static class CopyCountingSequence implements CharSequence {
        private int copyCount;
        private boolean isCopyForbidden;
        private String value = "";

        @Override
        public char charAt(int index) {
            return value.charAt(index);
        }

        @Override
        public int length() {
            return value.length();
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            Assert.assertFalse("must reuse the previous immutable value", isCopyForbidden);
            copyCount++;
            return new String(value.toCharArray());
        }
    }

    private static class TestCursorFactory extends AbstractRecordCursorFactory {
        private final CopyCountingSequence flyweight = new CopyCountingSequence();
        private final Utf8StringSink utf8 = new Utf8StringSink();
        private final ObjList<String> values = new ObjList<>();
        private int closeCount;
        private int failure;
        private int initCount;
        private int openCount;

        private TestCursorFactory(int type) {
            super(metadata(type));
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            openCount++;
            fail(2);
            return new RecordCursor() {
                private final Record record = new Record() {
                    @Override
                    public CharSequence getStrA(int col) {
                        return flyweight.value == null ? null : flyweight;
                    }

                    @Override
                    public CharSequence getSymA(int col) {
                        return getStrA(col);
                    }

                    @Override
                    public Utf8Sequence getVarcharA(int col) {
                        return flyweight.value == null ? null : utf8;
                    }
                };
                private int row;

                @Override
                public void close() {
                    closeCount++;
                    flyweight.value = "closed";
                    utf8.clear();
                    fail(4);
                }

                @Override
                public Record getRecord() {
                    return record;
                }

                @Override
                public Record getRecordB() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean hasNext() {
                    if (row == 1) {
                        fail(3);
                    }
                    if (row == values.size()) {
                        return false;
                    }
                    flyweight.value = values.getQuick(row++);
                    utf8.clear();
                    if (flyweight.value != null) {
                        utf8.put(flyweight.value);
                    }
                    return true;
                }

                @Override
                public long preComputedStateSize() {
                    return 0;
                }

                @Override
                public void recordAt(Record record, long atRowId) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public long size() {
                    return values.size();
                }

                @Override
                public void toTop() {
                    row = 0;
                }
            };
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return false;
        }

        private static GenericRecordMetadata metadata(int type) {
            GenericRecordMetadata metadata = new GenericRecordMetadata();
            metadata.add(new TableColumnMetadata("s", type, IndexType.NONE, 0, false, null));
            return metadata;
        }

        private void fail(int at) {
            if (failure == at) {
                throw new IllegalStateException("failure " + at);
            }
        }
    }
}
