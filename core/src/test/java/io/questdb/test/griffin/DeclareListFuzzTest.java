package io.questdb.test.griffin;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.QueryAssertion;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * The property the whole design rests on: a declared list spliced into IN is indistinguishable from
 * the same list written out in full. Everything else - which IN overload applies, whether an
 * element keeps its own type, whether the members stay in order - follows from that, so it is worth
 * asserting over shapes nobody chose by hand rather than only the ones that occurred to me.
 * <p>
 * The splice itself never looks at a member's type; the types matter downstream, where the IN node
 * the splice builds picks an overload, a JIT path or an intrinsic scan. So the fuzz runs every shape
 * against one column per such route, see {@link #COLUMNS}.
 */
public class DeclareListFuzzTest extends AbstractCairoTest {

    /**
     * One column per route an IN node takes, each with the members a list for it is made of:
     * <ul>
     *     <li>every IN overload: SYMBOL, LONG (which also serves INT, SHORT and BYTE, where the JIT
     *     narrows the key and widens a constant that does not fit), DOUBLE and FLOAT, VARCHAR, STRING,
     *     CHAR, TIMESTAMP (which also serves TIMESTAMP_NS and DATE, and reads a lone string member as
     *     an interval but a string among several as a point), UUID and IPv4;</li>
     *     <li>an indexed SYMBOL and the designated TIMESTAMP, which the WHERE clause analysis turns into
     *     index and interval scans by reading the IN node's shape;</li>
     *     <li>BOOLEAN and DECIMAL, which no IN overload accepts, so the two spellings have to fail
     *     alike.</li>
     * </ul>
     * Members are literals, casts, negative numbers and calls whose own commas sit inside brackets
     * ({@code to_timestamp}, {@code concat}, an array subscript). {@link #member} also wraps one in
     * grouping brackets now and then, which is what the list parser's nested-list check has to tell
     * apart from a nested list.
     */
    private static final ObjList<FuzzColumn> COLUMNS = new ObjList<>();
    private static final String DDL = """
            CREATE TABLE %s (
                s SYMBOL, si SYMBOL INDEX, l LONG, i INT, sh SHORT, b BYTE, d DOUBLE, f FLOAT, v VARCHAR,
                st STRING, c CHAR, tm TIMESTAMP, tn TIMESTAMP_NS, dt DATE, u UUID, ip IPV4, bo BOOLEAN,
                dc DECIMAL(10, 2), ts TIMESTAMP
            ) TIMESTAMP(ts) PARTITION BY DAY""";
    // Every other row sits at noon. The Java filter of the TIMESTAMP overload reads a lone day string
    // as the whole day, so it matches a row that the same string among several members, read as
    // midnight, does not. The JIT filter reads a lone day string as midnight, a known divergence from
    // the Java filter that predates this test. Both spellings of a list run on the same filter, so
    // the fuzz compares them under either reading and verifies neither. The NULL row is what a NULL
    // member matches, see member(); SHORT, BYTE and BOOLEAN have no NULL and keep a value, as does
    // the designated timestamp.
    private static final String ROWS = """
            INSERT INTO %s VALUES
                ('AAPL', 'AAPL', 1, 1, 1, 1, 1.5, 1.5, 'AAPL', 'AAPL', 'a', '2024-01-01T00:00:00.000000Z',
                 '2024-01-01T00:00:00.000000000Z', '2024-01-01T00:00:00.000Z', '11111111-1111-1111-1111-111111111111',
                 '1.1.1.1', false, 1.5m, '2024-01-01T00:00:00.000000Z'),
                ('MSFT', 'MSFT', 2, 2, 2, 2, 2.5, 2.5, 'MSFT', 'MSFT', 'b', '2024-01-02T12:00:00.000000Z',
                 '2024-01-02T12:00:00.000000000Z', '2024-01-02T12:00:00.000Z', '22222222-2222-2222-2222-222222222222',
                 '2.2.2.2', true, 2.5m, '2024-01-02T12:00:00.000000Z'),
                ('TSLA', 'TSLA', 3, 3, 3, 3, 3.5, 3.5, 'TSLA', 'TSLA', 'c', '2024-01-03T00:00:00.000000Z',
                 '2024-01-03T00:00:00.000000000Z', '2024-01-03T00:00:00.000Z', '33333333-3333-3333-3333-333333333333',
                 '3.3.3.3', false, 3.5m, '2024-01-03T00:00:00.000000Z'),
                ('AMZN', 'AMZN', 4, 4, 4, 4, 4.5, 4.5, 'AMZN', 'AMZN', 'd', '2024-01-04T12:00:00.000000Z',
                 '2024-01-04T12:00:00.000000000Z', '2024-01-04T12:00:00.000Z', '44444444-4444-4444-4444-444444444444',
                 '4.4.4.4', true, 4.5m, '2024-01-04T12:00:00.000000Z'),
                ('GOOG', 'GOOG', 5, 5, 5, 5, 5.5, 5.5, 'GOOG', 'GOOG', 'e', '2024-01-05T00:00:00.000000Z',
                 '2024-01-05T00:00:00.000000000Z', '2024-01-05T00:00:00.000Z', '55555555-5555-5555-5555-555555555555',
                 '5.5.5.5', false, 5.5m, '2024-01-05T00:00:00.000000Z'),
                (NULL, NULL, NULL, NULL, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL,
                 NULL, NULL, NULL, NULL, false, NULL, '2024-01-06T00:00:00.000000Z')""";
    // Lists both spellings refused, counted by assertSplicedListMatches().
    private int rejectedLists;

    static {
        final ObjList<String> symbols = new ObjList<>("'AAPL'", "'MSFT'", "'TSLA'", "'AMZN'", "'GOOG'", "'NFLX'");
        final ObjList<String> strings = new ObjList<>(
                "'AAPL'", "'MSFT'", "'TSLA'", "'AMZN'", "'GOOG'", "'NFLX'", "'a'", "concat('MS', 'FT')"
        );
        final ObjList<String> timestamps = new ObjList<>(
                "'2024-01-01T00:00:00.000000Z'", "'2024-01-02T12:00:00.000000Z'", "'2024-01-03T00:00:00.000000Z'",
                "'2024-01-02'", "'2024-01-04'", "'2024-01-09'", "to_timestamp('2024-01-03', 'yyyy-MM-dd')",
                "1_704_067_200_000_000"
        );
        addColumn("s", symbols);
        addColumn("si", symbols);
        addColumn("l", new ObjList<>("1", "2", "3", "4", "5", "-1", "5_000_000_000"));
        addColumn("i", new ObjList<>("1", "2", "3", "4", "5", "-1", "5_000_000_000", "1 + 1"));
        addColumn("sh", new ObjList<>("1", "2", "3", "4", "5", "-1", "40_000"));
        addColumn("b", new ObjList<>("1", "2", "3", "4", "5", "-1", "300"));
        addColumn("d", new ObjList<>("1.5", "2.5", "3.5", "4.5", "5.5", "2", "-1.5", "ARRAY[1.5, 2.5][2]"));
        addColumn("f", new ObjList<>("1.5", "2.5", "3.5", "4.5", "5.5", "2"));
        addColumn("v", strings);
        addColumn("st", strings);
        addColumn("c", new ObjList<>("'a'", "'b'", "'c'", "'d'", "'e'", "'z'"));
        addColumn("tm", timestamps);
        addColumn(
                "tn",
                new ObjList<>(
                        "'2024-01-01T00:00:00.000000000Z'", "'2024-01-02T12:00:00.000000000Z'", "'2024-01-03T00:00:00.000000Z'",
                        "'2024-01-02'", "'2024-01-04'"
                )
        );
        addColumn(
                "dt",
                new ObjList<>(
                        "'2024-01-01T00:00:00.000Z'", "'2024-01-02T12:00:00.000Z'", "'2024-01-02'", "'2024-01-04'",
                        "to_date('2024-01-03', 'yyyy-MM-dd')"
                )
        );
        addColumn(
                "u",
                new ObjList<>(
                        "'11111111-1111-1111-1111-111111111111'", "'22222222-2222-2222-2222-222222222222'",
                        "'33333333-3333-3333-3333-333333333333'::uuid", "'99999999-9999-9999-9999-999999999999'"
                )
        );
        addColumn("ip", new ObjList<>("'1.1.1.1'", "'2.2.2.2'", "'3.3.3.3'::ipv4", "'9.9.9.9'"));
        addColumn("bo", new ObjList<>("true", "false"));
        addColumn("dc", new ObjList<>("1.5", "2.5m"));
        addColumn("ts", timestamps);
    }

    @Test
    public void testSplicedListMatchesWrittenOutList() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t");

            final Rnd rnd = TestUtils.generateRandom(LOG);
            int notInForms = 0;
            int listsWithNull = 0;
            int bracketedMembers = 0;
            for (int c = 0, n = COLUMNS.size(); c < n; c++) {
                final FuzzColumn column = COLUMNS.getQuick(c);

                // Every member alone, NULL included. A one-member list splices into the binary IN
                // shape, which the routes treat apart: the TIMESTAMP overload reads a lone string as
                // an interval rather than a point, the JIT compiles a lone NULL into its own null
                // check, and it widens a lone constant too wide for a narrow key. A member that carries
                // its own commas goes in brackets, the one form the list parser's nested-list check has
                // to see through.
                final ObjList<String> values = column.members();
                for (int m = -1, k = values.size(); m < k; m++) {
                    final String value = m < 0 ? "NULL" : values.getQuick(m);
                    final String member = value.indexOf(',') > -1 || rnd.nextInt(5) == 0 ? '(' + value + ')' : value;
                    final boolean isNotIn = rnd.nextBoolean();
                    if (isNotIn) {
                        notInForms++;
                    }
                    // The trailing comma is the only way to write a list of one; without it the
                    // brackets are grouping.
                    assertSplicedListMatches(column.name(), member + ',', "", "", isNotIn, rnd.nextBoolean());
                }

                // Every list length meets every placement of the list among literals, with the
                // members, NOT IN and the brackets around a lone variable chosen at random.
                for (int members = 1; members <= 4; members++) {
                    // 0 = list alone, 1 = literal before it, 2 = literal after it, 3 = both
                    for (int shape = 0; shape < 4; shape++) {
                        final StringSink list = new StringSink();
                        for (int m = 0; m < members; m++) {
                            if (m > 0) {
                                list.put(',');
                            }
                            final String member = member(rnd, column);
                            if (member.charAt(0) == '(') {
                                bracketedMembers++;
                            }
                            list.put(member);
                        }
                        if (members == 1) {
                            list.put(',');
                        }
                        if (list.toString().contains("NULL")) {
                            listsWithNull++;
                        }
                        final String before = shape == 1 || shape == 3 ? member(rnd, column) + ", " : "";
                        final String after = shape == 2 || shape == 3 ? ", " + member(rnd, column) : "";
                        final boolean isNotIn = rnd.nextBoolean();
                        if (isNotIn) {
                            notInForms++;
                        }
                        assertSplicedListMatches(column.name(), list.toString(), before, after, isNotIn, rnd.nextBoolean());
                    }
                }
            }
            // A fuzz run that never reached these shapes would prove nothing about them.
            assertTrue("NOT IN was never exercised", notInForms > 0);
            assertTrue("no list in the grid had a NULL member", listsWithNull > 0);
            assertTrue("no member in the grid was wrapped in brackets", bracketedMembers > 0);
            assertTrue("no list met a type without an IN overload", rejectedLists > 0);
        });
    }

    @Test
    public void testSplicedListInsideAViewMatchesWrittenOutList() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t2");

            // A view body is re-parsed as a subquery when the view is read, where a bare ')' ends
            // the subquery rather than the list - which is how a list in a view came to fail while
            // the identical query worked. Top-level fuzzing never reaches that path.
            final Rnd rnd = TestUtils.generateRandom(LOG);
            int overridden = 0;
            int rejected = 0;
            for (int c = 0, n = COLUMNS.size(); c < n; c++) {
                final FuzzColumn column = COLUMNS.getQuick(c);
                final String col = column.name();
                for (int members = 1; members <= 3; members++) {
                    final StringSink list = new StringSink();
                    for (int m = 0; m < members; m++) {
                        if (m > 0) {
                            list.put(',');
                        }
                        list.put(member(rnd, column));
                    }
                    if (members == 1) {
                        list.put(',');
                    }

                    final String viewName = "v_fuzz_" + col + '_' + members;
                    final String createView = "CREATE VIEW " + viewName + " AS (DECLARE OVERRIDABLE @x := (" + list + ") "
                            + "SELECT " + col + " FROM t2 WHERE " + col + " IN @x)";
                    final String written = "SELECT " + col + " FROM t2 WHERE " + col + " IN ("
                            + stripTrailingComma(list.toString()) + ") ORDER BY " + col;
                    final String expected;
                    try {
                        printSql(written);
                        expected = sink.toString();
                    } catch (SqlException e) {
                        // The view body is compiled when the view is created, so that is where a type
                        // no IN overload takes has to be refused, and in the same words.
                        assertFailsLikeWrittenOut(e, written, createView, () -> execute(createView));
                        rejected++;
                        continue;
                    }
                    execute(createView);
                    drainWalAndViewQueues();

                    // Read it as declared, and with a caller override of a different length, since the
                    // override is re-parsed through the same subquery path.
                    final String viaView = "SELECT " + col + " FROM " + viewName + " ORDER BY " + col;
                    assertReturnsLikeWrittenOut(
                            written,
                            viaView,
                            expected,
                            col,
                            "view with a declared list differs from the written-out list"
                                    + "\n  list    : " + list
                                    + "\n  via view: " + viaView
                                    + "\n  written : " + written
                    );

                    if (rnd.nextBoolean()) {
                        final String other = member(rnd, column) + ", " + member(rnd, column);
                        final String writtenOverride = "SELECT " + col + " FROM t2 WHERE " + col
                                + " IN (" + other + ") ORDER BY " + col;
                        printSql(writtenOverride);
                        final String expectedOverride = sink.toString();
                        final String viaOverride = "DECLARE @x := (" + other + ") SELECT " + col
                                + " FROM " + viewName + " ORDER BY " + col;
                        assertReturnsLikeWrittenOut(
                                writtenOverride,
                                viaOverride,
                                expectedOverride,
                                col,
                                "overridden list in a view differs from the written-out list"
                                        + "\n  override: " + viaOverride
                                        + "\n  written : " + writtenOverride
                        );
                        overridden++;
                    }
                }
            }
            assertTrue("no view had its list overridden, so that path went untested", overridden > 0);
            assertTrue("no view met a type without an IN overload", rejected > 0);
        });
    }

    private static void addColumn(String name, ObjList<String> members) {
        COLUMNS.add(new FuzzColumn(name, members));
    }

    // The error has to read the same and point at the same text: a member's own error at that member,
    // which in the declared form sits in the declaration, and an overload error at the IN.
    private static void assertFailsLikeWrittenOut(
            SqlException writtenError,
            String written,
            String sql,
            TestUtils.LeakProneCode code
    ) throws Exception {
        try {
            code.run();
        } catch (SqlException e) {
            final String context = "\n  written: " + written + "\n  other  : " + sql;
            assertEquals(
                    "fails differently from the written-out list" + context,
                    writtenError.getFlyweightMessage().toString(),
                    e.getFlyweightMessage().toString()
            );
            assertEquals(
                    "error points elsewhere than in the written-out list" + context,
                    tokenAt(written, writtenError.getPosition()),
                    tokenAt(sql, e.getPosition())
            );
            return;
        }
        fail("the written-out list fails with '" + writtenError.getFlyweightMessage() + "' but this succeeds"
                + "\n  written: " + written + "\n  other  : " + sql);
    }

    private static void createTable(String name) throws SqlException {
        execute(DDL.formatted(name));
        execute(ROWS.formatted(name));
    }

    // A NULL member is a member like any other: IN treats NULL as equal to NULL, so it matches the
    // tables' NULL row, and the written-out list has to agree on that as on everything else. Grouping
    // brackets around a member change nothing about its value, only what the list parser has to see
    // through.
    private static String member(Rnd rnd, FuzzColumn column) {
        if (rnd.nextInt(6) == 0) {
            return "NULL";
        }
        final ObjList<String> members = column.members();
        final String member = members.getQuick(rnd.nextInt(members.size()));
        return rnd.nextInt(5) == 0 ? '(' + member + ')' : member;
    }

    private static String stripTrailingComma(String list) {
        return list.endsWith(",") ? list.substring(0, list.length() - 1) : list;
    }

    private static String tokenAt(String sql, int position) {
        if (position < 0 || position >= sql.length()) {
            return "<position " + position + '>';
        }
        int end = position;
        while (end < sql.length()
                && !Character.isWhitespace(sql.charAt(end))
                && sql.charAt(end) != ','
                && sql.charAt(end) != ')') {
            end++;
        }
        return sql.substring(position, end);
    }

    /**
     * Asserts that {@code sql} returns what the written-out list returned and that its factory makes
     * the same promises: random access, a known size and the designated timestamp are taken from the
     * written-out query's own factory rather than pinned per type, since what is under test is that the
     * two agree, whichever way the type routes the IN.
     */
    private void assertReturnsLikeWrittenOut(
            String written,
            String sql,
            String expected,
            String col,
            String failureContext
    ) throws Exception {
        final boolean isRandomAccess;
        final boolean hasSize;
        final boolean hasTimestamp;
        try (RecordCursorFactory factory = select(written)) {
            isRandomAccess = factory.recordCursorSupportsRandomAccess();
            hasTimestamp = factory.getMetadata().getTimestampIndex() != -1;
            try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                hasSize = cursor.size() != -1;
            }
        }
        final QueryAssertion assertion = assertQuery(sql)
                .noLeakCheck()
                .supportsRandomAccess(isRandomAccess)
                .expectSize(hasSize);
        if (hasTimestamp) {
            assertion.timestamp(col);
        }
        try {
            assertion.returns(expected);
        } catch (AssertionError | SqlException e) {
            // A SqlException here is the declared side refusing what the written-out side ran.
            throw new AssertionError(failureContext
                    + "\n  expected: " + expected.replace('\n', '/')
                    + "\n  detail  : " + e.getMessage(), e);
        }
    }

    /**
     * Runs one generated list both ways: written out in full, the reference behaviour, and reached
     * through a declared variable, which has to return the same rows from a factory that makes the
     * same promises and plans the same, or, when no IN overload takes the type, fail the same way.
     */
    private void assertSplicedListMatches(
            String col,
            String list,
            String before,
            String after,
            boolean isNotIn,
            boolean isParenthesised
    ) throws Exception {
        final String op = isNotIn ? " NOT IN " : " IN ";
        final String written = "SELECT " + col + " FROM t WHERE " + col + op + "("
                + before + stripTrailingComma(list) + after + ") ORDER BY " + col;
        final String declaredRhs = isParenthesised && before.isEmpty() && after.isEmpty()
                ? "(@x)"
                : before.isEmpty() && after.isEmpty() ? "@x" : "(" + before + "@x" + after + ")";
        final String declared = "DECLARE @x := (" + list + ") SELECT " + col + " FROM t WHERE "
                + col + op + declaredRhs + " ORDER BY " + col;

        // The reference side is printed, because what it returns is what the declared side has to
        // match.
        final String expected;
        try {
            printSql(written);
            expected = sink.toString();
        } catch (SqlException e) {
            assertFailsLikeWrittenOut(e, written, declared, () -> printSql(declared));
            rejectedLists++;
            return;
        }
        assertReturnsLikeWrittenOut(
                written,
                declared,
                expected,
                col,
                "spliced list differs from the written-out list"
                        + "\n  written : " + written
                        + "\n  declared: " + declared
        );

        // Matching rows is the weaker half of the property. The splice also has to produce the same
        // NODE as the written-out list, and only the plan shows that: a one-member list that kept the
        // operator node type returned exactly these rows while quietly dropping off the JIT filter
        // path. Comparing plans is what catches that class of divergence - anything that routes on
        // node shape shows up here first.
        printSql("EXPLAIN " + written);
        final String writtenPlan = sink.toString();
        printSql("EXPLAIN " + declared);
        final String declaredPlan = sink.toString();
        if (!writtenPlan.equals(declaredPlan)) {
            throw new AssertionError("spliced list plans differently from the written-out list"
                    + "\n  written : " + written
                    + "\n  declared: " + declared
                    + "\n  plan(written) : " + writtenPlan.replace('\n', '/')
                    + "\n  plan(declared): " + declaredPlan.replace('\n', '/'));
        }
    }

    private record FuzzColumn(String name, ObjList<String> members) {
    }
}
