package io.questdb.test.griffin;

import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * The property the whole design rests on: a declared list spliced into IN is indistinguishable from
 * the same list written out in full. Everything else - which IN overload applies, whether an
 * element keeps its own type, whether the members stay in order - follows from that, so it is worth
 * asserting over shapes nobody chose by hand rather than only the ones that occurred to me.
 */
public class DeclareListFuzzTest extends AbstractCairoTest {

    private static final int ITERATIONS = 200;
    private static final String[] SYMBOLS = {"AAPL", "MSFT", "TSLA", "AMZN", "GOOG"};

    @Test
    public void testSplicedListMatchesWrittenOutList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (s SYMBOL, l LONG)");
            execute("INSERT INTO t VALUES ('AAPL',1),('MSFT',2),('TSLA',3),('AMZN',4),('GOOG',5)");

            final Rnd rnd = new Rnd();
            int oneMemberLists = 0;
            int mixedWithLiterals = 0;
            int notInForms = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                final boolean useLong = rnd.nextBoolean();
                final boolean notIn = rnd.nextBoolean();
                final boolean parenthesised = rnd.nextBoolean();
                // 0 = list alone, 1 = literal before it, 2 = literal after it, 3 = both
                final int shape = rnd.nextInt(4);
                final int members = 1 + rnd.nextInt(4);

                final StringSink list = new StringSink();
                for (int m = 0; m < members; m++) {
                    if (m > 0) {
                        list.put(',');
                    }
                    list.put(member(rnd, useLong));
                }
                if (members == 1) {
                    // The only way to write a list of one; without it the brackets are grouping.
                    list.put(',');
                    oneMemberLists++;
                }

                final String before = shape == 1 || shape == 3 ? member(rnd, useLong) + ", " : "";
                final String after = shape == 2 || shape == 3 ? ", " + member(rnd, useLong) : "";
                if (!before.isEmpty() || !after.isEmpty()) {
                    mixedWithLiterals++;
                }
                if (notIn) {
                    notInForms++;
                }

                final String col = useLong ? "l" : "s";
                final String op = notIn ? " NOT IN " : " IN ";
                // Written out in full: the reference behaviour.
                final String written = "SELECT " + col + " FROM t WHERE " + col + op + "("
                        + before + stripTrailingComma(list.toString()) + after + ") ORDER BY " + col;
                // The same members reached through a declared variable.
                final String declaredRhs = parenthesised && before.isEmpty() && after.isEmpty()
                        ? "(@x)"
                        : before.isEmpty() && after.isEmpty() ? "@x" : "(" + before + "@x" + after + ")";
                final String declared = "DECLARE @x := (" + list + ") SELECT " + col + " FROM t WHERE "
                        + col + op + declaredRhs + " ORDER BY " + col;

                printSql(written);
                final String expected = sink.toString();
                printSql(declared);
                final String actual = sink.toString();
                if (!expected.equals(actual)) {
                    throw new AssertionError("spliced list differs from the written-out list"
                            + "\n  written : " + written
                            + "\n  declared: " + declared
                            + "\n  expected: " + expected.replace('\n', '/')
                            + "\n  actual  : " + actual.replace('\n', '/'));
                }
            }
            // A fuzz run that never reached these shapes would prove nothing about them.
            assertTrue("no one-member lists generated", oneMemberLists > 0);
            assertTrue("lists were never mixed with literals", mixedWithLiterals > 0);
            assertTrue("NOT IN was never exercised", notInForms > 0);
        });
    }

    @Test
    public void testSplicedListInsideAViewMatchesWrittenOutList() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t2 (s SYMBOL, l LONG)");
            execute("INSERT INTO t2 VALUES ('AAPL',1),('MSFT',2),('TSLA',3),('AMZN',4),('GOOG',5)");

            // A view body is re-parsed as a subquery when the view is read, where a bare ')' ends
            // the subquery rather than the list - which is how a list in a view came to fail while
            // the identical query worked. Top-level fuzzing never reaches that path.
            final Rnd rnd = new Rnd();
            int overridden = 0;
            for (int i = 0; i < 60; i++) {
                final boolean useLong = rnd.nextBoolean();
                final int members = 1 + rnd.nextInt(3);
                final String col = useLong ? "l" : "s";

                final StringSink list = new StringSink();
                for (int m = 0; m < members; m++) {
                    if (m > 0) {
                        list.put(',');
                    }
                    list.put(member(rnd, useLong));
                }
                if (members == 1) {
                    list.put(',');
                }

                final String viewName = "v_fuzz_" + i;
                execute("CREATE VIEW " + viewName + " AS (DECLARE OVERRIDABLE @x := (" + list + ") "
                        + "SELECT " + col + " FROM t2 WHERE " + col + " IN @x)");
                drainWalAndViewQueues();

                // Read it as declared, and with a caller override of a different length, since the
                // override is re-parsed through the same subquery path.
                printSql("SELECT " + col + " FROM " + viewName + " ORDER BY " + col);
                final String viaView = sink.toString();
                printSql("SELECT " + col + " FROM t2 WHERE " + col + " IN ("
                        + stripTrailingComma(list.toString()) + ") ORDER BY " + col);
                final String written = sink.toString();
                if (!viaView.equals(written)) {
                    throw new AssertionError("view with a declared list differs from the written-out list"
                            + "\n  list    : " + list
                            + "\n  via view: " + viaView.replace('\n', '/')
                            + "\n  written : " + written.replace('\n', '/'));
                }

                if (rnd.nextBoolean()) {
                    final String other = member(rnd, useLong) + ", " + member(rnd, useLong);
                    printSql("DECLARE @x := (" + other + ") SELECT " + col + " FROM " + viewName + " ORDER BY " + col);
                    final String viaOverride = sink.toString();
                    printSql("SELECT " + col + " FROM t2 WHERE " + col + " IN (" + other + ") ORDER BY " + col);
                    if (!viaOverride.equals(sink.toString())) {
                        throw new AssertionError("overridden list in a view differs from the written-out list");
                    }
                    overridden++;
                }
            }
            assertTrue("no view had its list overridden, so that path went untested", overridden > 0);
        });
    }

    private static String member(Rnd rnd, boolean useLong) {
        return useLong ? Integer.toString(1 + rnd.nextInt(5)) : '\'' + SYMBOLS[rnd.nextInt(SYMBOLS.length)] + '\'';
    }

    private static String stripTrailingComma(String list) {
        return list.endsWith(",") ? list.substring(0, list.length() - 1) : list;
    }
}
