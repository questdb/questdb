package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A `TIMESTAMP(ts)` suffix designates a timestamp; it must not reorder the SELECT list.
 */
public class ExplicitTimestampProjectionOrderTest extends AbstractCairoTest {

    @Test
    public void testCtasPersistsSelectListOrder() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            execute("CREATE TABLE r AS (SELECT x, ts FROM (t) TIMESTAMP(ts))");
            assertQuery("SELECT * FROM r").timestamp("ts").expectSize()
                    .returns("x\tts\n1\t2024-01-01T00:10:00.000000Z\n");
        });
    }

    @Test
    public void testSuffixDoesNotReorderSelectList() throws Exception {
        assertMemoryLeak(() -> {
            createTable();
            assertQuery("SELECT x, ts FROM (t) TIMESTAMP(ts)")
                    .timestamp("ts").expectSize()
                    .returns("x\tts\n1\t2024-01-01T00:10:00.000000Z\n");
        });
    }

    @Test
    public void testSuffixOrderMatchesEquivalentSpellings() throws Exception {
        // the same query with the suffix removed, or wrapped in a no-op outer SELECT *,
        // already returns `x, ts`; the suffixed form must agree.
        assertMemoryLeak(() -> {
            createTable();
            assertSqlCursors(
                    "SELECT * FROM (SELECT x, ts FROM (t) TIMESTAMP(ts))",
                    "SELECT x, ts FROM (t) TIMESTAMP(ts)"
            );
        });
    }

    private void createTable() throws Exception {
        execute("CREATE TABLE t (x LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO t VALUES (1, '2024-01-01T00:10:00.000000Z')");
    }
}
