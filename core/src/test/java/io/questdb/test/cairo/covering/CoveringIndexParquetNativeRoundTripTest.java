package io.questdb.test.cairo.covering;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A COVERING posting index converted to Parquet and then back to native must
 * keep returning its covered values. {@code convertPartitionParquetToNative}
 * previously rebuilt the posting index as non-covering and dropped the covering
 * sidecars (.pci/.pc*), so a covering scan over the reconverted partition read
 * NULL for every covered column while a non-covering scan of the same data was
 * correct. The reconvert path now links the existing index files (including the
 * covering sidecars) across, mirroring the native->parquet direction.
 */
public class CoveringIndexParquetNativeRoundTripTest extends AbstractCairoTest {

    @Test
    public void testMultiKeyCoveringSurvivesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            roundTrip();

            // Multi-key (IN-list) covering scan, plus a full covered-vs-uncovered
            // cursor comparison across every row.
            assertQuery("SELECT sum(price) sum_price, count() c FROM t_rt WHERE sym IN ('A0', 'A2')")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tc\n2550.0\t50\n");
            assertSqlCursors(
                    "SELECT ts, sym, price, tag FROM t_rt ORDER BY ts",
                    "SELECT /*+ no_covering */ ts, sym, price, tag FROM t_rt ORDER BY ts"
            );
        });
    }

    @Test
    public void testBitmapIndexSurvivesRoundTrip() throws Exception {
        // A plain (non-posting) BITMAP symbol index is also carried across the
        // reconvert via the link path; assert an indexed scan still resolves.
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX");
            drainWalQueue();

            roundTrip();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_rt'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");
            // Indexed scan resolves the same rows as a full scan of the same
            // predicate (the cast on the reference side defeats index usage).
            assertSqlCursors(
                    "SELECT ts, sym, price FROM t_rt WHERE sym = 'A0' ORDER BY ts",
                    "SELECT ts, sym, price FROM t_rt WHERE sym::string = 'A0' ORDER BY ts"
            );
            assertQuery("SELECT count() c, sum(price) s FROM t_rt WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("c\ts\n25\t1300.0\n");
        });
    }

    @Test
    public void testPostingCoveringSurvivesRoundTrip() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            assertCovered("native (pre-parquet)");
            roundTrip();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't_rt'")
                    .noRandomAccess()
                    .noLeakCheck()
                    .returns("suspended\nfalse\n");

            // Control: the reconverted native data is intact regardless of the
            // covering index.
            assertQuery("SELECT /*+ no_covering */ sum(price) sum_price, first(tag) first_tag FROM t_rt WHERE sym = 'A0'")
                    .noRandomAccess()
                    .expectSize()
                    .noLeakCheck()
                    .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
            // The covering scan must agree (it returned NULL before the fix).
            assertCovered("native (post round-trip)");
        });
    }

    @Test
    public void testRepeatedRoundTripsKeepCovering() throws Exception {
        assertMemoryLeak(() -> {
            createAndSeed();
            execute("ALTER TABLE t_rt ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (price, tag)");
            drainWalQueue();

            for (int i = 0; i < 3; i++) {
                roundTrip();
                assertCovered("round-trip #" + i);
            }
        });
    }

    private void assertCovered(String stage) throws Exception {
        assertQuery("SELECT sum(price) sum_price, first(tag) first_tag FROM t_rt WHERE sym = 'A0' -- " + stage)
                .noRandomAccess()
                .expectSize()
                .noLeakCheck()
                .returns("sum_price\tfirst_tag\n1300.0\tV0\n");
    }

    private void createAndSeed() throws Exception {
        execute("""
                CREATE TABLE t_rt (
                    ts TIMESTAMP,
                    sym SYMBOL,
                    price DOUBLE,
                    tag VARCHAR
                ) TIMESTAMP(ts) PARTITION BY DAY WAL
                """);
        execute("""
                INSERT INTO t_rt
                SELECT
                    dateadd('m', x::INT, '2024-01-01T00:00:00Z'::TIMESTAMP),
                    'A' || (x % 4), x::DOUBLE, 'V' || (x % 4)
                FROM long_sequence(100)
                """);
        drainWalQueue();
    }

    private void roundTrip() throws Exception {
        execute("ALTER TABLE t_rt CONVERT PARTITION TO PARQUET LIST '2024-01-01'");
        drainWalQueue();
        execute("ALTER TABLE t_rt CONVERT PARTITION TO NATIVE LIST '2024-01-01'");
        drainWalQueue();
    }
}
