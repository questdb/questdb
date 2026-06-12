package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class ParquetLimitProbeTest extends AbstractCairoTest {

    // BINARY var-len column with a mid-frame offset. BinaryTypeDriver extends
    // StringTypeDriver, so it takes the aux-shift rebase path. Not covered by the
    // PR's tests (those use STRING and VARCHAR).
    @Test
    public void testBinaryVarLenMidFrameOffset() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (b BINARY, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT rnd_bin(4, 32, 2), timestamp_sequence('2024-01-01', 60_000_000) FROM long_sequence(40)");
            execute("INSERT INTO x VALUES (rnd_bin(4, 8, 0), '2024-01-02T00:00:00.000000Z')");
            execute("CREATE TABLE x_native AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts in '2024-01-01'");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT * FROM x_native LIMIT 18, 24",
                    "SELECT * FROM x LIMIT 18, 24",
                    LOG);
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT * FROM x_native LIMIT 5, 35",
                    "SELECT * FROM x LIMIT 5, 35",
                    LOG);
        });
    }

    // The same parquet column projected twice. openParquet dedups decode slots;
    // remapColumns fans one decoded slot out to two query columns. With a mid-frame
    // offset both copies must carry the same rebased addresses.
    @Test
    public void testDuplicateColumnProjectionMidFrameOffset() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 16);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (i INT, v VARCHAR, s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT x::INT, 'v' || x, 's' || x, timestamp_sequence('2024-01-01', 60_000_000) FROM long_sequence(40)");
            execute("INSERT INTO x VALUES (0, 'v0', 's0', '2024-01-02T00:00:00.000000Z')");
            execute("CREATE TABLE x_native AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts in '2024-01-01'");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT i, i AS i2, v, v AS v2, s, s AS s2 FROM x_native LIMIT 18, 24",
                    "SELECT i, i AS i2, v, v AS v2, s, s AS s2 FROM x LIMIT 18, 24",
                    LOG);
        });
    }

    // A single-row parquet partition, plus LIMIT larger than the table, LIMIT 0,
    // and an offset landing exactly on the partition boundary.
    @Test
    public void testEdgeLimitsSingleRowAndBoundaries() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x VALUES " +
                    "(1, '2024-01-01T00:00:00.000000Z'), " +
                    "(2, '2024-01-02T00:00:00.000000Z'), " +
                    "(3, '2024-01-03T00:00:00.000000Z')");
            execute("CREATE TABLE x_native AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts in ('2024-01-01','2024-01-02')");
            for (String lim : new String[]{
                    "LIMIT 0", "LIMIT 100", "LIMIT 1", "LIMIT 1, 1", "LIMIT 1, 2",
                    "LIMIT 2, 3", "LIMIT 3, 100", "LIMIT -1", "LIMIT -100", "LIMIT -2, -1"
            }) {
                TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                        "SELECT * FROM x_native " + lim,
                        "SELECT * FROM x " + lim,
                        LOG);
            }
        });
    }

    // ARRAY column (DOUBLE[][]) is var-size; exercise the aux-shift rebase for arrays
    // with a mid-frame offset crossing row groups.
    @Test
    public void testArrayMidFrameOffsetAcrossRowGroups() throws Exception {
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 8);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (a INT, arr DOUBLE[][], ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY");
            execute("INSERT INTO x SELECT x::INT, ARRAY[[x::DOUBLE, x+0.5],[x*2.0, x*3.0]], timestamp_sequence('2024-01-01', 60_000_000) FROM long_sequence(40)");
            execute("INSERT INTO x VALUES (0, ARRAY[[0.0,0.0],[0.0,0.0]], '2024-01-02T00:00:00.000000Z')");
            execute("CREATE TABLE x_native AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY DAY");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET WHERE ts in '2024-01-01'");
            TestUtils.assertSqlCursors(engine, sqlExecutionContext,
                    "SELECT * FROM x_native LIMIT 13, 30",
                    "SELECT * FROM x LIMIT 13, 30",
                    LOG);
        });
    }
}
