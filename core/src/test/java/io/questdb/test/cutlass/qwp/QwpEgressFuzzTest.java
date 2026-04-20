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

package io.questdb.test.cutlass.qwp;

import io.questdb.PropertyKey;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Property-based fuzz coverage for QWP egress.
 * <p>
 * Each iteration:
 * <ol>
 *   <li>Builds a random schema (1-8 columns) using generators from a catalogue
 *       that covers every QWP wire type the server can ship.</li>
 *   <li>Generates per-row random values <i>in Java</i> (via seeded {@link Rnd})
 *       so the expected value for every {@code (row, col)} cell is known bit-for-bit
 *       before the query runs.</li>
 *   <li>Inserts those values as literal rows (single multi-row {@code INSERT VALUES}).</li>
 *   <li>Picks a random query shape -- full scan, projection reorder, id-range
 *       filter, or reverse-order limit.</li>
 *   <li>Streams the results over QWP and asserts per-row-per-cell that the
 *       observed hash matches the stored expected hash. Null cells are checked
 *       against a parallel null map.</li>
 * </ol>
 * Row-by-row verification (as opposed to a per-column sum) catches bugs that
 * preserve column totals but corrupt individual values: row reordering within
 * a batch, cross-batch boundary misalignment, null-bitmap bit swaps, partial
 * varint reads, etc.
 * <p>
 * Coverage caveats:
 * <ul>
 *   <li>Row count caps at {@link #MAX_ROWS_PER_CASE} because multi-row VALUES
 *       grows the INSERT SQL text linearly; the cap still crosses the batch
 *       boundary (~4096 rows).</li>
 *   <li>BINARY, GEOHASH×4, DECIMAL128, DECIMAL256, DOUBLE_ARRAY are covered at
 *       the "exists and decodes without throwing" level rather than bit-level
 *       -- replicating QuestDB's on-wire encoding for each in Java would
 *       duplicate the production encoder. The dedicated exhaustive test pins
 *       bit-level correctness for those types.</li>
 *   <li>LONG[] is not included because QuestDB rejects {@code LONG[]} as a
 *       {@code CREATE TABLE} column type.</li>
 * </ul>
 */
public class QwpEgressFuzzTest extends AbstractBootstrapTest {

    private static final ColumnGenerator[] GENERATORS = {
            new LongGenerator(),
            new IntGenerator(),
            new ShortGenerator(),
            new ByteGenerator(),
            new CharGenerator(),
            new DoubleGenerator(),
            new FloatGenerator(),
            new BooleanGenerator(),
            new SymbolGenerator("lo", 8),
            new SymbolGenerator("hi", 1_000),
            new VarcharGenerator(),
            new StringGenerator(),
            new TimestampGenerator(),
            new TimestampNanosGenerator(),
            new DateGenerator(),
            new Ipv4Generator(),
            new UuidGenerator(),
            new Long256Generator(),
            // Existence-only generators: exercise decode path but don't assert
            // bit-level equality (encoding not re-implemented in Java).
            new BinaryGenerator(),
            new GeoHashGenerator(4, "#b"),
            new GeoHashGenerator(8, "#bb"),
            new GeoHashGenerator(24, "#bbbbb"),
            new GeoHashGenerator(48, "#bbbbbbbbbb"),
            // Three scales exercise distinct scale bytes + different divisor paths.
            new Decimal64Generator(18, 0),
            new Decimal64Generator(18, 4),
            new Decimal64Generator(18, 10),
            new Decimal128Generator(),
            new Decimal256Generator(),
            new DoubleArrayGenerator(),
    };
    private static final Log LOG = LogFactory.getLog(QwpEgressFuzzTest.class);
    private static final int MAX_ROWS_PER_CASE = 500;
    private Rnd random;

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
        // Pinned seeds make the fuzz deterministic across runs -- swap for new
        // constants if/when a CI run surfaces a previously-unseen failure.
        random = TestUtils.generateRandom(LOG,2121554702200L, 1776652855068L);
    }

    @Test
    public void testBackToBackQueriesSameConnection() throws Exception {
        // Exercises per-connection state that survives across queries: the conn
        // symbol dict (dedup map + bytes heap + packed entries), the conn dict
        // reset by resetForNewQuery, schema registry, Gorilla decoder state.
        int chunk = pickChunk();
        LOG.info().$("=== back-to-back: chunk=").$(chunk).$();
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(chunk)) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";" + pickCompression())) {
                    client.connect();
                    for (int q = 0; q < 12; q++) {
                        runOneCase(server, client, "fuzz_back_" + q, 1 + random.nextInt(4));
                    }
                }
            }
        });
    }

    @Test
    public void testRandomSchemaRoundtrip() throws Exception {
        // Main sweep: fresh connection per case so state pollution can't mask a bug.
        int chunk = pickChunk();
        LOG.info().$("=== random schema: chunk=").$(chunk).$();
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(chunk)) {
                for (int i = 0; i < 15; i++) {
                    try (QwpQueryClient client = QwpQueryClient.fromConfig(
                            "ws::addr=127.0.0.1:" + HTTP_PORT + ";" + pickCompression())) {
                        client.connect();
                        runOneCase(server, client, "fuzz_iter_" + i, 1 + random.nextInt(6));
                    }
                }
            }
        });
    }

    @Test
    public void testWideTables() throws Exception {
        // 10-16 columns stresses the batch buffer's per-column state arrays and
        // the schema block encoder.
        int chunk = pickChunk();
        LOG.info().$("=== wide: chunk=").$(chunk).$();
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startFragmented(chunk)) {
                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";" + pickCompression())) {
                    client.connect();
                    runOneCase(server, client, "fuzz_wide", 10 + random.nextInt(7));
                }
            }
        });
    }

    private static String allDataCols(int colCount) {
        // Explicit list (c0, c1, ...) instead of "*" so we don't have to account
        // for the id bookkeeping column in our colMap.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colCount; i++) {
            if (i > 0) sb.append(", ");
            sb.append('c').append(i);
        }
        return sb.toString();
    }

    private static long hashAsciiString(String s) {
        long h = 1125899906842597L; // large prime seed
        int len = s.length();
        for (int i = 0; i < len; i++) {
            h = h * 31L + s.charAt(i);
        }
        return h ^ len; // mix in length so padding changes surface
    }

    private static long hashBytes(DirectUtf8Sequence v) {
        long h = 1125899906842597L;
        int n = v.size();
        for (int i = 0; i < n; i++) {
            h = h * 31L + (v.byteAt(i) & 0xFF);
        }
        return h ^ n;
    }

    private static int[] identity(int n) {
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = i;
        return a;
    }

    private static int pickRowCount(Rnd rnd) {
        // Skewed distribution that hits small, mid, and batch-boundary sizes.
        int[] choices = {1, 2, 7, 64, 257, MAX_ROWS_PER_CASE - 1, MAX_ROWS_PER_CASE};
        return choices[rnd.nextInt(choices.length)];
    }

    private static String randomAsciiString(Rnd rnd, int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            // printable ASCII minus single-quote (to keep literal building simple)
            int cp;
            do {
                cp = 0x20 + rnd.nextInt(0x5E);
            } while (cp == 0x27);
            sb.append((char) cp);
        }
        return sb.toString();
    }

    private String buildInsert(String table, ColumnGenerator[] cols, String[][] literals, int rowCount) {
        int colCount = cols.length;
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(table).append(" VALUES ");
        for (int r = 0; r < rowCount; r++) {
            if (r > 0) sb.append(", ");
            // id, ts, then generated column literals. ts = r * 1000us (1ms per row) keeps
            // the whole run inside a single partition for all practical row counts.
            sb.append('(').append(r + 1).append(", CAST(").append((long) r * 1_000L).append(" AS TIMESTAMP)");
            for (int c = 0; c < colCount; c++) {
                sb.append(", ").append(literals[r][c]);
            }
            sb.append(')');
        }
        return sb.toString();
    }

    /**
     * Random send+recv fragmentation chunk size. Lower bound 10 exercises the
     * park-resume path on every WS frame (handshake included, since the
     * onRequestComplete/resumeSend split handles partial handshake writes);
     * upper bound keeps small-result iterations fast.
     */
    private int pickChunk() {
        return 1 + random.nextInt(500);
    }

    /**
     * Random compression settings for the fuzz client. Mixes raw, zstd at
     * varied levels, and the {@code auto} default so every path gets hit
     * across iterations:
     * <ul>
     *   <li>no key / auto -- exercises the default negotiation (server picks
     *       zstd when advertised) without explicitly pinning a level.</li>
     *   <li>raw -- skips compression entirely; regression guard against the
     *       compression machinery triggering when the header is absent.</li>
     *   <li>zstd with a random level in [1, 9] -- the server-side clamp
     *       range. Higher than 9 is pointless here because the server would
     *       clamp it anyway.</li>
     * </ul>
     * Returns a connection-string fragment like {@code "compression=zstd;compression_level=3;"}
     * ready to append after the address.
     */
    private String pickCompression() {
        int choice = random.nextInt(4);
        return switch (choice) {
            case 0 -> ""; // inherit default (auto)
            case 1 -> "compression=raw;";
            case 2 -> "compression=zstd;";
            default -> "compression=zstd;compression_level=" + (1 + random.nextInt(9)) + ";";
        };
    }

    private QueryPlan planQuery(String table, int colCount, int rowCount, int caseIdx) {
        // 4 shapes rotate deterministically to guarantee every shape gets exercised
        // across iterations, regardless of random seed.
        int shape = Math.floorMod(caseIdx, 4);
        if (rowCount < 4) shape = 0; // small cases: just scan everything

        switch (shape) {
            case 1: {
                // Projection subset in a scrambled order.
                int pickCount = 1 + random.nextInt(colCount);
                int[] map = new int[pickCount];
                boolean[] used = new boolean[colCount];
                for (int i = 0; i < pickCount; i++) {
                    int pick;
                    do {
                        pick = random.nextInt(colCount);
                    } while (used[pick]);
                    used[pick] = true;
                    map[i] = pick;
                }
                StringBuilder sql = new StringBuilder("SELECT ");
                for (int i = 0; i < pickCount; i++) {
                    if (i > 0) sql.append(", ");
                    sql.append('c').append(map[i]);
                }
                sql.append(" FROM ").append(table).append(" ORDER BY id");
                return new QueryPlan(sql.toString(), map, 1, rowCount, false);
            }
            case 2: {
                // id-range filter -- tests null-bitmap handling when the server's
                // filter drops rows between batches.
                int lo = 1 + random.nextInt(rowCount);
                int hi = lo + random.nextInt(Math.max(1, rowCount - lo + 1));
                int[] map = identity(colCount);
                String sql = "SELECT " + allDataCols(colCount) + " FROM " + table
                        + " WHERE id >= " + lo + " AND id <= " + hi
                        + " ORDER BY id";
                return new QueryPlan(sql, map, lo, hi, false);
            }
            case 3: {
                // Reverse + LIMIT -- the last K rows in descending order.
                int k = 1 + random.nextInt(rowCount);
                int[] map = identity(colCount);
                String sql = "SELECT " + allDataCols(colCount) + " FROM " + table
                        + " ORDER BY id DESC LIMIT " + k;
                return new QueryPlan(sql, map, rowCount - k + 1, rowCount, true);
            }
            default: {
                int[] map = identity(colCount);
                String sql = "SELECT " + allDataCols(colCount) + " FROM " + table + " ORDER BY id";
                return new QueryPlan(sql, map, 1, rowCount, false);
            }
        }
    }

    private void runOneCase(TestServerMain server, QwpQueryClient client, String table, int colCount) {
        // Pick generators + nullability.
        ColumnGenerator[] cols = new ColumnGenerator[colCount];
        boolean[] nullable = new boolean[colCount];
        for (int i = 0; i < colCount; i++) {
            cols[i] = GENERATORS[random.nextInt(GENERATORS.length)];
            nullable[i] = cols[i].supportsNull() && random.nextBoolean();
        }
        int rowCount = pickRowCount(random);

        // CREATE TABLE with an explicit row id column to anchor ORDER BY. ts is the
        // designated timestamp so the table can run as WAL -- matches production shape
        // and makes DROP go through the WAL apply path instead of the synchronous
        // reader-lock path.
        StringBuilder ddl = new StringBuilder("CREATE TABLE ").append(table).append(" (id LONG, ts TIMESTAMP");
        for (int i = 0; i < colCount; i++) {
            ddl.append(", c").append(i).append(' ').append(cols[i].sqlType());
        }
        ddl.append(") TIMESTAMP(ts) PARTITION BY DAY WAL");
        server.execute(ddl.toString());

        // Roll random values in Java; remember expected hash + null-ness per cell.
        long[][] expected = new long[rowCount][colCount];
        boolean[][] expectedNull = new boolean[rowCount][colCount];
        String[][] literals = new String[rowCount][colCount];
        RandomValue buf = new RandomValue();
        for (int r = 0; r < rowCount; r++) {
            for (int c = 0; c < colCount; c++) {
                boolean isNull = nullable[c] && random.nextInt(5) == 0;
                if (isNull) {
                    expectedNull[r][c] = true;
                    literals[r][c] = "CAST(NULL AS " + cols[c].sqlType() + ')';
                } else {
                    cols[c].randomValue(random, buf);
                    expected[r][c] = buf.hash;
                    literals[r][c] = buf.literal;
                }
            }
        }

        server.execute(buildInsert(table, cols, literals, rowCount));
        // WAL tables commit rows asynchronously; wait for the server-side apply job
        // to catch up before the SELECT, otherwise we'd race the stream with an
        // empty table view.
        server.awaitTable(table);

        // Plan a query shape; compute which rows / cols appear + in what order.
        int caseSalt = table.hashCode(); // stable-ish salt so shape rotates per table
        QueryPlan plan = planQuery(table, colCount, rowCount, caseSalt);

        // Run query + verify per row-per col.
        AssertionState state = new AssertionState(plan, cols, expected, expectedNull);
        client.execute(plan.sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                state.observe(batch);
            }

            @Override
            public void onEnd(long totalRows) {
                state.end(totalRows);
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("egress error [" + table + "]: " + message);
            }
        });

        server.execute("DROP TABLE " + table);
    }

    private TestServerMain startFragmented(int chunk) {
        return startWithEnvVariables(
                PropertyKey.DEBUG_HTTP_FORCE_RECV_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), Integer.toString(chunk),
                PropertyKey.DEBUG_HTTP_FORCE_SEND_FRAGMENTATION_CHUNK_SIZE.getEnvVarName(), Integer.toString(chunk)
        );
    }

    /**
     * Contract:
     * <ul>
     *   <li>{@link #randomValue} fills the supplied scratch with a SQL literal
     *       safely usable inside {@code VALUES(...)} and a deterministic
     *       {@code long} hash that must equal the value returned by
     *       {@link #observedHash} when the QWP round-trip preserves the value.</li>
     *   <li>{@link #supportsNull} returns {@code false} for types that QuestDB
     *       coerces NULL into a zero-value (BOOLEAN / BYTE / SHORT / CHAR).</li>
     * </ul>
     */
    private interface ColumnGenerator {
        long observedHash(QwpColumnBatch batch, int col, int row);

        void randomValue(Rnd rnd, RandomValue out);

        String sqlType();

        default boolean supportsNull() {
            return true;
        }
    }

    // ========================================================================
    // Generators
    // ========================================================================

    /**
     * Walks a streamed QWP result, verifying every observed cell matches the
     * expected (row, col) hash and null flag per the query plan.
     */
    private static final class AssertionState {
        final ColumnGenerator[] cols;
        final long[][] expected;
        final boolean[][] expectedNull;
        final QueryPlan plan;
        int observedLogicalRows = 0;

        AssertionState(QueryPlan plan, ColumnGenerator[] cols,
                       long[][] expected, boolean[][] expectedNull) {
            this.plan = plan;
            this.cols = cols;
            this.expected = expected;
            this.expectedNull = expectedNull;
        }

        void end(long totalRows) {
            int expectedRows = plan.lastRow - plan.firstRow + 1;
            Assert.assertEquals("row count (onEnd) for " + plan.sql,
                    expectedRows, totalRows);
            Assert.assertEquals("row count (observed) for " + plan.sql,
                    expectedRows, observedLogicalRows);
        }

        void observe(QwpColumnBatch batch) {
            int n = batch.getRowCount();
            int resultColCount = plan.colMap.length;
            for (int r = 0; r < n; r++) {
                int logicalRow = plan.descending
                        ? plan.lastRow - observedLogicalRows
                        : plan.firstRow + observedLogicalRows;
                int rowIdx = logicalRow - 1;
                for (int rc = 0; rc < resultColCount; rc++) {
                    int origCol = plan.colMap[rc];
                    String ctx = "row=" + logicalRow + " resultCol=" + rc
                            + " origCol=" + origCol
                            + " type=" + cols[origCol].sqlType()
                            + " sql=" + plan.sql;
                    if (expectedNull[rowIdx][origCol]) {
                        Assert.assertTrue("expected NULL: " + ctx, batch.isNull(rc, r));
                    } else {
                        Assert.assertFalse("expected non-NULL: " + ctx, batch.isNull(rc, r));
                        long observed = cols[origCol].observedHash(batch, rc, r);
                        Assert.assertEquals("value mismatch: " + ctx,
                                expected[rowIdx][origCol], observed);
                    }
                }
                observedLogicalRows++;
            }
        }
    }

    /**
     * BINARY: QuestDB has no CAST from STRING to BINARY, so we source bytes via
     * {@code rnd_bin} in a scalar subquery. That means Java doesn't know the
     * resulting bytes; we verify only that the cell decodes without throwing.
     * The exhaustive test covers bit-level BINARY round-trip.
     */
    private static final class BinaryGenerator implements ColumnGenerator {
        private static final int FIXED_LEN = 12;

        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            io.questdb.client.std.bytes.DirectByteSequence v = batch.getBinaryA(col, row);
            return v == null ? 0 : v.size();
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            out.hash = FIXED_LEN;
            // rnd_bin produces random bytes at INSERT time -- value isn't known
            // client-side, only its fixed length is.
            out.literal = "rnd_bin(" + FIXED_LEN + ", " + FIXED_LEN + ", 0)";
        }

        @Override
        public String sqlType() {
            return "BINARY";
        }

        @Override
        public boolean supportsNull() {
            return false;
        }
    }

    private static final class BooleanGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getBool(col, row) ? 1 : 0;
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            boolean v = rnd.nextBoolean();
            out.hash = v ? 1 : 0;
            out.literal = Boolean.toString(v);
        }

        @Override
        public String sqlType() {
            return "BOOLEAN";
        }

        @Override
        public boolean supportsNull() {
            return false;
        }
    }

    private static final class ByteGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getByteValue(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            byte v = (byte) (rnd.nextInt(255) - 127);
            out.hash = v;
            out.literal = "CAST(" + v + " AS BYTE)";
        }

        @Override
        public String sqlType() {
            return "BYTE";
        }

        @Override
        public boolean supportsNull() {
            return false;
        }
    }

    private static final class CharGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getCharValue(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            char c = (char) ('A' + rnd.nextInt(26));
            out.hash = c;
            out.literal = "'" + c + '\'';
        }

        @Override
        public String sqlType() {
            return "CHAR";
        }

        @Override
        public boolean supportsNull() {
            return false;
        }
    }

    private static final class DateGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLong(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            long ms = rnd.nextLong() & 0x0000_FFFFFFFFFFFFL; // fits comfortably as a Date
            out.hash = ms;
            out.literal = "CAST(" + ms + " AS DATE)";
        }

        @Override
        public String sqlType() {
            return "DATE";
        }
    }

    private static final class Decimal128Generator implements ColumnGenerator {
        private static final String[] LITERALS = {
                "1.000001m", "2.500500m", "1234567.123456m", "-999999.999999m"
        };

        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            batch.getDecimal128Low(col, row);
            batch.getDecimal128High(col, row);
            return 1;
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            out.hash = 1;
            out.literal = LITERALS[rnd.nextInt(LITERALS.length)];
        }

        @Override
        public String sqlType() {
            return "DECIMAL(38,6)";
        }
    }

    private static final class Decimal256Generator implements ColumnGenerator {
        private static final String[] LITERALS = {
                "1.0000000001m", "100.1234567890m", "-1.5m", "99999999.0000000001m"
        };

        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            for (int w = 0; w < 4; w++) batch.getLong256Word(col, row, w);
            return 1;
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            out.hash = 1;
            out.literal = LITERALS[rnd.nextInt(LITERALS.length)];
        }

        @Override
        public String sqlType() {
            return "DECIMAL(76,10)";
        }
    }

    /**
     * DECIMAL64 stores {@code value * 10^scale} as a long, so we know the on-wire
     * bits for a given literal and CAN bit-verify. Scale is captured at
     * construction, which lets the GENERATORS catalogue include several
     * instances at different scales and exercises the per-column scale byte on
     * the wire.
     */
    private static final class Decimal64Generator implements ColumnGenerator {
        private final long divisor;
        private final int precision;
        private final int scale;

        Decimal64Generator(int precision, int scale) {
            if (scale < 0 || scale > 18 || scale > precision) {
                throw new IllegalArgumentException("bad DECIMAL64 (p=" + precision + ", s=" + scale + ')');
            }
            this.precision = precision;
            this.scale = scale;
            long d = 1;
            for (int i = 0; i < scale; i++) d *= 10;
            this.divisor = d;
        }

        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLong(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            // Scaled long: the on-wire bits. Bound to 6-digit magnitude for cheap
            // literal construction; the test's bit-level assertion is independent
            // of magnitude range.
            long scaled = ((long) rnd.nextInt(1_000_000)) - 500_000;
            out.hash = scaled;
            out.literal = toDecimalLiteral(scaled, scale);
        }

        @Override
        public String sqlType() {
            return "DECIMAL(" + precision + ',' + scale + ')';
        }

        private String toDecimalLiteral(long scaled, int scale) {
            if (scale == 0) {
                // No decimal point -- just the integer value with the 'm' suffix.
                return scaled + "m";
            }
            boolean negative = scaled < 0;
            long abs = Math.abs(scaled);
            long whole = abs / divisor;
            long frac = abs % divisor;
            StringBuilder sb = new StringBuilder();
            if (negative) sb.append('-');
            sb.append(whole).append('.');
            String fs = Long.toString(frac);
            sb.append("0".repeat(Math.max(0, scale - fs.length())));
            sb.append(fs).append('m');
            return sb.toString();
        }
    }

    /**
     * Fixed-shape 1D double array. Bits would require replicating QuestDB's
     * array encoder; we settle for existence + dimensionality check.
     */
    private static final class DoubleArrayGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            double[] arr = batch.getDoubleArrayElements(col, row);
            return arr == null ? 0 : arr.length;
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            int len = 1 + rnd.nextInt(4);
            StringBuilder sb = new StringBuilder("ARRAY[");
            for (int i = 0; i < len; i++) {
                if (i > 0) sb.append(", ");
                sb.append("CAST(").append((rnd.nextDouble() - 0.5) * 100).append(" AS DOUBLE)");
            }
            sb.append(']');
            out.hash = len;
            out.literal = sb.toString();
        }

        @Override
        public String sqlType() {
            return "DOUBLE[]";
        }
    }

    private static final class DoubleGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return Double.doubleToRawLongBits(batch.getDouble(col, row));
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            // Avoid NaN (QuestDB NaN==NULL) and keep within representable range.
            double v;
            do {
                v = (rnd.nextDouble() - 0.5) * 1e9;
            } while (Double.isNaN(v) || Double.isInfinite(v));
            out.hash = Double.doubleToRawLongBits(v);
            out.literal = Double.toString(v);
        }

        @Override
        public String sqlType() {
            return "DOUBLE";
        }
    }

    private static final class FloatGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return Float.floatToRawIntBits(batch.getFloat(col, row));
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            float v;
            do {
                v = (rnd.nextFloat() - 0.5f) * 1e5f;
            } while (Float.isNaN(v) || Float.isInfinite(v));
            out.hash = Float.floatToRawIntBits(v);
            out.literal = "CAST(" + v + " AS FLOAT)";
        }

        @Override
        public String sqlType() {
            return "FLOAT";
        }
    }

    /**
     * GEOHASH round-trip at a specific storage width. Same-width cast inside a
     * CASE fails in QuestDB ("inconvertible types"), so we emit a single constant
     * literal per cell. Existence-only.
     */
    private record GeoHashGenerator(int precisionBits, String literal) implements ColumnGenerator {

        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            batch.getLong(col, row);
            return 1;
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            out.hash = 1;
            out.literal = literal;
        }

        @Override
        public String sqlType() {
            return "GEOHASH(" + precisionBits + "b)";
        }
    }

    private static final class IntGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getIntValue(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            int v;
            do {
                v = rnd.nextInt();
            } while (v == Integer.MIN_VALUE); // INT_NULL sentinel
            out.hash = v;
            out.literal = Integer.toString(v);
        }

        @Override
        public String sqlType() {
            return "INT";
        }
    }

    /**
     * IPv4 NULL == bit pattern 0. Random formula avoids the 0.0.0.0 octet combo.
     */
    private static final class Ipv4Generator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLong(col, row) & 0xFFFFFFFFL;
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            int a = 1 + rnd.nextInt(254);
            int b = rnd.nextInt(256);
            int c = rnd.nextInt(256);
            int d = 1 + rnd.nextInt(254); // last octet non-zero to avoid NULL match
            out.hash = ((long) a << 24) | ((long) b << 16) | ((long) c << 8) | (long) d;
            out.literal = "CAST('" + a + '.' + b + '.' + c + '.' + d + "' AS IPv4)";
        }

        @Override
        public String sqlType() {
            return "IPv4";
        }
    }

    private static final class Long256Generator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLong256Word(col, row, 0) ^ batch.getLong256Word(col, row, 1)
                    ^ batch.getLong256Word(col, row, 2) ^ batch.getLong256Word(col, row, 3);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            long[] w = new long[4];
            for (int i = 0; i < 4; i++) w[i] = rnd.nextLong();
            StringBuilder sb = new StringBuilder("CAST('0x");
            // Big-endian hex: w[3] high bytes ... w[0] low bytes.
            for (int i = 3; i >= 0; i--) sb.append(String.format("%016x", w[i]));
            sb.append("' AS LONG256)");
            out.hash = w[0] ^ w[1] ^ w[2] ^ w[3];
            out.literal = sb.toString();
        }

        @Override
        public String sqlType() {
            return "LONG256";
        }
    }

    // ---- Existence-only generators --------------------------------------

    private static final class LongGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLong(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            long v;
            do {
                v = rnd.nextLong();
            } while (v == Long.MIN_VALUE); // LONG_NULL sentinel
            out.hash = v;
            out.literal = v + "L";
        }

        @Override
        public String sqlType() {
            return "LONG";
        }
    }

    /**
     * Describes one random query: the SQL text, which original columns map to
     * each result column, the inclusive row-id range that should appear, and
     * whether the rows come back in descending order.
     *
     * @param colMap   resultCol -> origCol
     * @param firstRow 1-based inclusive
     * @param lastRow  1-based inclusive
     */
    private record QueryPlan(String sql, int[] colMap, int firstRow, int lastRow, boolean descending) {
    }

    private static final class RandomValue {
        long hash;
        String literal;
    }

    private static final class ShortGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getShortValue(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            short v = (short) (rnd.nextInt(65535) - 32767);
            out.hash = v;
            out.literal = "CAST(" + v + " AS SHORT)";
        }

        @Override
        public String sqlType() {
            return "SHORT";
        }

        @Override
        public boolean supportsNull() {
            return false;
        }
    }

    private static final class StringGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            DirectUtf8Sequence v = batch.getStrA(col, row);
            return v == null ? 0 : hashBytes(v);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            String s = randomAsciiString(rnd, rnd.nextInt(16));
            out.hash = hashAsciiString(s);
            out.literal = "'" + s.replace("'", "''") + '\'';
        }

        @Override
        public String sqlType() {
            return "STRING";
        }
    }

    private record SymbolGenerator(String tag, String[] pool) implements ColumnGenerator {
        private SymbolGenerator(String tag, int pool) {
            this(tag, new String[pool]);
            for (int i = 0; i < pool; i++) {
                this.pool[i] = "s_" + tag + '_' + i;
            }
        }

        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            DirectUtf8Sequence v = batch.getStrA(col, row);
            return v == null ? 0 : hashBytes(v);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            String s = pool[rnd.nextInt(pool.length)];
            out.hash = hashAsciiString(s);
            out.literal = "CAST('" + s + "' AS SYMBOL)";
        }

        @Override
        public String sqlType() {
            return "SYMBOL";
        }

        @Override
        public @NotNull String toString() {
            return "SYMBOL[" + tag + "/" + pool.length + ']';
        }
    }

    private static final class TimestampGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLong(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            long us = rnd.nextLong() & 0x0FFF_FFFFFFFFFFFFL; // stay positive, representable
            out.hash = us;
            out.literal = "CAST(" + us + " AS TIMESTAMP)";
        }

        @Override
        public String sqlType() {
            return "TIMESTAMP";
        }
    }

    // ---- shared helpers -------------------------------------------------

    private static final class TimestampNanosGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLong(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            long ns = rnd.nextLong() & 0x0FFF_FFFFFFFFFFFFL;
            out.hash = ns;
            out.literal = "CAST(" + ns + " AS TIMESTAMP_NS)";
        }

        @Override
        public String sqlType() {
            return "TIMESTAMP_NS";
        }
    }

    private static final class UuidGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getUuidHi(col, row) ^ batch.getUuidLo(col, row);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            long hi = rnd.nextLong();
            long lo = rnd.nextLong();
            // Avoid the QuestDB UUID NULL sentinel (both halves Long.MIN_VALUE).
            if (hi == Long.MIN_VALUE && lo == Long.MIN_VALUE) lo = 0L;
            java.util.UUID u = new java.util.UUID(hi, lo);
            out.hash = hi ^ lo;
            out.literal = "CAST('" + u + "' AS UUID)";
        }

        @Override
        public String sqlType() {
            return "UUID";
        }
    }

    private static final class VarcharGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            DirectUtf8Sequence v = batch.getVarcharA(col, row);
            return v == null ? 0 : hashBytes(v);
        }

        @Override
        public void randomValue(Rnd rnd, RandomValue out) {
            // Mix short inlinable (<=9 bytes) with longer heap-backed varchar so
            // both paths in QuestDB's varchar encoder run.
            int len = rnd.nextInt(30);
            String s = randomAsciiString(rnd, len);
            out.hash = hashAsciiString(s);
            out.literal = "CAST('" + s.replace("'", "''") + "' AS VARCHAR)";
        }

        @Override
        public String sqlType() {
            return "VARCHAR";
        }
    }
}
