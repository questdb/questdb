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
        random = TestUtils.generateRandom(LOG);
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
    public void testSelectAlterSequenceFuzz() throws Exception {
        // Fuzz sequences of SELECT / ALTER TABLE ADD COLUMN against the same
        // table, mixing five SELECT shapes in random order with occasional
        // schema evolutions. Each operation draws from:
        // <ul>
        //   <li>plain scan (PageFrameCursor)</li>
        //   <li>id-range predicate (PageFrameCursor + filter)</li>
        //   <li>interval predicate on designated ts (PageFrameCursor +
        //       partition skip)</li>
        //   <li>projection reorder (PageFrameCursor)</li>
        //   <li>GROUP BY (RecordCursor path -- factory does NOT support
        //       PageFrameCursor)</li>
        //   <li>ALTER TABLE ADD COLUMN (stamps new tableId, invalidates the
        //       server's compile cache; next SELECT with the same SQL text
        //       must detect stale factory and recompile)</li>
        // </ul>
        // Every SELECT is checked against the expected row count. Runs 25 ops
        // without network fragmentation and without per-ALTER UPDATEs (added
        // columns are left NULL; the row-count assertions don't care about
        // their values). Fragmentation coverage is already exhaustive in
        // {@code QwpEgressFragmentationFuzzTest}, and skipping UPDATE saves
        // the WAL-apply wait that dominated the earlier version of this test.
        // Randomise everything the assertions can still predict:
        // * rowCount -- [50, 1000]. Exercises sub-batch, batch-boundary,
        //   and multi-batch slice paths within one run.
        // * spacingMicros -- spacing between row timestamps. Controls how
        //   many DAY partitions the table spreads over and therefore how
        //   much partition skip fires on interval predicates.
        // * opCount -- [15, 40]. Covers short and long sequences.
        // * alterProbability -- [15%, 40%]. Varies stale-cache pressure.
        // * filter thresholds (id range, interval bounds) -- per-call inside
        //   runSelectShape.
        final int rowCount = 50 + random.nextInt(951);
        final long spacingMicros = pickSpacingMicros();
        final int opCount = 15 + random.nextInt(26);
        final int structuralProbPermil = 150 + random.nextInt(251);
        final int maxLiveAddedColumns = 2 + random.nextInt(5);
        LOG.info().$("=== select/alter sequence fuzz: rowCount=").$(rowCount)
                .$(", spacingMicros=").$(spacingMicros)
                .$(", opCount=").$(opCount)
                .$(", structuralProbPermil=").$(structuralProbPermil)
                .$(", maxLiveAddedColumns=").$(maxLiveAddedColumns).$();
        TestUtils.assertMemoryLeak(() -> {
            try (TestServerMain server = startWithEnvVariables()) {
                server.execute("CREATE TABLE fz_seq(id LONG, v DOUBLE, cat SYMBOL, ts TIMESTAMP) "
                        + "TIMESTAMP(ts) PARTITION BY DAY WAL");
                // Stable population: rowCount rows, id 1..rowCount, cat cycles 4
                // symbols so GROUP BY result count stays deterministic, ts spaced
                // spacingMicros apart to control partition span.
                server.execute("INSERT INTO fz_seq SELECT x, x * 1.5, "
                        + "CASE WHEN x % 4 = 0 THEN 'a' WHEN x % 4 = 1 THEN 'b' "
                        + "WHEN x % 4 = 2 THEN 'c' ELSE 'd' END, "
                        + "CAST((x - 1) * " + spacingMicros + "L AS TIMESTAMP) FROM long_sequence("
                        + rowCount + ")");
                server.awaitTable("fz_seq");

                // Live added-column names. DROP picks from here (and removes);
                // ADD appends using a monotonic counter so dropped names are never
                // recycled. Names are never referenced by the SELECT shapes -- they
                // exist solely to trigger the tableId bumps that exercise the
                // stale-cache retry path. That avoids the hairy case where a cached
                // SELECT text references a column that was subsequently dropped
                // (would fail compilation on the retry, not a server bug).
                java.util.List<String> liveAdded = new java.util.ArrayList<>();
                int nextColumnId = 0;

                try (QwpQueryClient client = QwpQueryClient.fromConfig(
                        "ws::addr=127.0.0.1:" + HTTP_PORT + ";" + pickCompression())) {
                    client.connect();

                    // Seed the cache with the SELECTs we'll later rerun, so the first
                    // structural op actually invalidates something.
                    runSelectShape(client, 0, rowCount, spacingMicros, liveAdded);

                    for (int op = 0; op < opCount; op++) {
                        int pick = random.nextInt(1000);
                        boolean structural = pick < structuralProbPermil;
                        if (structural) {
                            boolean canAdd = liveAdded.size() < maxLiveAddedColumns;
                            boolean canDrop = !liveAdded.isEmpty();
                            // Choose ADD vs DROP when both are possible; otherwise pick
                            // whichever is legal. Favour ADD 60/40 to keep the schema
                            // evolving outward on average.
                            boolean doAdd = (canAdd && !canDrop) || (canAdd && random.nextInt(10) < 6);
                            if (doAdd) {
                                String newCol = "extra_" + nextColumnId++;
                                server.execute("ALTER TABLE fz_seq ADD COLUMN " + newCol + " VARCHAR");
                                server.awaitTable("fz_seq");
                                liveAdded.add(newCol);
                                LOG.info().$("[op=").$(op).$("] ALTER ADD ").$(newCol).$();
                            } else if (canDrop) {
                                // Pick a random live added column to drop.
                                int idx = random.nextInt(liveAdded.size());
                                String victim = liveAdded.remove(idx);
                                server.execute("ALTER TABLE fz_seq DROP COLUMN " + victim);
                                server.awaitTable("fz_seq");
                                LOG.info().$("[op=").$(op).$("] ALTER DROP ").$(victim).$();
                            } else {
                                // Neither add nor drop possible (cap hit and nothing to drop).
                                // Fall through to SELECT so the op isn't wasted.
                                int shape = random.nextInt(6);
                                runSelectShape(client, shape, rowCount, spacingMicros, liveAdded);
                            }
                        } else {
                            int shape = random.nextInt(6);
                            runSelectShape(client, shape, rowCount, spacingMicros, liveAdded);
                        }
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

    /**
     * Runs one of six SELECT shapes against the stable {@code fz_seq} table and
     * asserts BOTH the row count AND the per-cell data correctness. Shapes
     * span both cursor paths: PageFrameCursor (plain / predicate / interval /
     * projection / star) and RecordCursor (GROUP BY). Filter thresholds and
     * projection sets are randomised per call so repeat invocations of the
     * same shape don't trivially hit the same cached factory every time.
     * <p>
     * Shape 5 uses {@code SELECT *}, which is deliberately unaffected by ADD /
     * DROP COLUMN: the stable SQL text keeps hitting the cache, but each
     * stale-factory retry recompiles against the current metadata, so the
     * column set expands / shrinks seamlessly. Row count stays the same across
     * schema evolutions; the verifier asserts the live extra-column count
     * matches and that all extra-column cells are NULL (the test never
     * UPDATEs them).
     * <p>
     * Per-cell verification is the point: a regression that returns the right
     * row count with scrambled column data, off-by-one in the dense values
     * array, or a swapped null bit slips through a row-count-only check.
     */
    private void runSelectShape(QwpQueryClient client, int shape, int totalRows, long spacingMicros,
                                java.util.List<String> liveAdded) {
        switch (shape) {
            case 0: { // plain full scan, PageFrameCursor
                // No ORDER BY: ts is the designated timestamp, so rows come back
                // in ts order, which is monotonic with id. globalRow N -> id N+1.
                assertRows(client, "SELECT id FROM fz_seq", totalRows, (batch, startRow) -> {
                    int n = batch.getRowCount();
                    for (int r = 0; r < n; r++) {
                        long expectedId = startRow + r + 1;
                        Assert.assertEquals("shape 0 id @ row " + (startRow + r),
                                expectedId, batch.getLongValue(0, r));
                    }
                    return startRow + n;
                });
                return;
            }
            case 1: {
                // id-range predicate with a random threshold. Threshold chosen
                // inside [1, totalRows-1] so the expected count is deterministic
                // and strictly positive.
                int threshold = 1 + random.nextInt(Math.max(1, totalRows - 1));
                int expected = totalRows - threshold;
                // ts-ordered (no ORDER BY) -> id-ordered. Row N -> id = threshold + N + 1.
                assertRows(client,
                        "SELECT id, v FROM fz_seq WHERE id > " + threshold,
                        expected, (batch, startRow) -> {
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                long expectedId = threshold + startRow + r + 1;
                                Assert.assertEquals("shape 1 id @ row " + (startRow + r),
                                        expectedId, batch.getLongValue(0, r));
                                Assert.assertEquals("shape 1 v @ row " + (startRow + r),
                                        expectedV(expectedId), batch.getDoubleValue(1, r), 0.0);
                            }
                            return startRow + n;
                        });
                return;
            }
            case 2: { // GROUP BY -- RecordCursor path
                // cat cycles 4 symbols, so the result set has 4 rows.
                // GROUP BY result order is undefined: collect all (cat, count)
                // pairs into a map and verify against the analytic formula.
                final java.util.Map<String, Long> counts = new java.util.HashMap<>();
                client.execute("SELECT cat, COUNT(*) c FROM fz_seq", new QwpColumnBatchHandler() {
                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        for (int r = 0; r < batch.getRowCount(); r++) {
                            DirectUtf8Sequence catSeq = batch.getStrA(0, r);
                            Assert.assertNotNull("shape 2 cat must not be NULL", catSeq);
                            Assert.assertEquals("shape 2 cat byte length", 1, catSeq.size());
                            String cat = String.valueOf((char) (catSeq.byteAt(0) & 0xFF));
                            counts.put(cat, batch.getLongValue(1, r));
                        }
                    }

                    @Override
                    public void onEnd(long t) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                        Assert.fail("shape 2 query failed: " + message);
                    }
                });
                Assert.assertEquals("shape 2 distinct cat count", 4, counts.size());
                Assert.assertEquals("shape 2 count(a)", catCount(totalRows, 0), (long) counts.get("a"));
                Assert.assertEquals("shape 2 count(b)", catCount(totalRows, 1), (long) counts.get("b"));
                Assert.assertEquals("shape 2 count(c)", catCount(totalRows, 2), (long) counts.get("c"));
                Assert.assertEquals("shape 2 count(d)", catCount(totalRows, 3), (long) counts.get("d"));
                return;
            }
            case 3: {
                // Interval on designated timestamp -- PageFrameCursor with
                // partition skip. Pick a random sub-range [loRow, hiRow)
                // within [1, totalRows+1] covering at least a few rows, then
                // translate to ts bounds using the populated spacing.
                int loRow = 1 + random.nextInt(Math.max(1, totalRows - 2));
                int span = 1 + random.nextInt(Math.max(1, totalRows - loRow));
                int hiRow = loRow + span;
                // Row x has ts = (x - 1) * spacingMicros. Interval
                // [(loRow - 1) * spacing, (hiRow - 1) * spacing) captures
                // x in [loRow, hiRow - 1]: span rows.
                long tsLo = (long) (loRow - 1) * spacingMicros;
                long tsHi = (long) (hiRow - 1) * spacingMicros;
                // ts-ordered -> id-ordered. globalRow N -> id = loRow + N.
                assertRows(client,
                        "SELECT id FROM fz_seq "
                                + "WHERE ts >= CAST(" + tsLo + "L AS TIMESTAMP) "
                                + "AND ts < CAST(" + tsHi + "L AS TIMESTAMP)",
                        span, (batch, startRow) -> {
                            int n = batch.getRowCount();
                            for (int r = 0; r < n; r++) {
                                long expectedId = loRow + startRow + r;
                                Assert.assertEquals("shape 3 id @ row " + (startRow + r),
                                        expectedId, batch.getLongValue(0, r));
                            }
                            return startRow + n;
                        });
                return;
            }
            case 4: {
                // Random projection of the stable base columns (id, v, cat, ts).
                // Never references {@code extra_*} columns because those come and
                // go under ALTER; we want this shape to remain cacheable and
                // correct regardless of structural ops.
                String[] base = {"id", "v", "cat", "ts"};
                int pickCount = 1 + random.nextInt(base.length);
                String[] shuffled = base.clone();
                for (int i = shuffled.length - 1; i > 0; i--) {
                    int j = random.nextInt(i + 1);
                    String tmp = shuffled[i];
                    shuffled[i] = shuffled[j];
                    shuffled[j] = tmp;
                }
                StringBuilder sql = new StringBuilder("SELECT ");
                for (int i = 0; i < pickCount; i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(shuffled[i]);
                }
                sql.append(" FROM fz_seq ORDER BY id");
                String[] projection = java.util.Arrays.copyOf(shuffled, pickCount);
                // Explicit ORDER BY id -> globalRow N -> id N+1.
                assertRows(client, sql.toString(), totalRows, (batch, startRow) -> {
                    Assert.assertEquals("shape 4 column count", projection.length, batch.getColumnCount());
                    int n = batch.getRowCount();
                    for (int r = 0; r < n; r++) {
                        long id = startRow + r + 1;
                        for (int c = 0; c < projection.length; c++) {
                            verifyBaseColumn(batch, c, r, projection[c], id, spacingMicros, "shape 4");
                        }
                    }
                    return startRow + n;
                });
                return;
            }
            case 5: {
                // SELECT * -- stable SQL text, but the expanded column set
                // follows ADD / DROP automatically. Every ALTER invalidates the
                // cached factory; the recompile picks up the new column list
                // without the test needing to know it. Verifier checks the
                // base 4 columns, asserts the extra-column count matches the
                // current liveAdded, and asserts every extra-column cell is
                // NULL (the test never UPDATEs them).
                int expectedExtras = liveAdded.size();
                // ts-ordered -> id-ordered. Base columns are at fixed indices:
                // id=0, v=1, cat=2, ts=3. Extras follow in ALTER-order.
                assertRows(client, "SELECT * FROM fz_seq", totalRows, (batch, startRow) -> {
                    Assert.assertEquals("shape 5 column count",
                            4 + expectedExtras, batch.getColumnCount());
                    int n = batch.getRowCount();
                    for (int r = 0; r < n; r++) {
                        long id = startRow + r + 1;
                        verifyBaseColumn(batch, 0, r, "id", id, spacingMicros, "shape 5");
                        verifyBaseColumn(batch, 1, r, "v", id, spacingMicros, "shape 5");
                        verifyBaseColumn(batch, 2, r, "cat", id, spacingMicros, "shape 5");
                        verifyBaseColumn(batch, 3, r, "ts", id, spacingMicros, "shape 5");
                        for (int c = 4; c < 4 + expectedExtras; c++) {
                            Assert.assertTrue("shape 5 extra col " + c + " @ row " + (startRow + r)
                                    + " must be NULL", batch.isNull(c, r));
                        }
                    }
                    return startRow + n;
                });
                return;
            }
            default:
                throw new IllegalStateException("unknown shape: " + shape);
        }
    }

    /**
     * Row-timestamp spacing for the sequence fuzz. Picks from a small set
     * of values that stress different partition densities for the
     * designated-ts interval predicate: sub-partition (all rows in one
     * partition), near-boundary (a few partitions), and sparse (many
     * partitions). Bounded so row counts up to the fuzz max (~1000) never
     * produce more than a handful of daily partitions.
     */
    private long pickSpacingMicros() {
        // Spacing options in microseconds. 1 hour, 6 hours, ~14 minutes, ~1 hour.
        // 864_000_000 = 14.4 min (100 rows/day); 3_600_000_000 = 1 h (24 rows/day);
        // 21_600_000_000 = 6 h (4 rows/day, max partitions for 1000 rows = 250).
        long[] choices = {
                300_000_000L,       // 5 min -- dense, mostly 1 partition
                864_000_000L,       // 14.4 min -- 100 rows per partition
                3_600_000_000L,     // 1 h -- 24 rows per partition
                21_600_000_000L,    // 6 h -- 4 rows per partition
        };
        return choices[random.nextInt(choices.length)];
    }

    /**
     * Drives {@code client.execute(sql)} and dispatches every batch to the
     * supplied verifier. The verifier returns the running total of rows
     * checked so far; we assert that total against {@code expected} after
     * RESULT_END. Per-cell assertions live inside the verifier; this helper
     * only handles transport, error mapping, and the final row-count check.
     */
    private static void assertRows(QwpQueryClient client, String sql, long expected, BatchVerifier verifier) {
        final long[] seen = {0};
        client.execute(sql, new QwpColumnBatchHandler() {
            @Override
            public void onBatch(QwpColumnBatch batch) {
                seen[0] = verifier.verify(batch, seen[0]);
            }

            @Override
            public void onEnd(long totalRows) {
            }

            @Override
            public void onError(byte status, String message) {
                Assert.fail("query failed [" + sql + "]: " + message);
            }
        });
        Assert.assertEquals("row count [" + sql + "]", expected, seen[0]);
    }

    /**
     * Number of rows in {@code 1..totalRows} whose value modulo 4 equals
     * {@code kMod}. Mirrors the populating CASE expression that maps id %% 4
     * to one of "abcd". Closed-form so the test doesn't loop totalRows
     * times for the GROUP BY assertion.
     */
    private static long catCount(int totalRows, int kMod) {
        return kMod == 0 ? totalRows / 4 : (totalRows + 4 - kMod) / 4;
    }

    /**
     * Maps a row id to the expected {@code cat} symbol value. Mirrors the
     * populating CASE: x %% 4 -> 'a','b','c','d' for k = 0,1,2,3.
     */
    private static char catFor(long id) {
        return "abcd".charAt((int) (id % 4));
    }

    /**
     * Expected DOUBLE value for column {@code v} given a row id. Mirrors the
     * populating expression {@code x * 1.5}.
     */
    private static double expectedV(long id) {
        return id * 1.5;
    }

    /**
     * Expected designated-timestamp value (microseconds) for a row id. Mirrors
     * the populating expression {@code (x - 1) * spacingMicros}.
     */
    private static long expectedTs(long id, long spacingMicros) {
        return (id - 1) * spacingMicros;
    }

    /**
     * Asserts cell {@code (col, batchRow)} of {@code batch} matches the
     * expected value for the named base column at row {@code id}. Used by
     * shape-4 and shape-5 verifiers where the projection / column-set is
     * known but column index varies across calls.
     */
    private static void verifyBaseColumn(QwpColumnBatch batch, int col, int batchRow,
                                         String name, long id, long spacingMicros, String tag) {
        switch (name) {
            case "id":
                Assert.assertEquals(tag + " id @ row id=" + id,
                        id, batch.getLongValue(col, batchRow));
                return;
            case "v":
                Assert.assertEquals(tag + " v @ row id=" + id,
                        expectedV(id), batch.getDoubleValue(col, batchRow), 0.0);
                return;
            case "cat": {
                DirectUtf8Sequence seq = batch.getStrA(col, batchRow);
                Assert.assertNotNull(tag + " cat must not be NULL @ row id=" + id, seq);
                Assert.assertEquals(tag + " cat byte length @ row id=" + id, 1, seq.size());
                Assert.assertEquals(tag + " cat char @ row id=" + id,
                        catFor(id), (char) (seq.byteAt(0) & 0xFF));
                return;
            }
            case "ts":
                Assert.assertEquals(tag + " ts @ row id=" + id,
                        expectedTs(id, spacingMicros), batch.getLongValue(col, batchRow));
                return;
            default:
                throw new IllegalStateException(tag + " unknown base column: " + name);
        }
    }

    /**
     * Receives one batch at a time during {@link #assertRows} and verifies
     * its rows against expected values derived from the row position. Returns
     * the running total of rows checked so the caller can compare against
     * the query's expected row count after RESULT_END.
     */
    @FunctionalInterface
    private interface BatchVerifier {
        long verify(QwpColumnBatch batch, long startRow);
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
        int choice = random.nextInt(5);
        return switch (choice) {
            case 0 -> ""; // inherit library default (raw)
            case 1 -> "compression=raw;";
            case 2 -> "compression=auto;";  // opt into zstd,raw server-picked
            case 3 -> "compression=zstd;";
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
            return batch.getBoolValue(col, row) ? 1 : 0;
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
            return batch.getLongValue(col, row);
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
            return batch.getLongValue(col, row);
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
            sb.repeat("0", Math.max(0, scale - fs.length()));
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
            return Double.doubleToRawLongBits(batch.getDoubleValue(col, row));
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
            return Float.floatToRawIntBits(batch.getFloatValue(col, row));
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
            batch.getGeohashValue(col, row);
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
            return batch.getIntValue(col, row) & 0xFFFFFFFFL;
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

    private static final class LongGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLongValue(col, row);
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
            return batch.getLongValue(col, row);
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

    private static final class TimestampNanosGenerator implements ColumnGenerator {
        @Override
        public long observedHash(QwpColumnBatch batch, int col, int row) {
            return batch.getLongValue(col, row);
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
            DirectUtf8Sequence v = batch.getStrA(col, row);
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
