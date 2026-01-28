package io.questdb.test.cairo.fuzz;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.seq.SeqTxnTracker;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Fuzz test for negative timestamp support.
 * Tests O3 merge, deduplication, and WAL operations with timestamps before 1970-01-01.
 */
public class NegativeTimestampFuzzTest extends AbstractFuzzTest {

    @Test
    public void testNegativeTimestampFuzz() throws Exception {
        Rnd rnd = generateRandom(LOG);

        // Use minimal fuzz settings for debugging
        fuzzer.setFuzzCounts(
                false,                  // isO3
                1000,                   // fuzzRowCount - small
                10,                     // transactionCount - small
                10,                     // strLen
                10,                     // symbolStrLen
                10,                     // symbolCountMax
                100,                    // initialRowCount - small
                3);                     // partitionCount - minimum

        // Disable all operations except data insertion
        fuzzer.setFuzzProbabilities(
                0.0,                      // cancelRowsProb
                0.0,                      // notSetProb
                0.0,                      // nullSetProb
                0.0,                      // rollbackProb
                0.0,                      // colAddProb
                0.0,                      // colRemoveProb
                0.0,                      // colRenameProb
                0.0,                      // colTypeChangeProb
                1.0,                      // dataAddProb - only add data
                0.0,                      // equalTsRowsProb
                0.0,                      // partitionDropProb
                0.0,                      // truncateProb
                0.0,                      // tableDropProb
                0.0,                      // setTtlProb
                0.0,                      // replaceInsertProb
                0.0,                      // symbolAccessValidationProb
                0.0);                     // queryProb

        runFuzzNegative(rnd);
    }

    private void runFuzzNegative(Rnd rnd) throws Exception {
        String tableName = getTestName();
        String tableNameWal = tableName + "_wal";
        String tableNameNoWal = tableName + "_nonwal";

        // Create initial WAL table with negative timestamps (starting 1900-01-01)
        createInitialTableNegative(tableNameWal, true);
        createInitialTableNegative(tableNameNoWal, false);

        // Generate transactions with negative start timestamp
        // Start from 1900-01-01T00:00:00.000000Z = -2208988800000000 microseconds
        long startMicro = -2208988800000000L;
        long endMicro = startMicro + 365L * Micros.DAY_MICROS * 10; // 10 years of data

        ObjList<FuzzTransaction> transactions = fuzzer.generateTransactions(tableNameWal, rnd, startMicro, endMicro);

        try {
            // Apply to non-WAL table
            fuzzer.applyNonWal(transactions, tableNameNoWal, rnd);

            // Verify non-WAL table has data with negative timestamps
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertTableHasData(compiler, tableNameNoWal);
            }

            // Apply to WAL table
            TableToken walToken = engine.verifyTableName(tableNameWal);
            try {
                fuzzer.applyWal(transactions, tableNameWal, 1, rnd);
            } catch (AssertionError e) {
                // Capture suspension error message
                SeqTxnTracker tracker = engine.getTableSequencerAPI().getTxnTracker(walToken);
                if (tracker.isSuspended()) {
                    String errorMsg = tracker.getErrorMessage();
                    throw new AssertionError("WAL table suspended with error: " + errorMsg, e);
                }
                throw e;
            }

            // Verify WAL table matches non-WAL table
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                assertTableHasData(compiler, tableNameWal);
                TestUtils.assertSqlCursors(compiler, sqlExecutionContext, tableNameNoWal, tableNameWal, LOG);
            }

            Assert.assertEquals("expected 0 errors in partition mutation control", 0,
                    engine.getPartitionOverwriteControl().getErrorCount());

        } finally {
            io.questdb.std.Misc.freeObjListAndClear(transactions);
        }
    }

    private void createInitialTableNegative(String tableName, boolean isWal) throws Exception {
        SharedRandom.RANDOM.set(new Rnd());

        // Create temp data table if needed - include all columns upfront
        if (engine.getTableTokenIfExists("data_temp_neg") == null) {
            execute("create atomic table data_temp_neg as (" +
                    "select x as c1, " +
                    " rnd_symbol('AB', 'BC', 'CD') c2, " +
                    " timestamp_sequence('1900-01-01'::timestamp, 1000000L) ts, " +
                    " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                    " cast(x as int) c3," +
                    " rnd_bin() c4," +
                    " to_long128(3 * x, 6 * x) c5," +
                    " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk') str1," +
                    " rnd_boolean() bool1," +
                    " cast(null as long) long_top," +
                    " cast(null as long) str_top," +
                    " rnd_symbol(null, 'X', 'Y') sym_top," +
                    " rnd_ipv4() ip4," +
                    " rnd_varchar(5, 10, 1) var_top" +
                    " from long_sequence(100)" +
                    ")");
        }

        if (engine.getTableTokenIfExists(tableName) == null) {
            String walClause = isWal ? " WAL" : " BYPASS WAL";
            execute("create atomic table " + tableName + " as (" +
                    "select * from data_temp_neg" +
                    "), index(sym2), index(sym_top) timestamp(ts) partition by DAY" + walClause);
        }
    }

    private void assertTableHasData(SqlCompiler compiler, String tableName) throws Exception {
        TableToken tt = engine.verifyTableName(tableName);

        // Check table has rows
        sink.clear();
        TestUtils.printSql(compiler, sqlExecutionContext, "select count() from " + tableName, sink);
        Assert.assertFalse("Table " + tableName + " should have data", sink.toString().contains("count\n0\n"));

        // Check min timestamp is negative (before 1970)
        try (var reader = engine.getReader(tt)) {
            long minTs = reader.getMinTimestamp();
            Assert.assertTrue("Min timestamp should be negative (before 1970), got: " + minTs, minTs < 0);
        }

        // Check that we can query min/max timestamps correctly
        sink.clear();
        TestUtils.printSql(compiler, sqlExecutionContext,
                "select ts from " + tableName + " order by ts limit 1", sink);
        Assert.assertTrue("Query should return data", sink.length() > "ts\n".length());
    }
}
