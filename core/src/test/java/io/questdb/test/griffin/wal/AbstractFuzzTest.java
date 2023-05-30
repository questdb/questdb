/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.griffin.wal;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.log.Log;
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.fuzz.FuzzTransaction;
import io.questdb.test.fuzz.FuzzTransactionGenerator;
import io.questdb.test.fuzz.FuzzTransactionOperation;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;

public class AbstractFuzzTest extends AbstractGriffinTest {
    protected final static int MAX_WAL_APPLY_O3_SPLIT_PARTITION_CEIL = 20000;
    protected final static int MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN = 200;
    protected final static int MAX_WAL_APPLY_TIME_PER_TABLE_CEIL = 250;
    protected int initialRowCount;
    protected int partitionCount;
    private double cancelRowsProb;
    private double colRenameProb;
    private double collAddProb;
    private double collRemoveProb;
    private double dataAddProb;
    private double equalTsRowsProb;
    private int fuzzRowCount;
    private boolean isO3;
    private double notSetProb;
    private double nullSetProb;
    private double rollbackProb;
    private long s0;
    private long s1;
    private int strLen;
    private int symbolCountMax;
    private int symbolStrLenMax;
    private int transactionCount;
    private double truncateProb;

    @Before
    public void clearSeeds() {
        s0 = 0;
        s1 = 0;
    }

    public Rnd generateRandom(Log log, long s0, long s1) {
        Rnd rnd = TestUtils.generateRandom(log, s0, s1);
        this.s0 = rnd.getSeed0();
        this.s1 = rnd.getSeed1();
        return rnd;
    }

    public Rnd generateRandom(Log log) {
        Rnd rnd = TestUtils.generateRandom(log);
        s0 = rnd.getSeed0();
        s1 = rnd.getSeed1();
        return rnd;
    }

    public ObjList<FuzzTransaction> generateSet(Rnd rnd, RecordMetadata metadata, long start, long end, String tableName) {
        return FuzzTransactionGenerator.generateSet(
                metadata,
                rnd,
                start,
                end,
                fuzzRowCount,
                transactionCount,
                isO3,
                cancelRowsProb,
                notSetProb,
                nullSetProb,
                rollbackProb,
                collAddProb,
                collRemoveProb,
                colRenameProb,
                dataAddProb,
                truncateProb,
                equalTsRowsProb,
                strLen,
                generateSymbols(rnd, rnd.nextInt(Math.max(1, symbolCountMax - 5)) + 5, symbolStrLenMax, tableName),
                3
        );
    }

    @After
    public void logSeeds() {
        if (this.s0 != 0 || this.s1 != 0) {
            LOG.info().$("random seeds: ").$(s0).$("L, ").$(s1).$('L').$();
            System.out.printf("random seeds: %dL, %dL%n", s0, s1);
        }
    }

    private static void checkIndexRandomValueScan(String expectedTableName, String actualTableName, Rnd rnd, long recordCount, String columnName) throws SqlException {
        long randomRow = rnd.nextLong(recordCount);
        sink.clear();
        TestUtils.printSql(compiler, sqlExecutionContext, "select \"" + columnName + "\" as a from " + expectedTableName + " limit " + randomRow + ", 1", sink);
        String prefix = "a\n";
        String randomValue = sink.length() > prefix.length() + 2 ? sink.subSequence(prefix.length(), sink.length() - 1).toString() : null;
        String indexedWhereClause = " where \"" + columnName + "\" = " + (randomValue == null ? "null" : "'" + randomValue + "'");
        LOG.info().$("checking random index with filter: ").$(indexedWhereClause).I$();
        String limit = ""; // For debugging
        TestUtils.assertSqlCursors(compiler, sqlExecutionContext, expectedTableName + indexedWhereClause + limit, actualTableName + indexedWhereClause + limit, LOG);
    }

    private static void reloadPartitions(TableReader rdr1) {
        if (rdr1.isActive()) {
            LOG.info().$("reloading partitions [table=").$(rdr1.getTableToken()).$(", txn=").$(rdr1.getTxn()).I$();
            for (int i = 0; i < rdr1.getPartitionCount(); i++) {
                rdr1.openPartition(i);
            }
        }
    }

    private static void reloadReader(Rnd reloadRnd, TableReader rdr1, CharSequence rdrId) {
        if (reloadRnd.nextBoolean()) {
            reloadPartitions(rdr1);
            LOG.info().$("releasing reader txn [rdr=").$(rdrId).$(", table=").$(rdr1.getTableToken()).$(", txn=").$(rdr1.getTxn()).I$();
            rdr1.goPassive();

            if (reloadRnd.nextBoolean()) {
                rdr1.goActive();
                LOG.info().$("acquired reader txn [rdr=").$(rdrId).$(", table=").$(rdr1.getTableToken()).$(", txn=").$(rdr1.getTxn()).I$();
            }
        }
    }

    private String[] generateSymbols(Rnd rnd, int totalSymbols, int strLen, String baseSymbolTableName) {
        String[] symbols = new String[totalSymbols];
        int symbolIndex = 0;

        try (TableReader reader = getReader(baseSymbolTableName)) {
            TableReaderMetadata metadata = reader.getMetadata();
            for (int i = 0; i < metadata.getColumnCount(); i++) {
                int columnType = metadata.getColumnType(i);
                if (ColumnType.isSymbol(columnType)) {
                    SymbolMapReader symbolReader = reader.getSymbolMapReader(i);
                    for (int sym = 0; symbolIndex < totalSymbols && sym < symbolReader.getSymbolCount() - 1; sym++) {
                        symbols[symbolIndex++] = Chars.toString(symbolReader.valueOf(sym));
                    }
                }
            }
        }

        for (; symbolIndex < totalSymbols; symbolIndex++) {
            symbols[symbolIndex] = strLen > 0 ? Chars.toString(rnd.nextChars(rnd.nextInt(strLen))) : "";
        }
        return symbols;
    }

    protected static void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName, Rnd reloadRnd) {

        try (
                TableReader rdr1 = getReader(tableName);
                TableReader rdr2 = getReader(tableName);
                TableWriter writer = getWriter(tableName);
                O3PartitionPurgeJob purgeJob = new O3PartitionPurgeJob(engine.getMessageBus(), 1)
        ) {
            int transactionSize = transactions.size();
            Rnd rnd = new Rnd();
            for (int i = 0; i < transactionSize; i++) {
                FuzzTransaction transaction = transactions.getQuick(i);
                int size = transaction.operationList.size();
                for (int operationIndex = 0; operationIndex < size; operationIndex++) {
                    FuzzTransactionOperation operation = transaction.operationList.getQuick(operationIndex);
                    operation.apply(rnd, writer, -1);
                }

                if (transaction.rollback) {
                    writer.rollback();
                } else {
                    writer.commit();
                }
                purgeAndReloadReaders(reloadRnd, rdr1, rdr2, purgeJob, 0.25);
            }
        }
    }

    protected static int getRndO3PartitionSplit(Rnd rnd) {
        return MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN + rnd.nextInt(MAX_WAL_APPLY_O3_SPLIT_PARTITION_CEIL - MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN);
    }

    protected static int getRndO3PartitionSplitMaxCount(Rnd rnd) {
        return 1 + rnd.nextInt(2);
    }

    protected static void purgeAndReloadReaders(Rnd reloadRnd, TableReader rdr1, TableReader rdr2, O3PartitionPurgeJob purgeJob, double realoadThreashold) {
        if (reloadRnd.nextDouble() < realoadThreashold) {
            purgeJob.run(0);
            reloadReader(reloadRnd, rdr1, "1");
            reloadReader(reloadRnd, rdr2, "2");
        }
    }

    protected void assertRandomIndexes(String tableNameNoWal, String tableNameWal, Rnd rnd) throws SqlException {
        try (TableReader reader = newTableReader(configuration, tableNameNoWal)) {
            if (reader.size() > 0) {
                TableReaderMetadata metadata = reader.getMetadata();
                for (int columnIndex = 0; columnIndex < metadata.getColumnCount(); columnIndex++) {
                    if (ColumnType.isSymbol(metadata.getColumnType(columnIndex))
                            && metadata.isColumnIndexed(columnIndex)) {
                        checkIndexRandomValueScan(tableNameNoWal, tableNameWal, rnd, reader.size(), metadata.getColumnName(columnIndex));
                    }
                }
            }
        }
    }

    protected TableToken createInitialTable(String tableName1, boolean isWal, int rowCount) throws SqlException {
        SharedRandom.RANDOM.set(new Rnd());
        compile("create table " + tableName1 + " as (" +
                "select x as c1, " +
                " rnd_symbol('AB', 'BC', 'CD') c2, " +
                " timestamp_sequence('2022-02-24', 1000000L) ts, " +
                " rnd_symbol('DE', null, 'EF', 'FG') sym2," +
                " cast(x as int) c3," +
                " rnd_bin() c4," +
                " to_long128(3 * x, 6 * x) c5," +
                " rnd_str('a', 'bdece', null, ' asdflakji idid', 'dk')," +
                " rnd_boolean() bool1 " +
                " from long_sequence(" + rowCount + ")" +
                "), index(sym2) timestamp(ts) partition by DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        // force few column tops
        compile("alter table " + tableName1 + " add column long_top long");
        compile("alter table " + tableName1 + " add column str_top long");
        compile("alter table " + tableName1 + " add column sym_top symbol index");

        return engine.verifyTableName(tableName1);
    }

    protected void setFuzzCounts(boolean isO3, int fuzzRowCount, int transactionCount, int strLen, int symbolStrLenMax, int symbolCountMax, int initialRowCount, int partitionCount) {
        this.isO3 = isO3;
        this.fuzzRowCount = fuzzRowCount;
        this.transactionCount = transactionCount;
        this.strLen = strLen;
        this.symbolStrLenMax = symbolStrLenMax;
        this.symbolCountMax = symbolCountMax;
        this.initialRowCount = initialRowCount;
        this.partitionCount = partitionCount;
    }

    protected void setFuzzProbabilities(double cancelRowsProb, double notSetProb, double nullSetProb, double rollbackProb, double collAddProb, double collRemoveProb, double colRenameProb, double dataAddProb, double truncateProb, double equalTsRowsProb) {
        this.cancelRowsProb = cancelRowsProb;
        this.notSetProb = notSetProb;
        this.nullSetProb = nullSetProb;
        this.rollbackProb = rollbackProb;
        this.collAddProb = collAddProb;
        this.collRemoveProb = collRemoveProb;
        this.colRenameProb = colRenameProb;
        this.dataAddProb = dataAddProb;
        this.truncateProb = truncateProb;
        this.equalTsRowsProb = equalTsRowsProb;
    }

    protected void setFuzzProperties(long maxApplyTimePerTable, long splitPartitionThreshold, int o3PartitionSplitMaxCount) {
        node1.getConfigurationOverrides().setWalApplyTableTimeQuote(maxApplyTimePerTable);
        node1.getConfigurationOverrides().setPartitionO3SplitThreshold(splitPartitionThreshold);
        node1.getConfigurationOverrides().setO3PartitionSplitMaxCount(o3PartitionSplitMaxCount);
    }

    protected void setRandomAppendPageSize(Rnd rnd) {
        int minPage = 18;
        dataAppendPageSize = 1L << (minPage + rnd.nextInt(22 - minPage)); // MAX page size 4Mb
        LOG.info().$("dataAppendPageSize=").$(dataAppendPageSize).$();
    }

}
