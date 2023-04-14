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
import io.questdb.std.Chars;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractGriffinTest;
import io.questdb.test.griffin.wal.fuzz.FuzzTransaction;
import io.questdb.test.griffin.wal.fuzz.FuzzTransactionGenerator;
import io.questdb.test.griffin.wal.fuzz.FuzzTransactionOperation;

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
    private int strLen;
    private int symbolCountMax;
    private int symbolStrLenMax;
    private int transactionCount;
    private double truncateProb;

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
                generateSymbols(rnd, rnd.nextInt(Math.max(1, symbolCountMax - 5)) + 5, symbolStrLenMax, tableName)
        );
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

    protected static void applyNonWal(ObjList<FuzzTransaction> transactions, String tableName) {
        try (TableWriterAPI writer = getWriter(tableName)) {
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
            }
        }
    }

    protected static int getRndO3PartitionSplit(Rnd rnd) {
        return MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN + rnd.nextInt(MAX_WAL_APPLY_O3_SPLIT_PARTITION_CEIL - MAX_WAL_APPLY_O3_SPLIT_PARTITION_MIN);
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
                ") timestamp(ts) partition by DAY " + (isWal ? "WAL" : "BYPASS WAL"));
        return engine.getTableToken(tableName1);
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

    protected void setFuzzProperties(long maxApplyTimePerTable, long splitPartitionThreshold) {
        node1.getConfigurationOverrides().setWalApplyTableTimeQuote(maxApplyTimePerTable);
        node1.getConfigurationOverrides().setPartitionO3SplitThreshold(splitPartitionThreshold);
    }

    protected void setRandomAppendPageSize(Rnd rnd) {
        int minPage = 18;
        dataAppendPageSize = 1L << (minPage + rnd.nextInt(22 - minPage)); // MAX page size 4Mb
        LOG.info().$("dataAppendPageSize=").$(dataAppendPageSize).$();
    }

}
