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

package io.questdb.test.griffin.fuzz;

import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.griffin.fuzz.types.FuzzColumnType;
import io.questdb.test.griffin.fuzz.types.FuzzColumnTypes;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Builds a random WAL table and a sibling shadow table holding the same
 * data but with independently random storage settings. Timestamps step
 * forward so a configurable chunk of rows spans multiple DAY partitions.
 * <p>
 * Every table carries at least one SYMBOL column named {@code sym} so a
 * join fuzzer has a predictable key to target. The designated timestamp
 * column is always the last column and is named {@code ts}.
 * <p>
 * Each of the two siblings independently picks one of three parquet
 * modes uniformly at random: {@link ParquetMode#NONE} keeps every
 * partition native; {@link ParquetMode#ALL} converts every non-active
 * partition; {@link ParquetMode#PARTIAL} flips an independent coin per
 * non-active partition so the resulting table holds a mix of native and
 * parquet partitions. With {@link #INDEXED_SYMBOL_CHANCE} probability
 * each SYMBOL column is created with {@code INDEX} so query paths that
 * take the indexed branch (key scan, filtered index reader) are
 * exercised. Independent draws on the shadow mean a query that reads
 * from the primary (random storage) and the same query rewritten to
 * read from the shadow (different random storage) must agree -- any
 * silent storage divergence surfaces as a row-set mismatch.
 */
public final class FuzzTableFactory {
    private static final long DAY_MICROS = 24L * 60L * 60L * 1_000_000L;
    private static final double INDEXED_SYMBOL_CHANCE = 0.2;
    private static final String JOIN_KEY_COLUMN = "sym";
    private static final double PARTIAL_PARQUET_PARTITION_CHANCE = 0.5;
    private static final String TS_COLUMN = "ts";

    private final FuzzConfig config;

    public FuzzTableFactory(FuzzConfig config) {
        this.config = config;
    }

    public FuzzTable create(Rnd rnd, String primaryName, SqlExecutor executor, Runnable drainWal) throws SqlException {
        String shadowName = primaryName + "_shadow";
        ObjList<FuzzColumn> primaryColumns = buildColumnList(rnd);
        ObjList<FuzzColumn> shadowColumns = mirrorColumns(rnd, primaryColumns);

        executor.execute(buildCreateDdl(primaryName, primaryColumns));
        executor.execute(buildCreateDdl(shadowName, shadowColumns));

        // Generate the random data on the primary, drain so it's committed,
        // then copy onto the shadow via SELECT * FROM primary so both tables
        // hold byte-identical content. Doing it the other way (running the
        // rnd_* insert on each independently) would draw two different
        // random sequences and break the row-set comparison.
        executor.execute(buildInsertDml(primaryName, primaryColumns));
        drainWal.run();
        executor.execute("INSERT INTO " + shadowName + " SELECT * FROM " + primaryName);

        // Convert statements are queued behind the shadow insert in the WAL
        // queue and processed by the second drain.
        ParquetMode primaryMode = applyParquetConversion(rnd, primaryName, executor);
        ParquetMode shadowMode = applyParquetConversion(rnd, shadowName, executor);
        drainWal.run();

        FuzzTable shadow = new FuzzTable(shadowName, shadowColumns, TS_COLUMN, shadowMode, null);
        return new FuzzTable(primaryName, primaryColumns, TS_COLUMN, primaryMode, shadow);
    }

    private ParquetMode applyParquetConversion(Rnd rnd, String tableName, SqlExecutor executor) throws SqlException {
        ParquetMode mode = pickParquetMode(rnd);
        switch (mode) {
            case NONE -> {
            }
            case ALL -> executor.execute("ALTER TABLE " + tableName
                    + " CONVERT PARTITION TO PARQUET WHERE " + TS_COLUMN + " >= 0");
            case PARTIAL -> {
                String partitionList = pickPartialPartitionList(rnd);
                if (partitionList != null) {
                    executor.execute("ALTER TABLE " + tableName
                            + " CONVERT PARTITION TO PARQUET LIST " + partitionList);
                } else {
                    mode = ParquetMode.NONE;
                }
            }
        }
        return mode;
    }

    private ObjList<FuzzColumn> buildColumnList(Rnd rnd) {
        int numExtra = config.getMinColumnsPerTable()
                + rnd.nextInt(config.getMaxColumnsPerTable() - config.getMinColumnsPerTable() + 1);
        ObjList<FuzzColumn> columns = new ObjList<>();

        // Shared join key. Always SYMBOL so ASOF/LT/SPLICE on (sym) has a target.
        columns.add(new FuzzColumn(JOIN_KEY_COLUMN, SymbolType.INSTANCE, isIndexedSymbol(rnd)));

        for (int i = 0; i < numExtra; i++) {
            FuzzColumnType type = FuzzColumnTypes.pickRandom(rnd);
            boolean isIndexed = type == SymbolType.INSTANCE && isIndexedSymbol(rnd);
            columns.add(new FuzzColumn("c" + i, type, isIndexed));
        }

        // Designated timestamp last.
        columns.add(new FuzzColumn(TS_COLUMN, TimestampType.INSTANCE));
        return columns;
    }

    private String buildCreateDdl(String tableName, ObjList<FuzzColumn> columns) {
        StringSink ddl = new StringSink();
        ddl.put("CREATE TABLE ").put(tableName).put(" (");
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                ddl.put(", ");
            }
            FuzzColumn c = columns.getQuick(i);
            ddl.put(c.getName()).put(' ').put(c.getType().getDdl());
            if (c.isIndexed()) {
                ddl.put(" INDEX");
            }
        }
        ddl.put(") TIMESTAMP(").put(TS_COLUMN).put(") PARTITION BY DAY WAL");
        return ddl.toString();
    }

    private String buildInsertDml(String tableName, ObjList<FuzzColumn> columns) {
        StringSink dml = new StringSink();
        dml.put("INSERT INTO ").put(tableName).put(" SELECT ");
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                dml.put(", ");
            }
            FuzzColumn c = columns.getQuick(i);
            // The ts column uses a sequence instead of the type's rnd_call so
            // rows are monotonically ordered and spread across partitions.
            if (c.getName().equals(TS_COLUMN)) {
                dml.put("timestamp_sequence(to_timestamp('").put(config.getTsStart())
                        .put("', 'yyyy-MM-dd'), ").put(config.getStepMicros()).put("L)");
            } else {
                dml.put(c.getType().getRndCall());
            }
            dml.put(' ').put(c.getName());
        }
        dml.put(" FROM long_sequence(").put(config.getRowsPerTable()).put(')');
        return dml.toString();
    }

    private boolean isIndexedSymbol(Rnd rnd) {
        return rnd.nextDouble() < INDEXED_SYMBOL_CHANCE;
    }

    /**
     * Mirrors a column list -- same names and types -- with index flags
     * drawn independently so the shadow can have a different indexed-symbol
     * shape than the primary.
     */
    private ObjList<FuzzColumn> mirrorColumns(Rnd rnd, ObjList<FuzzColumn> source) {
        ObjList<FuzzColumn> out = new ObjList<>();
        for (int i = 0, n = source.size(); i < n; i++) {
            FuzzColumn c = source.getQuick(i);
            boolean isIndexed = c.getType() == SymbolType.INSTANCE && isIndexedSymbol(rnd);
            out.add(new FuzzColumn(c.getName(), c.getType(), isIndexed));
        }
        return out;
    }

    /**
     * Returns the partition list to convert for a {@link ParquetMode#PARTIAL}
     * table (or {@code null} when the coin landed on no partition). The list
     * holds non-active partitions only -- the active partition is skipped by
     * the convert path -- and is rendered as comma-separated quoted ISO dates
     * suitable for {@code CONVERT PARTITION TO PARQUET LIST ...}.
     */
    private String pickPartialPartitionList(Rnd rnd) {
        long totalMicros = config.getStepMicros() * (long) config.getRowsPerTable();
        int numPartitions = (int) (totalMicros / DAY_MICROS) + 1;
        if (numPartitions <= 1) {
            return null;
        }
        LocalDate start = LocalDate.parse(config.getTsStart(), DateTimeFormatter.ISO_LOCAL_DATE);
        StringSink sink = new StringSink();
        int picked = 0;
        for (int i = 0; i < numPartitions - 1; i++) {
            if (rnd.nextDouble() < PARTIAL_PARQUET_PARTITION_CHANCE) {
                if (picked > 0) {
                    sink.put(", ");
                }
                sink.put('\'').put(start.plusDays(i).toString()).put('\'');
                picked++;
            }
        }
        return picked == 0 ? null : sink.toString();
    }

    private ParquetMode pickParquetMode(Rnd rnd) {
        return ParquetMode.values()[rnd.nextInt(ParquetMode.values().length)];
    }

    @FunctionalInterface
    public interface SqlExecutor {
        void execute(String sql) throws SqlException;
    }

    public enum ParquetMode {NONE, ALL, PARTIAL}
}
