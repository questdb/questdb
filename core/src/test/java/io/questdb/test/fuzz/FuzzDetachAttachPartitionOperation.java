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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableRecordMetadata;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.ops.AlterOperation;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.Rnd;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Detaches a partition and attaches it back. Between the two, it can run a statement that moves the partition the
 * columns were added at, which tells whether a partition without a column-top record holds a column: TRUNCATE, or a
 * DROP PARTITION of every partition from a given day on. The attach must keep reading a column added after the
 * partition was written as NULL there. Or it renames a column away and back, which moves the column's file version
 * on in the attached partitions only: the attach must keep reading the version the partition holds. While the column
 * is renamed away, it tries the ATTACH of a native partition, which must refuse the partition when it holds rows of the
 * column, and attach it otherwise. Or it renames a column away, then to its name in other letter case, which name
 * lookups take for its name, and back, with the same ATTACH while the column has the name in other letter case.
 * <p>
 * The test names the detached copy as ATTACH looks for it, see {@code cairo.attach.partition.suffix}, so a WAL table
 * applies the three statements in one go. A DETACH of a partition the table does not hold, or of its active
 * partition, fails, and so does the ATTACH then: both fail the same way on WAL and non-WAL tables, and a WAL table
 * skips them, so the operation goes on past them.
 */
public class FuzzDetachAttachPartitionOperation implements FuzzTransactionOperation {
    public static final int BETWEEN_DROP_NEWEST = 2;
    public static final int BETWEEN_NOTHING = 0;
    public static final int BETWEEN_RENAME_AWAY_AND_BACK = 3;
    public static final int BETWEEN_RENAME_CASE_ONLY_AND_BACK = 4;
    public static final int BETWEEN_TRUNCATE = 1;
    // the name a column takes while renamed away: this prefix and its name
    public static final String RENAMED_AWAY_PREFIX = "away_";
    // what the ATTACH while a column is renamed away did, see getRenamedAwayAttachOutcome()
    public static final int RENAMED_AWAY_ATTACH_ATTACHED = 2;
    public static final int RENAMED_AWAY_ATTACH_NOT_TRIED = 0;
    public static final int RENAMED_AWAY_ATTACH_REFUSED = 1;
    // what apply() reports for a statement
    private static final int APPLIED = 0;
    private static final int FAILED = 1;
    private static final int TABLE_GONE = 2;
    private final int between;
    // the first partition BETWEEN_DROP_NEWEST drops
    private final long dropFromTimestamp;
    // the partitioning of the table, which names the partition
    private final int partitionBy;
    private final long partitionTimestamp;
    // the column BETWEEN_RENAME_CASE_ONLY_AND_BACK renamed: its name, and its name in other letter case, which a failed
    // attempt of the operation can leave it with
    private String caseOnlyRenamedFrom;
    private String caseOnlyRenamedTo;
    private int renamedAwayAttachOutcome = RENAMED_AWAY_ATTACH_NOT_TRIED;

    public FuzzDetachAttachPartitionOperation(long partitionTimestamp, int between, long dropFromTimestamp) {
        this(partitionTimestamp, between, dropFromTimestamp, PartitionBy.DAY);
    }

    /**
     * Detaches the partition of a table partitioned by {@code partitionBy} that starts at {@code partitionTimestamp}.
     */
    public FuzzDetachAttachPartitionOperation(long partitionTimestamp, int between, long dropFromTimestamp, int partitionBy) {
        this.partitionTimestamp = partitionTimestamp;
        this.partitionBy = partitionBy;
        this.between = between;
        this.dropFromTimestamp = dropFromTimestamp;
    }

    @Override
    public boolean apply(Rnd rnd, CairoEngine engine, TableWriterAPI wApi, int virtualTimestampIndex, LongList excludedTsIntervals) {
        try (
                SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1);
                SqlCompiler compiler = engine.getSqlCompiler()
        ) {
            context.with(AllowAllSecurityContext.INSTANCE);
            final TableRecordMetadata metadata = wApi.getMetadata();
            final StringSink partitionName = new StringSink();
            PartitionBy.setSinkForPartition(partitionName, metadata.getTimestampType(), partitionBy, partitionTimestamp);
            final String tableName = TestUtils.randomiseCase(rnd, wApi.getTableToken().getTableName());
            final int detached = apply(compiler, context, wApi, "ALTER TABLE " + tableName + " DETACH PARTITION LIST '" + partitionName + '\'');
            if (detached == TABLE_GONE) {
                return true;
            }
            final String attachSql = "ALTER TABLE " + tableName + " ATTACH PARTITION LIST '" + partitionName + '\'';
            boolean isAttached = false;
            switch (between) {
                case BETWEEN_TRUNCATE -> wApi.truncateSoft();
                case BETWEEN_DROP_NEWEST -> {
                    final String dropSql = "ALTER TABLE " + tableName + " DROP PARTITION WHERE "
                            + FuzzTransactionOperation.partitionsFromFilter(metadata, dropFromTimestamp);
                    if (apply(compiler, context, wApi, dropSql) == TABLE_GONE) {
                        return true;
                    }
                }
                case BETWEEN_RENAME_AWAY_AND_BACK, BETWEEN_RENAME_CASE_ONLY_AND_BACK -> {
                    // The renames change no data, so the WAL table, which takes none, holds what the non-WAL table
                    // must read after the attach
                    if (!wApi.getTableToken().isWal()) {
                        // A Parquet partition finds its columns by their ids: the ATTACH while a column is renamed
                        // away is for native partitions
                        final boolean isAttachTried = detached == APPLIED && !hasDetachedParquetData(engine, wApi, partitionName);
                        isAttached = renameColumnAwayAndBack(rnd, compiler, context, wApi, tableName, isAttachTried ? attachSql : null);
                    }
                }
                default -> {
                    assert between == BETWEEN_NOTHING;
                }
            }
            if (!isAttached && apply(compiler, context, wApi, attachSql) == FAILED && detached == APPLIED) {
                // A WAL table reports neither: it applies the statements later, and skips them when they fail. A
                // partition a non-WAL table detached must attach back, or it loses the partition, and so would the WAL
                // table, unnoticed by the comparison of the two.
                throw new AssertionError("a detached partition did not attach back [table=" + tableName + ", partition=" + partitionName + ']');
            }
            return true;
        } catch (SqlException e) {
            throw new RuntimeException(e);
        }
    }

    public int getBetween() {
        return between;
    }

    public long getDropFromTimestamp() {
        return dropFromTimestamp;
    }

    public long getPartitionTimestamp() {
        return partitionTimestamp;
    }

    /**
     * What the last ATTACH while a column was renamed away, or renamed in letter case only, did:
     * RENAMED_AWAY_ATTACH_REFUSED for a partition that holds rows of the column, RENAMED_AWAY_ATTACH_ATTACHED for one
     * that holds none. RENAMED_AWAY_ATTACH_NOT_TRIED until the operation tries one.
     */
    public int getRenamedAwayAttachOutcome() {
        return renamedAwayAttachOutcome;
    }

    private static int apply(SqlCompiler compiler, SqlExecutionContextImpl context, TableWriterAPI wApi, String sql) throws SqlException {
        try {
            final AlterOperation alterOp = compiler.compile(sql, context).getAlterOperation();
            alterOp.withSqlStatement(sql);
            alterOp.withContext(context);
            wApi.apply(alterOp, false);
            return APPLIED;
        } catch (CairoException e) {
            if (Chars.contains(e.getFlyweightMessage(), "table does not exist") || e.isTableDropped()) {
                // a drop and create step of the parallel WAL apply recreates the table
                return TABLE_GONE;
            }
            // a partition the table does not hold, or its active one, which a WAL table skips too
            if (e.isWALTolerable()) {
                return FAILED;
            }
            throw e;
        }
    }

    // the name with the letter case of each letter flipped, which name lookups take for the name itself
    private static String flipCase(String name) {
        final StringBuilder flipped = new StringBuilder(name.length());
        for (int i = 0, n = name.length(); i < n; i++) {
            final char c = name.charAt(i);
            flipped.append(Character.isUpperCase(c) ? Character.toLowerCase(c) : Character.toUpperCase(c));
        }
        return flipped.toString();
    }

    // whether the detached copy of the partition, named as ATTACH looks for it, is a Parquet one
    private static boolean hasDetachedParquetData(CairoEngine engine, TableWriterAPI wApi, CharSequence partitionName) {
        final CairoConfiguration configuration = engine.getConfiguration();
        try (Path path = new Path()) {
            path.of(configuration.getDbRoot()).concat(wApi.getTableToken()).concat(partitionName)
                    .put(configuration.getAttachPartitionSuffix()).concat(TableUtils.PARQUET_PARTITION_NAME);
            return Files.exists(path.$());
        }
    }

    private static void rename(SqlCompiler compiler, SqlExecutionContextImpl context, TableWriterAPI wApi, String tableName, String from, String to) throws SqlException {
        final String sql = "ALTER TABLE " + tableName + " RENAME COLUMN " + from + " TO " + to;
        final AlterOperation alterOp = compiler.compile(sql, context).getAlterOperation();
        alterOp.withSqlStatement(sql);
        alterOp.withContext(context);
        wApi.apply(alterOp, true);
    }

    /**
     * Attaches the partition while the column is renamed to renamedName, away or in letter case only, and returns
     * whether the partition attached. The partition holds the column under its name from before the renames: the ATTACH
     * must refuse the partition when it holds rows of the column, and attach it otherwise, which reads the column as NULL
     * there, as the WAL table without the renames does. Any other failure, such as an injected IO failure, goes to the
     * run, which retries the operation.
     */
    private boolean attachWhileRenamedAway(
            SqlCompiler compiler,
            SqlExecutionContextImpl context,
            TableWriterAPI wApi,
            String attachSql,
            String renamedName
    ) throws SqlException {
        final TableRecordMetadata metadata = wApi.getMetadata();
        final String refusal = "Detached column [index=" + metadata.getWriterIndex(metadata.getColumnIndex(renamedName)) + ", name=" + renamedName
                + ", attribute=name] does not match current table metadata";
        final AlterOperation alterOp = compiler.compile(attachSql, context).getAlterOperation();
        alterOp.withSqlStatement(attachSql);
        alterOp.withContext(context);
        try {
            wApi.apply(alterOp, false);
            renamedAwayAttachOutcome = RENAMED_AWAY_ATTACH_ATTACHED;
            return true;
        } catch (CairoException e) {
            if (!Chars.equals(e.getFlyweightMessage(), refusal)) {
                throw e;
            }
            renamedAwayAttachOutcome = RENAMED_AWAY_ATTACH_REFUSED;
            return false;
        }
    }

    /**
     * Renames a column other than the designated timestamp away and back. The detached _cv names the version of its
     * files the partition holds: a column name txn, or none for a column from CREATE TABLE. BETWEEN_RENAME_CASE_ONLY_AND_BACK
     * picks a column with a letter in its name, and renames it from away to its name in other letter case, which RENAME
     * COLUMN refuses as a new name of the column itself, before the way back. A column that a failed attempt of this
     * operation left renamed away, or in letter case only, which the run retries, takes its name back first. The
     * designated timestamp keeps its name: an ATTACH after it was renamed fails on reading the partition's timestamps.
     *
     * @param attachSql the ATTACH to run while the column is renamed away, or in letter case only, or null for none
     * @return whether that ATTACH attached the partition
     */
    private boolean renameColumnAwayAndBack(
            Rnd rnd,
            SqlCompiler compiler,
            SqlExecutionContextImpl context,
            TableWriterAPI wApi,
            String tableName,
            @Nullable String attachSql
    ) throws SqlException {
        final TableRecordMetadata metadata = wApi.getMetadata();
        final ObjList<String> renamedAway = new ObjList<>();
        final ObjList<String> candidates = new ObjList<>();
        boolean isLeftRenamedInCaseOnly = false;
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (metadata.getColumnType(i) > 0 && i != metadata.getTimestampIndex()) {
                final String columnName = metadata.getColumnName(i);
                if (columnName.startsWith(RENAMED_AWAY_PREFIX)) {
                    renamedAway.add(columnName);
                } else if (columnName.equals(caseOnlyRenamedTo)) {
                    isLeftRenamedInCaseOnly = true;
                } else {
                    candidates.add(columnName);
                }
            }
        }
        for (int i = 0, n = renamedAway.size(); i < n; i++) {
            final String columnName = renamedAway.getQuick(i).substring(RENAMED_AWAY_PREFIX.length());
            rename(compiler, context, wApi, tableName, renamedAway.getQuick(i), columnName);
            candidates.add(columnName);
        }
        if (isLeftRenamedInCaseOnly) {
            rename(compiler, context, wApi, tableName, caseOnlyRenamedTo, RENAMED_AWAY_PREFIX + caseOnlyRenamedFrom);
            rename(compiler, context, wApi, tableName, RENAMED_AWAY_PREFIX + caseOnlyRenamedFrom, caseOnlyRenamedFrom);
            candidates.add(caseOnlyRenamedFrom);
        }
        if (between == BETWEEN_RENAME_CASE_ONLY_AND_BACK) {
            for (int i = candidates.size() - 1; i > -1; i--) {
                final String columnName = candidates.getQuick(i);
                final String flipped = flipCase(columnName);
                if (flipped.equals(columnName) || !Chars.equalsIgnoreCase(flipped, columnName)) {
                    // without a letter, or with one whose other case name lookups do not fold
                    candidates.remove(i);
                }
            }
        }
        boolean isAttached = false;
        if (candidates.size() > 0) {
            final String columnName = candidates.getQuick(rnd.nextInt(candidates.size()));
            final String awayName = RENAMED_AWAY_PREFIX + columnName;
            rename(compiler, context, wApi, tableName, columnName, awayName);
            if (between == BETWEEN_RENAME_CASE_ONLY_AND_BACK) {
                caseOnlyRenamedFrom = columnName;
                caseOnlyRenamedTo = flipCase(columnName);
                rename(compiler, context, wApi, tableName, awayName, caseOnlyRenamedTo);
                if (attachSql != null) {
                    isAttached = attachWhileRenamedAway(compiler, context, wApi, attachSql, caseOnlyRenamedTo);
                }
                rename(compiler, context, wApi, tableName, caseOnlyRenamedTo, awayName);
            } else if (attachSql != null) {
                isAttached = attachWhileRenamedAway(compiler, context, wApi, attachSql, awayName);
            }
            rename(compiler, context, wApi, tableName, awayName, columnName);
        }
        return isAttached;
    }
}
