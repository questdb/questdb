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
import io.questdb.test.griffin.fuzz.types.ColumnKind;
import io.questdb.test.griffin.fuzz.types.FuzzColumnType;
import io.questdb.test.griffin.fuzz.types.FuzzColumnTypes;
import io.questdb.test.griffin.fuzz.types.IntKeyType;
import io.questdb.test.griffin.fuzz.types.LongKeyType;
import io.questdb.test.griffin.fuzz.types.LongType;
import io.questdb.test.griffin.fuzz.types.ShortType;
import io.questdb.test.griffin.fuzz.types.SymbolType;
import io.questdb.test.griffin.fuzz.types.TimestampType;
import io.questdb.test.griffin.fuzz.types.VarcharKeyType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Builds a random WAL table and a sibling shadow table holding the same
 * data but with independently random storage settings. Timestamps step
 * forward so a configurable chunk of rows spans multiple DAY partitions.
 * <p>
 * Every table carries four join-key columns over small domains, so a join
 * fuzzer has a predictable key of each shape the fused hash join GROUP BY
 * takes: the SYMBOL column {@code sym} and the INT column {@code k}, which
 * the narrow INT layout reads, plus the LONG column {@code lk} (see
 * {@link LongKeyType}) and the VARCHAR column {@code vk} (see
 * {@link VarcharKeyType}), which reach the map-backed build through the key
 * sinks. {@code vk} holds {@code sym}'s texts, so the two also join each
 * other. The designated timestamp column is named {@code ts}; it is the last
 * column but for the late columns of a table with column tops, which follow
 * it.
 * <p>
 * The primary also takes a storage shape of its own (see {@link FuzzStorage}
 * and {@code pickStorage()}): a non-WAL or a non-partitioned table, column
 * tops of some columns, and a column dropped after its first rows. The
 * shadow stays a partitioned WAL table that holds every column from its
 * first row, in the primary's column order.
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
 * <p>
 * With {@link #COMPOSITE_PARTITION_CHANCE} probability the primary backdates
 * a handful of rows into one of its own non-active days, landing between
 * that day's existing timestamps. With merge-append enabled (see
 * {@code QueryFuzzTest#runFuzz}) this leaves the day a real composite
 * partition -- a relocated piece and dead space, not a simulated one. This
 * runs only on the primary, before the shadow copies its data across: both
 * siblings end up with identical rows, so the always-plain shadow gives the
 * diff-shadow oracle a genuine composite-vs-plain contrast for free, the
 * same way it already contrasts parquet-vs-native and indexed-vs-not.
 */
public final class FuzzTableFactory {
    private static final long DAY_MICROS = 24L * 60L * 60L * 1_000_000L;
    // Per-table chance that the primary gets a merge-append-driven composite
    // partition (see the class javadoc).
    private static final double COMPOSITE_PARTITION_CHANCE = 0.5;
    // SYMBOL columns are relatively rare and there are four index kinds
    // (bitmap plus three posting variants), so a higher rate keeps every kind,
    // and the primary-vs-shadow index contrast, well represented.
    private static final double INDEXED_SYMBOL_CHANCE = 0.5;
    // Drawn uniformly when a SYMBOL column gets indexed, so bitmap and the
    // three posting variants all get exercised over a run.
    private static final FuzzIndex.Kind[] INDEX_KINDS = FuzzIndex.Kind.values();
    private static final String INT_JOIN_KEY_COLUMN = "k";
    private static final String JOIN_KEY_COLUMN = "sym";
    private static final String LONG_JOIN_KEY_COLUMN = "lk";
    private static final double PARTIAL_PARQUET_PARTITION_CHANCE = 0.5;
    // Per-posting-index chance of attaching a covering INCLUDE list.
    private static final double POSTING_COVERING_CHANCE = 0.5;
    private static final int POSTING_MAX_COVERING_COLUMNS = 3;
    // Per-table chances of the primary's storage shapes, see pickStorage().
    private static final double COLUMN_TOP_CHANCE = 0.4;
    private static final double DROPPED_COLUMN_CHANCE = 0.25;
    private static final double LATE_COLUMN_CHANCE = 0.3;
    private static final double LATE_JOIN_KEY_CHANCE = 0.5;
    private static final double NON_PARTITIONED_CHANCE = 0.2;
    private static final double NON_WAL_CHANCE = 0.2;
    // The day before the run's start whose rows pad a primary's sym dictionary, see pickStorage().
    private static final String PADDING_DAY = "2023-12-01";
    private static final double PADDED_JOIN_KEY_DICTIONARY_CHANCE = 0.2;
    private static final long STORAGE_SEED_SALT = 0x5707_a6e5_c01d_70f5L;
    private static final String TS_COLUMN = "ts";
    private static final String VARCHAR_JOIN_KEY_COLUMN = "vk";

    private final FuzzConfig config;
    // One shuffled deck for the whole run, dealt across every table the run
    // builds rather than reshuffled per table. A run with a single narrow
    // table still cannot hold every type, but two or three tables between them
    // now cover most of the registry instead of drawing the same handful of
    // popular types over and over.
    private final ObjList<FuzzColumnType> typeDeck = new ObjList<>();
    private int tablesCreated;
    private int typeDeckCursor;

    public FuzzTableFactory(FuzzConfig config) {
        this.config = config;
    }

    /**
     * Creates the run's next table, which holds {@link FuzzConfig#getTableRows(int)} rows for its
     * index among the tables this factory created, and its shadow.
     */
    public FuzzTable create(Rnd rnd, String primaryName, SqlExecutor executor, Runnable drainWal) throws SqlException {
        final int tableIndex = tablesCreated++;
        final int rows = config.getTableRows(tableIndex);
        String shadowName = primaryName + "_shadow";
        ObjList<FuzzColumn> columns = buildColumnList(rnd);
        ObjList<FuzzColumn> mirroredColumns = mirrorColumns(rnd, columns);
        // The primary's storage shape, drawn from a generator of its own (see pickStorage()), and
        // both tables' columns in the order the primary ends up with: the late columns, which
        // ALTER TABLE ADD COLUMN appends, after the others.
        final FuzzStorage storage = pickStorage(rnd, tableIndex, columns, mirroredColumns, rows);
        ObjList<FuzzColumn> primaryColumns = inStorageOrder(columns, storage);
        ObjList<FuzzColumn> shadowColumns = inStorageOrder(mirroredColumns, storage);

        // The shadow holds every column, in the primary's order, before its first row.
        executor.execute(buildCreateDdl(shadowName, earlyColumns(shadowColumns, storage, false), true, true));
        addLateColumns(shadowName, shadowColumns, storage, executor);

        // Generate the random data on the primary, drain so it's committed,
        // then copy onto the shadow via SELECT * FROM primary so both tables
        // hold byte-identical content. Doing it the other way (running the
        // rnd_* insert on each independently) would draw two different
        // random sequences and break the row-set comparison.
        if (storage.hasColumnTops() || storage.hasDroppedColumn()) {
            // The first rows, before the late columns exist and while the dropped column does; the
            // rest continue the timestamps, in the partition the first rows left off in, which then
            // holds column tops in its middle, and in the later ones.
            final ObjList<FuzzColumn> createColumns = earlyColumns(primaryColumns, storage, true);
            executor.execute(buildCreateDdl(primaryName, createColumns, storage.isWal(), storage.isPartitioned()));
            executor.execute(buildInsertDml(primaryName, createColumns, storage.firstChunkRows(), 0));
            drainWal.run();
            if (storage.hasDroppedColumn()) {
                executor.execute("ALTER TABLE " + primaryName + " DROP COLUMN " + FuzzStorage.DROPPED_COLUMN);
            }
            addLateColumns(primaryName, primaryColumns, storage, executor);
            executor.execute(buildInsertDml(primaryName, primaryColumns, rows - storage.firstChunkRows(), storage.firstChunkRows()));
        } else {
            executor.execute(buildCreateDdl(primaryName, primaryColumns, storage.isWal(), storage.isPartitioned()));
            executor.execute(buildInsertDml(primaryName, primaryColumns, rows, 0));
        }
        drainWal.run();
        if (storage.isJoinKeyDictionaryPadded()) {
            // Rows of distinct sym texts in a day before the run's start, which the table drops: their
            // texts stay in the dictionary, and the shadow, which copies the table's rows, lacks them.
            executor.execute("INSERT INTO " + primaryName + " (" + JOIN_KEY_COLUMN + ", " + TS_COLUMN + ") SELECT 'pad' || x, '"
                    + PADDING_DAY + "'::TIMESTAMP + x FROM long_sequence(" + FuzzStorage.PADDING_TEXT_COUNT + ")");
            drainWal.run();
            executor.execute("ALTER TABLE " + primaryName + " DROP PARTITION LIST '" + PADDING_DAY + "'");
            drainWal.run();
        }

        // Composite-partition backdate, primary only and before the shadow copy
        // (see the class javadoc) -- both siblings still end up with identical rows.
        // A table of one partition, which takes no rows out of order, draws as the others do and
        // backdates nothing.
        String compositeDay = applyCompositePartition(rnd, primaryName, primaryColumns, rows, storage.isPartitioned() ? executor : null);
        if (compositeDay != null) {
            drainWal.run();
        }

        executor.execute("INSERT INTO " + shadowName + " SELECT * FROM " + primaryName);

        // Convert statements are queued behind the shadow insert in the WAL
        // queue and processed by the second drain. A table of one partition
        // draws its conversion as the others do, so that the draws stay the
        // same, and converts nothing.
        ParquetConversion primaryParquet = applyParquetConversion(rnd, primaryName, rows, storage.isPartitioned() ? executor : null);
        ParquetConversion shadowParquet = applyParquetConversion(rnd, shadowName, rows, executor);
        drainWal.run();

        FuzzTable shadow = new FuzzTable(shadowName, shadowColumns, TS_COLUMN,
                shadowParquet.mode, shadowParquet.partitions, null, null);
        return new FuzzTable(primaryName, primaryColumns, TS_COLUMN,
                primaryParquet.mode, primaryParquet.partitions, compositeDay, shadow, storage);
    }

    // Appends the late columns to a table, which then holds them as column tops of its rows so far.
    private static void addLateColumns(String tableName, ObjList<FuzzColumn> columns, FuzzStorage storage, SqlExecutor executor) throws SqlException {
        for (int i = 0, n = columns.size(); i < n; i++) {
            final FuzzColumn c = columns.getQuick(i);
            if (storage.isLate(c.getName())) {
                executor.execute("ALTER TABLE " + tableName + " ADD COLUMN " + c.getName() + ' ' + c.getType().getDdl());
            }
        }
    }

    // The columns that CREATE TABLE makes: all but the late ones, and, for a primary that drops a
    // column, that column at its position.
    private static ObjList<FuzzColumn> earlyColumns(ObjList<FuzzColumn> columns, FuzzStorage storage, boolean isPrimary) {
        final ObjList<FuzzColumn> early = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            final FuzzColumn c = columns.getQuick(i);
            if (!storage.isLate(c.getName())) {
                early.add(c);
            }
        }
        if (isPrimary && storage.hasDroppedColumn()) {
            final ObjList<FuzzColumn> withDropped = new ObjList<>();
            for (int i = 0, n = early.size(); i < n; i++) {
                if (i == storage.droppedColumnPos()) {
                    withDropped.add(new FuzzColumn(FuzzStorage.DROPPED_COLUMN, LongType.INSTANCE));
                }
                withDropped.add(early.getQuick(i));
            }
            return withDropped;
        }
        return early;
    }

    // The columns in the order that the primary ends up with: the late ones after the others.
    private static ObjList<FuzzColumn> inStorageOrder(ObjList<FuzzColumn> columns, FuzzStorage storage) {
        final ObjList<FuzzColumn> ordered = new ObjList<>();
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final FuzzColumn c = columns.getQuick(i);
                if (storage.isLate(c.getName()) == (pass == 1)) {
                    ordered.add(c);
                }
            }
        }
        return ordered;
    }

    // Whether any index of the columns, of the primary or of its shadow, is on the column or covers it.
    private static boolean isIndexedOrCovered(String name, ObjList<FuzzColumn> columns) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            final FuzzColumn c = columns.getQuick(i);
            if (c.isIndexed()) {
                if (c.getName().equals(name)) {
                    return true;
                }
                final ObjList<String> covering = c.getIndex().coveringColumns();
                for (int j = 0, m = covering != null ? covering.size() : 0; j < m; j++) {
                    if (covering.getQuick(j).equals(name)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // Drawn last, so that a seed keeps the shapes it drew before this one existed. Only a partitioned
    // table can drop the day of its padding rows.
    private static boolean isJoinKeyDictionaryPadded(Rnd storageRnd, boolean isPartitioned) {
        return storageRnd.nextDouble() < PADDED_JOIN_KEY_DICTIONARY_CHANCE && isPartitioned;
    }

    /**
     * Draws the primary's storage shape (see {@link FuzzStorage}) from a generator seeded off the
     * run's one, which this draws nothing from, so that a seed replays the same queries whatever
     * shape its tables take. A table becomes, each with its own chance:
     * <ul>
     *   <li>a non-WAL or a non-partitioned table, see {@link #NON_WAL_CHANCE} and
     *   {@link #NON_PARTITIONED_CHANCE};</li>
     *   <li>a table with column tops, {@link #COLUMN_TOP_CHANCE}: some of its columns, the join keys
     *   more often, come after its first rows;</li>
     *   <li>a table that drops a column after its first rows, {@link #DROPPED_COLUMN_CHANCE};</li>
     *   <li>a partitioned table whose {@code sym} dictionary holds 60,000 more texts, of a day that it
     *   dropped, {@link #PADDED_JOIN_KEY_DICTIONARY_CHANCE}: a join that probes it reads fewer than
     *   one row per 256 of the texts, and translates its SYMBOL key through the workers' own caches,
     *   which its plain shadow's join does not.</li>
     * </ul>
     * A late column is neither indexed, nor covered by an index, on either table, so both tables
     * create their indexes up front. Nor is it a SHORT or a temporal column: the vectorized GROUP BY
     * reads a SHORT column top as no value where it reads an explicit value of 0, and its hour() keys
     * put an explicit NULL timestamp in another group than a column top. -Dquestdb.fuzz.storage=false
     * keeps every table a plain one.
     */
    private FuzzStorage pickStorage(Rnd rnd, int tableIndex, ObjList<FuzzColumn> columns, ObjList<FuzzColumn> mirroredColumns, int rows) {
        if (!config.isStorageAxesEnabled()) {
            return FuzzStorage.PLAIN;
        }
        final Rnd storageRnd = new Rnd(rnd.getSeed0() ^ STORAGE_SEED_SALT, rnd.getSeed1() + tableIndex);
        // Rnd's first draws are biased, see TestUtils.generateRandom().
        storageRnd.nextBoolean();
        storageRnd.nextBoolean();
        final double layout = storageRnd.nextDouble();
        final boolean isPartitioned = layout >= NON_PARTITIONED_CHANCE;
        final boolean isWal = isPartitioned && layout >= NON_PARTITIONED_CHANCE + NON_WAL_CHANCE;
        final ObjList<String> lateColumns = new ObjList<>();
        if (storageRnd.nextDouble() < COLUMN_TOP_CHANCE) {
            for (int i = 0, n = columns.size(); i < n; i++) {
                final FuzzColumn c = columns.getQuick(i);
                final String name = c.getName();
                if (name.equals(TS_COLUMN)
                        || c.getType() == ShortType.INSTANCE
                        || c.getType().getKind() == ColumnKind.TEMPORAL
                        || isIndexedOrCovered(name, columns)
                        || isIndexedOrCovered(name, mirroredColumns)) {
                    continue;
                }
                final boolean isJoinKey = name.equals(JOIN_KEY_COLUMN) || name.equals(INT_JOIN_KEY_COLUMN)
                        || name.equals(LONG_JOIN_KEY_COLUMN) || name.equals(VARCHAR_JOIN_KEY_COLUMN);
                if (storageRnd.nextDouble() < (isJoinKey ? LATE_JOIN_KEY_CHANCE : LATE_COLUMN_CHANCE)) {
                    lateColumns.add(name);
                }
            }
        }
        final boolean hasDroppedColumn = storageRnd.nextDouble() < DROPPED_COLUMN_CHANCE;
        if (lateColumns.size() == 0 && !hasDroppedColumn) {
            return new FuzzStorage(isWal, isPartitioned, lateColumns, 0, -1, isJoinKeyDictionaryPadded(storageRnd, isPartitioned));
        }
        // A quarter to three quarters of the rows come first.
        final int firstChunkRows = rows / 4 + storageRnd.nextInt(rows / 2);
        final int droppedColumnPos = hasDroppedColumn ? storageRnd.nextInt(columns.size() - lateColumns.size()) : -1;
        return new FuzzStorage(isWal, isPartitioned, lateColumns, firstChunkRows, droppedColumnPos, isJoinKeyDictionaryPadded(storageRnd, isPartitioned));
    }

    /**
     * With {@link #COMPOSITE_PARTITION_CHANCE} probability, backdates a few rows
     * into one non-active day the table already holds, landing between that day's
     * own existing timestamps ({@link #config}'s step evenly divides a day, so a
     * day boundary is always a row boundary too). With merge-append enabled
     * this relocates the overlapping stride to the tail as its own piece and
     * leaves the superseded copy dead, making the day a genuine composite
     * partition. Returns the backdated day (ISO date), or {@code null} if the
     * coin missed, the table has no non-active day to target, or executor is
     * null, which draws as the others do and backdates nothing.
     */
    private String applyCompositePartition(Rnd rnd, String tableName, ObjList<FuzzColumn> columns, int rows, SqlExecutor executor) throws SqlException {
        if (rnd.nextDouble() >= COMPOSITE_PARTITION_CHANCE) {
            return null;
        }
        long totalMicros = config.getStepMicros() * (long) rows;
        int numPartitions = (int) (totalMicros / DAY_MICROS) + 1;
        if (numPartitions <= 1) {
            // Only the active day exists - nothing non-active to backdate into.
            return null;
        }
        LocalDate start = LocalDate.parse(config.getTsStart(), DateTimeFormatter.ISO_LOCAL_DATE);
        String day = start.plusDays(rnd.nextInt(numPartitions - 1)).toString();
        int rowsPerDay = (int) (DAY_MICROS / config.getStepMicros());
        int overlapRows = 1 + rnd.nextInt(Math.min(3, rowsPerDay));
        if (executor == null) {
            return null;
        }
        executor.execute(buildOverlapInsertDml(tableName, columns, day, overlapRows));
        return day;
    }

    // Converts nothing when executor is null, a table of one partition, but draws as for the others.
    private ParquetConversion applyParquetConversion(Rnd rnd, String tableName, int rows, SqlExecutor executor) throws SqlException {
        ParquetMode mode = pickParquetMode(rnd);
        String partitions = null;
        if (executor == null) {
            if (mode == ParquetMode.PARTIAL) {
                pickPartialPartitionList(rnd, rows);
            }
            return new ParquetConversion(ParquetMode.NONE, null);
        }
        switch (mode) {
            case NONE -> {
            }
            case ALL -> executor.execute("ALTER TABLE " + tableName
                    + " CONVERT PARTITION TO PARQUET WHERE " + TS_COLUMN + " >= 0");
            case PARTIAL -> {
                partitions = pickPartialPartitionList(rnd, rows);
                if (partitions != null) {
                    executor.execute("ALTER TABLE " + tableName
                            + " CONVERT PARTITION TO PARQUET LIST " + partitions);
                } else {
                    mode = ParquetMode.NONE;
                }
            }
        }
        return new ParquetConversion(mode, partitions);
    }

    /**
     * Assigns a randomly chosen index to a fraction of the SYMBOL columns,
     * replacing the affected entries in place. The index pass runs after the
     * full column list exists so a posting index's covering INCLUDE list can
     * reference any other column by name. Each SYMBOL column is decided
     * independently, so calling this on a mirrored list gives the shadow a
     * different indexed shape than the primary.
     */
    private void assignIndexes(Rnd rnd, ObjList<FuzzColumn> columns) {
        for (int i = 0, n = columns.size(); i < n; i++) {
            FuzzColumn c = columns.getQuick(i);
            if (c.getType() == SymbolType.INSTANCE && isIndexedSymbol(rnd)) {
                FuzzIndex index = pickIndex(rnd, columns, c.getName());
                columns.setQuick(i, new FuzzColumn(c.getName(), c.getType(), index));
            }
        }
    }

    private ObjList<FuzzColumn> buildColumnList(Rnd rnd) {
        int numExtra = config.getMinColumnsPerTable()
                + rnd.nextInt(config.getMaxColumnsPerTable() - config.getMinColumnsPerTable() + 1);
        ObjList<FuzzColumn> columns = new ObjList<>();

        // Shared join key. Always SYMBOL so ASOF/LT/SPLICE on (sym) has a target.
        columns.add(new FuzzColumn(JOIN_KEY_COLUMN, SymbolType.INSTANCE));
        // Shared INT join key over a small domain, so an INT equi-join matches rows.
        columns.add(new FuzzColumn(INT_JOIN_KEY_COLUMN, IntKeyType.INSTANCE));
        // Shared LONG and VARCHAR join keys over the same domains, so an equi-join over
        // either matches rows and takes the fused plan's staged-key route.
        columns.add(new FuzzColumn(LONG_JOIN_KEY_COLUMN, LongKeyType.INSTANCE));
        columns.add(new FuzzColumn(VARCHAR_JOIN_KEY_COLUMN, VarcharKeyType.INSTANCE));

        for (int i = 0; i < numExtra; i++) {
            columns.add(new FuzzColumn("c" + i, dealType(rnd)));
        }

        // Designated timestamp last.
        columns.add(new FuzzColumn(TS_COLUMN, TimestampType.INSTANCE));

        // Decide indexes once the whole list (and thus the covering candidates)
        // is known.
        assignIndexes(rnd, columns);
        return columns;
    }

    /**
     * Next type off the run's deck, or a plain random draw once the deck runs
     * out. Dealing without replacement is what keeps a rare type - CHAR, IPv4,
     * UUID - from sitting out a whole run, taking every filter over it along
     * with it.
     */
    private FuzzColumnType dealType(Rnd rnd) {
        if (typeDeck.size() == 0) {
            typeDeck.addAll(FuzzColumnTypes.shuffledDeck(rnd));
        }
        if (typeDeckCursor < typeDeck.size()) {
            return typeDeck.getQuick(typeDeckCursor++);
        }
        return FuzzColumnTypes.pickRandom(rnd);
    }

    private String buildCreateDdl(String tableName, ObjList<FuzzColumn> columns, boolean isWal, boolean isPartitioned) {
        StringSink ddl = new StringSink();
        ddl.put("CREATE TABLE ").put(tableName).put(" (");
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                ddl.put(", ");
            }
            FuzzColumn c = columns.getQuick(i);
            ddl.put(c.getName()).put(' ').put(c.getType().getDdl());
            if (c.isIndexed()) {
                c.getIndex().appendDdl(ddl);
            }
        }
        ddl.put(") TIMESTAMP(").put(TS_COLUMN).put(isPartitioned ? ") PARTITION BY DAY" : ") PARTITION BY NONE").put(isWal ? " WAL" : " BYPASS WAL");
        return ddl.toString();
    }

    // The rows' timestamps continue from rowOffset steps after the run's start.
    private String buildInsertDml(String tableName, ObjList<FuzzColumn> columns, int rows, int rowOffset) {
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
                        .put("', 'yyyy-MM-dd') + ").put(rowOffset * config.getStepMicros()).put("L, ").put(config.getStepMicros()).put("L)");
            } else {
                dml.put(c.getType().getRndCall());
            }
            dml.put(' ').put(c.getName());
        }
        dml.put(" FROM long_sequence(").put(rows).put(')');
        return dml.toString();
    }

    /**
     * Same shape as {@link #buildInsertDml}, but landing INSIDE one day's own existing rows: half a
     * step above the day's start, so every backdated row falls between two rows the day already holds.
     * Half a step and not zero, because a row landing exactly on an existing timestamp would give the
     * table a tied pair, and the order a query returns a tie in is not decided by any storage layer -
     * a cursor reading through a symbol index emits rows grouped by symbol key rather than in physical
     * order, so a tie-sensitive projection such as {@code row_number() OVER (ORDER BY ts DESC)}
     * numbers the pair differently on the indexed primary than on its non-indexed shadow, and the
     * diff-shadow oracle reads that as storage divergence. Equal-timestamp merge order is covered
     * directly instead, by {@code CompositeEqualTimestampOrderTest}.
     */
    private String buildOverlapInsertDml(String tableName, ObjList<FuzzColumn> columns, String day, int rowCount) {
        StringSink dml = new StringSink();
        dml.put("INSERT INTO ").put(tableName).put(" SELECT ");
        for (int i = 0, n = columns.size(); i < n; i++) {
            if (i > 0) {
                dml.put(", ");
            }
            FuzzColumn c = columns.getQuick(i);
            if (c.getName().equals(TS_COLUMN)) {
                dml.put("timestamp_sequence(to_timestamp('").put(day)
                        .put("', 'yyyy-MM-dd') + ").put(config.getStepMicros() / 2).put("L, ")
                        .put(config.getStepMicros()).put("L)");
            } else {
                dml.put(c.getType().getRndCall());
            }
            dml.put(' ').put(c.getName());
        }
        dml.put(" FROM long_sequence(").put(rowCount).put(')');
        return dml.toString();
    }

    private boolean isIndexedSymbol(Rnd rnd) {
        return rnd.nextDouble() < INDEXED_SYMBOL_CHANCE;
    }

    /**
     * Mirrors a column list -- same names and types -- with indexes drawn
     * independently so the shadow can have a different index shape (kind and
     * covering columns) than the primary.
     */
    private ObjList<FuzzColumn> mirrorColumns(Rnd rnd, ObjList<FuzzColumn> source) {
        ObjList<FuzzColumn> out = new ObjList<>();
        for (int i = 0, n = source.size(); i < n; i++) {
            FuzzColumn c = source.getQuick(i);
            out.add(new FuzzColumn(c.getName(), c.getType()));
        }
        assignIndexes(rnd, out);
        return out;
    }

    /**
     * Picks up to {@link #POSTING_MAX_COVERING_COLUMNS} distinct covering
     * columns for a posting index on {@code indexedColName}. Eligible
     * candidates exclude the indexed column itself, the designated timestamp
     * (the engine auto-appends it), and the two var-width/composite types whose
     * covering support the fuzzer does not verify here (ARRAY and DECIMAL), so
     * table creation never throws. Returns {@code null} when no candidate
     * qualifies.
     */
    private ObjList<String> pickCoveringColumns(Rnd rnd, ObjList<FuzzColumn> columns, String indexedColName) {
        ObjList<String> candidates = new ObjList<>();
        for (int i = 0, n = columns.size(); i < n; i++) {
            FuzzColumn c = columns.getQuick(i);
            if (c.getName().equals(indexedColName) || c.getName().equals(TS_COLUMN)) {
                continue;
            }
            ColumnKind kind = c.getType().getKind();
            if (kind == ColumnKind.ARRAY || kind == ColumnKind.DECIMAL) {
                continue;
            }
            candidates.add(c.getName());
        }
        int size = candidates.size();
        if (size == 0) {
            return null;
        }
        int count = 1 + rnd.nextInt(Math.min(POSTING_MAX_COVERING_COLUMNS, size));
        // Partial Fisher-Yates over candidate indices yields 'count' distinct
        // picks without an INCLUDE duplicate.
        int[] order = new int[size];
        for (int i = 0; i < size; i++) {
            order[i] = i;
        }
        ObjList<String> picked = new ObjList<>();
        for (int i = 0; i < count; i++) {
            int j = i + rnd.nextInt(size - i);
            int tmp = order[i];
            order[i] = order[j];
            order[j] = tmp;
            picked.add(candidates.getQuick(order[i]));
        }
        return picked;
    }

    private FuzzIndex pickIndex(Rnd rnd, ObjList<FuzzColumn> columns, String indexedColName) {
        FuzzIndex.Kind kind = INDEX_KINDS[rnd.nextInt(INDEX_KINDS.length)];
        ObjList<String> covering = null;
        // Covering columns ride only on posting indexes.
        if (kind != FuzzIndex.Kind.BITMAP && rnd.nextDouble() < POSTING_COVERING_CHANCE) {
            covering = pickCoveringColumns(rnd, columns, indexedColName);
        }
        return new FuzzIndex(kind, covering);
    }

    /**
     * Returns the partition list to convert for a {@link ParquetMode#PARTIAL}
     * table (or {@code null} when the coin landed on no partition). The list
     * holds non-active partitions only -- the active partition is skipped by
     * the convert path -- and is rendered as comma-separated quoted ISO dates
     * suitable for {@code CONVERT PARTITION TO PARQUET LIST ...}.
     */
    private String pickPartialPartitionList(Rnd rnd, int rows) {
        long totalMicros = config.getStepMicros() * (long) rows;
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

    /**
     * Result of {@link #applyParquetConversion}: the realised {@link ParquetMode}
     * (which can downgrade from {@code PARTIAL} to {@code NONE} when the random
     * draw picks no partitions) and, for {@code PARTIAL}, the comma-separated
     * quoted ISO dates that went into the {@code CONVERT PARTITION TO PARQUET
     * LIST} command. The partition list is included in the schema log so a
     * post-mortem reader can reproduce the exact storage shape from the log
     * alone.
     */
    private record ParquetConversion(ParquetMode mode, String partitions) {
    }
}
