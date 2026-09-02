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

package io.questdb.cairo.lv;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable definition of a live view, persisted in the {@code _lv} block file.
 * <p>
 * Mirrors {@link io.questdb.cairo.mv.MatViewDefinition} — written once at CREATE,
 * never rewritten (ALTER LIVE VIEW is deferred). New schema bumps land as new
 * block types; old readers ignore unknown blocks.
 * <p>
 * Block types:
 * <ul>
 *     <li>{@code 0} — CORE_DEFINITION (required).</li>
 *     <li>{@code 1} — ANCHOR_SPEC (optional). Captures the single anchored named
 *     WINDOW the LV's SELECT defined, so the live-view runtime can compile the
 *     anchor expression at startup without re-parsing the SELECT.</li>
 *     <li>{@code 2} — PARTITION_KEY_DECISION (optional). Which PARTITION BY terms key
 *     as LV-private symbol ids, so a later build honors the answer the view was
 *     created with rather than re-deriving one its own classifier might change. Absent
 *     for a view created before the decision was persisted, which re-derives.</li>
 * </ul>
 */
public class LiveViewDefinition {
    public static final String LIVE_VIEW_DEFINITION_FILE_NAME = "_lv";
    public static final int LIVE_VIEW_DEFINITION_ANCHOR_MSG_TYPE = 1;
    public static final int LIVE_VIEW_DEFINITION_CORE_MSG_TYPE = 0;
    public static final int LIVE_VIEW_DEFINITION_PARTITION_KEY_MSG_TYPE = 2;
    // Format version stamped as the first field of the CORE block. A reader that
    // finds a higher value refuses to load the view and surfaces it as
    // version_unsupported. Live views ship at version 1: while the feature is
    // unreleased, any CORE layout change edits the v1 layout in place with no
    // back-compat read path. Bump this only for a post-release incompatible
    // change, and add explicit per-version read handling then.
    public static final int LIVE_VIEW_DEFINITION_FORMAT_VERSION = 1;
    // _lv.drop is the "DROP in progress" sentinel. dropLiveView creates it (and
    // fsyncs it) before any in-memory or on-disk teardown so a crash mid-drop
    // leaves an unambiguous signal for the startup loader to reap. Sits in the LV
    // directory alongside _lv and _lv.s; its mere existence is the signal, the
    // file contents are unused. The fsync covers the file, not the directory
    // entry that names it, so the ordering holds across a process crash but not
    // across a power loss -- see TableUtils.writeLiveViewDropSentinel.
    public static final String LIVE_VIEW_DROP_SENTINEL_FILE_NAME = "_lv.drop";
    // START FROM modes, as persisted in the CORE block. The mode the user wrote is kept
    // alongside the boundary it resolved to: NOW and an explicit timestamp both persist a
    // finite floor in viewLowerBoundTimestamp, and only the kind tells them apart for
    // SHOW CREATE LIVE VIEW and the catalogue.
    public static final byte START_FROM_BEGINNING = 1;
    public static final byte START_FROM_NOW = 0;
    public static final byte START_FROM_TIMESTAMP = 2;
    // Parse-time only, never persisted: the CREATE builder's "user has not written the
    // clause yet" state. The clause is mandatory, so a definition never carries this.
    public static final byte START_FROM_UNSET = -1;

    private final @Nullable LvAnchorSpec anchorSpec;
    private final String baseTableName;
    // Not final: on a read-only replica the LV's files can download and register BEFORE its base
    // table's, so the registration-time name lookup resolves to null. The refresh scan heals it
    // via resolveBaseTableToken once the base registers; until then the view serves disk-only.
    // Volatile: the healing write happens on a refresh worker while other workers read it.
    private volatile TableToken baseTableToken;
    private final int baseTimestampType;
    // Base-column names the SELECT depends on (filter inputs + window inputs +
    // designated ts). ApplyWal2TableJob's schema-change hook narrows invalidation
    // using this set: only changes touching one of these columns mark the view
    // INVALID; unrelated ALTERs leave it ACTIVE.
    private final ObjList<String> dependencyColumnNames;
    // Compile-time types of the dependency columns, positionally parallel to
    // dependencyColumnNames (same count - writer asserts, reader restores entry-for-
    // entry, so findFirstMissingOrRetypedColumn indexes both without a length guard).
    // ApplyWal2TableJob's schema-change hook invalidates the view when a referenced
    // column's TYPE changed (same name, different stride), since the cached factory
    // would read the new bytes through the old stride.
    private final IntList dependencyColumnTypes;
    private final long flushEveryInterval;
    private final char flushEveryIntervalUnit;
    // User-facing knob for the in-memory tier: how much of the most recent data
    // (by event time) the tier keeps before eviction trims it. Parsed at CREATE,
    // validated against cairo.live.view.in.memory.max, and persisted in _lv.
    private final long inMemoryInterval;
    private final char inMemoryIntervalUnit;
    private final GenericRecordMetadata metadata;
    private final int partitionBy;
    // Which PARTITION BY terms this view keys as LV-private symbol ids, decided by the
    // CREATE-time compile and honored by every later one. Null for a view created before
    // the decision was persisted: that view re-derives the classification on each compile,
    // which is what it has always done.
    private final @Nullable LiveViewPartitionKeyDecision partitionKeyDecision;
    // The START FROM mode the user wrote at CREATE: one of START_FROM_NOW,
    // START_FROM_BEGINNING, START_FROM_TIMESTAMP.
    private final byte startFromKind;
    // Not final: a replica can register a downloaded live view under a pending temp name when its
    // real name is still taken, and CairoEngine.applyTableRename later moves it to the real one.
    // Volatile: the rename runs on the WAL transfer / CheckWalTransactions thread while refresh
    // workers and the catalogue cursor read it.
    private volatile String viewName;
    private final String viewSql;
    // The resolved START FROM boundary, in base-table timestamp units. This is the live
    // view's membership rule and its only one: a base row belongs to the view iff its
    // designated timestamp is at or above this bound. The initial seed, the forward-append
    // refresh, and every applied-base replay apply the identical predicate, so a row never
    // appears or disappears merely because a replay ran.
    //
    // NOW resolves the engine clock once at CREATE; an explicit timestamp parses the literal
    // against the base's driver. BEGINNING has no lower bound at all and persists
    // Numbers.LONG_NULL, which is Long.MIN_VALUE - so `ts >= viewLowerBoundTimestamp` holds
    // for every row and needs no separate mode branch on the refresh hot paths. A designated
    // timestamp is never NULL, so no real row can sit at that sentinel.
    private final long viewLowerBoundTimestamp;

    public LiveViewDefinition(
            String viewName,
            String viewSql,
            String baseTableName,
            TableToken baseTableToken,
            int baseTimestampType,
            long flushEveryInterval,
            char flushEveryIntervalUnit,
            long inMemoryInterval,
            char inMemoryIntervalUnit,
            int partitionBy,
            long viewLowerBoundTimestamp,
            byte startFromKind,
            @Nullable LvAnchorSpec anchorSpec,
            ObjList<String> dependencyColumnNames,
            IntList dependencyColumnTypes,
            @Nullable LiveViewPartitionKeyDecision partitionKeyDecision,
            GenericRecordMetadata metadata
    ) {
        this.viewName = viewName;
        this.viewSql = viewSql;
        this.baseTableName = baseTableName;
        this.baseTableToken = baseTableToken;
        this.baseTimestampType = baseTimestampType;
        this.flushEveryInterval = flushEveryInterval;
        this.flushEveryIntervalUnit = flushEveryIntervalUnit;
        this.inMemoryInterval = inMemoryInterval;
        this.inMemoryIntervalUnit = inMemoryIntervalUnit;
        this.partitionBy = partitionBy;
        this.viewLowerBoundTimestamp = viewLowerBoundTimestamp;
        this.startFromKind = startFromKind;
        this.anchorSpec = anchorSpec;
        this.dependencyColumnNames = dependencyColumnNames;
        this.dependencyColumnTypes = dependencyColumnTypes;
        this.partitionKeyDecision = partitionKeyDecision;
        this.metadata = metadata;
    }

    public static void append(@NotNull LiveViewDefinition definition, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
        block.putInt(LIVE_VIEW_DEFINITION_FORMAT_VERSION);
        block.putStr(definition.viewSql);
        block.putStr(definition.baseTableName);
        block.putInt(definition.baseTimestampType);
        block.putLong(definition.flushEveryInterval);
        block.putChar(definition.flushEveryIntervalUnit);
        block.putLong(definition.inMemoryInterval);
        block.putChar(definition.inMemoryIntervalUnit);
        block.putInt(definition.partitionBy);
        block.putLong(definition.viewLowerBoundTimestamp);
        block.putByte(definition.startFromKind);
        final int depCount = definition.dependencyColumnNames.size();
        block.putInt(depCount);
        for (int i = 0; i < depCount; i++) {
            block.putStr(definition.dependencyColumnNames.getQuick(i));
        }
        // The dependency columns' compile-time types, positionally parallel to
        // the names above (same count). The reader pulls exactly depCount types,
        // so a types list shorter than the names list would both read OOB here
        // and emit a malformed block; the two are always built together at
        // CREATE, and this assert pins that invariant against any future
        // re-append path.
        assert definition.dependencyColumnTypes.size() == depCount
                : "dependencyColumnTypes count (" + definition.dependencyColumnTypes.size()
                + ") must equal dependencyColumnNames count (" + depCount + ")";
        for (int i = 0; i < depCount; i++) {
            block.putInt(definition.dependencyColumnTypes.getQuick(i));
        }
        block.commit(LIVE_VIEW_DEFINITION_CORE_MSG_TYPE);

        if (definition.anchorSpec != null) {
            final AppendableBlock anchor = writer.append();
            LvAnchorSpec spec = definition.anchorSpec;
            anchor.putStr(spec.windowName);
            anchor.putByte(spec.anchorKind);
            anchor.putStr(spec.anchorExpressionSql);
            anchor.putLong(spec.anchorDailyTimeUs);
            anchor.putStr(spec.anchorDailyTimeZone);
            anchor.putInt(spec.partitionColumnNames.size());
            for (int i = 0, n = spec.partitionColumnNames.size(); i < n; i++) {
                anchor.putStr(spec.partitionColumnNames.get(i));
            }
            anchor.commit(LIVE_VIEW_DEFINITION_ANCHOR_MSG_TYPE);
        }

        // The partition-key decision rides in a block of its own rather than in CORE,
        // because CORE has no read path for an older layout: a view written before this
        // block existed has to keep loading, and it does - the reader finds no block and
        // re-derives the classification, which is exactly what that view always did.
        if (definition.partitionKeyDecision != null) {
            final AppendableBlock partitionKey = writer.append();
            final int keyCount = definition.partitionKeyDecision.getColumnCount();
            partitionKey.putInt(keyCount);
            for (int i = 0; i < keyCount; i++) {
                partitionKey.putStr(definition.partitionKeyDecision.getColumnName(i));
            }
            partitionKey.commit(LIVE_VIEW_DEFINITION_PARTITION_KEY_MSG_TYPE);
        }

        writer.commit();
    }

    public static long toMicros(long value, char unit) {
        if (value == 0) {
            return 0;
        }
        // Compute the micros in long arithmetic with an exact multiply. The
        // timestamp driver's fromMinutes / fromHours / fromDays take an int, so
        // feeding them a large parsed value narrows it (e.g. 4294967297m would
        // truncate to 1m); Math.multiplyExact instead throws on any value that
        // does not fit, and the parse path converts that to a positioned
        // SqlException (see toMicrosChecked). Products match the driver's.
        return switch (unit) {
            case 'U' -> value; // explicit micros
            case 'T' -> Math.multiplyExact(value, Micros.MILLI_MICROS);
            case 's' -> Math.multiplyExact(value, Micros.SECOND_MICROS);
            case 'm' -> Math.multiplyExact(value, Micros.MINUTE_MICROS);
            case 'h' -> Math.multiplyExact(value, Micros.HOUR_MICROS);
            case 'd' -> Math.multiplyExact(value, Micros.DAY_MICROS);
            default -> value;
        };
    }

    /**
     * Converts a parsed {@code (value, unit)} duration to micros at parse time,
     * turning an out-of-range value (one that would overflow a long micros
     * count) into a positioned {@link SqlException} instead of letting
     * {@link #toMicros} truncate or wrap it. Use this on user-supplied CREATE
     * LIVE VIEW clauses; {@link #toMicros} stays the plain read-back codec.
     */
    public static long toMicrosChecked(long value, char unit, int position) throws SqlException {
        try {
            return toMicros(value, unit);
        } catch (ArithmeticException e) {
            throw SqlException.$(position, "live view duration is out of range");
        }
    }

    /**
     * Parses a duration token like "200ms", "1s", "30m", "1h", "1d" into a
     * {@code (value, unitChar)} pair. The unit char matches the {@link #toMicros}
     * encoding ('T' for millis, 'U' for explicit micros, single-letter for the
     * larger units). FLUSH EVERY / IN MEMORY use this directly so they can round-trip
     * the user spec to {@code live_views()}.
     */
    public static long parseDurationValue(CharSequence tok, int position) throws SqlException {
        int len = tok.length();
        int k = endOfDigits(tok, len, position);
        try {
            return Numbers.parseLong(tok, 0, k);
        } catch (NumericException ex) {
            throw SqlException.$(position, "invalid duration value ").put(tok);
        }
    }

    public static char parseDurationUnit(CharSequence tok, int position) throws SqlException {
        int len = tok.length();
        int k = endOfDigits(tok, len, position);
        int nChars = len - k;
        if (nChars == 1) {
            char c = tok.charAt(k);
            if (c == 's' || c == 'm' || c == 'h' || c == 'd') {
                return c;
            }
        } else if (nChars == 2 && tok.charAt(k) == 'm' && tok.charAt(k + 1) == 's') {
            return 'T';
        }
        throw SqlException.$(position + k, "invalid duration qualifier ").put(tok);
    }

    /**
     * Parses the {@code START FROM '<timestamp>'} literal against the base table's designated
     * timestamp driver, so a MICRO base and a NANO base each read the literal at their own
     * precision and a sub-microsecond literal survives on a NANO base. Returns the boundary in
     * base-table timestamp units.
     * <p>
     * The parse is deliberately deferred to CREATE - the parser cannot see the base's timestamp
     * type - which is why this takes the literal's position: a malformed literal must still
     * report against the token the user typed.
     */
    public static long parseStartFromTimestamp(
            @NotNull CharSequence literal,
            int baseTimestampType,
            int position
    ) throws SqlException {
        final long timestamp;
        try {
            timestamp = ColumnType.getTimestampDriver(baseTimestampType).parseFloorLiteral(literal);
        } catch (NumericException e) {
            throw SqlException.$(position, "invalid live view START FROM timestamp [ts=").put(literal).put(']');
        }
        // parseFloorLiteral maps a null literal to the NULL sentinel, and a designated timestamp
        // is never NULL, so a NULL boundary could neither admit nor reject a row meaningfully.
        if (timestamp == Numbers.LONG_NULL) {
            throw SqlException.$(position, "live view START FROM timestamp cannot be NULL");
        }
        return timestamp;
    }

    private static int endOfDigits(CharSequence tok, int len, int position) throws SqlException {
        // Advance over the numeric run, admitting '_' thousands separators (e.g.
        // "3_600s") so FLUSH EVERY / IN MEMORY stay consistent with mat-view
        // strides. Placement of the separators is not
        // checked here; parseDurationValue's Numbers.parseLong over [0, k) rejects a
        // leading, trailing, or doubled '_', so a malformed value still fails closed.
        int k = 0;
        while (k < len && ((tok.charAt(k) >= '0' && tok.charAt(k) <= '9') || tok.charAt(k) == '_')) {
            k++;
        }
        if (k == 0) {
            throw SqlException.$(position, "invalid duration value ").put(tok);
        }
        return k;
    }

    /**
     * Reads only the base table name from {@code _lv}. Used at startup before the full
     * definition can be constructed (the base TableToken needs resolving first).
     */
    public static String readBaseTableName(
            @NotNull BlockFileReader reader,
            @NotNull Path path,
            int rootLen,
            @NotNull TableToken liveViewToken
    ) {
        path.trimTo(rootLen).concat(liveViewToken.getDirName()).concat(LIVE_VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == LIVE_VIEW_DEFINITION_CORE_MSG_TYPE) {
                long offset = 0;
                requireSupportedFormatVersion(block.getInt(offset), liveViewToken);
                offset += Integer.BYTES;
                CharSequence viewSqlCs = block.getStr(offset);
                offset += Vm.getStorageLength(viewSqlCs);
                return Chars.toString(block.getStr(offset));
            }
        }
        throw CairoException.critical(0)
                .put("cannot read live view definition, block not found [path=").put(path).put(']');
    }

    public static LiveViewDefinition readFrom(
            @NotNull BlockFileReader reader,
            @NotNull Path path,
            int rootLen,
            @NotNull TableToken liveViewToken,
            @Nullable TableToken baseTableToken,
            @NotNull GenericRecordMetadata metadata
    ) {
        path.trimTo(rootLen).concat(liveViewToken.getDirName()).concat(LIVE_VIEW_DEFINITION_FILE_NAME);
        reader.of(path.$());

        boolean coreFound = false;
        String viewSql = null;
        String baseTableName = null;
        int baseTimestampType = 0;
        long flushEveryInterval = 0;
        char flushEveryIntervalUnit = 0;
        long inMemoryInterval = 0;
        char inMemoryIntervalUnit = 0;
        int partitionBy = 0;
        long viewLowerBoundTimestamp = 0;
        byte startFromKind = START_FROM_NOW;
        ObjList<String> dependencyColumnNames = new ObjList<>();
        IntList dependencyColumnTypes = new IntList();
        LvAnchorSpec anchorSpec = null;
        LiveViewPartitionKeyDecision partitionKeyDecision = null;

        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == LIVE_VIEW_DEFINITION_CORE_MSG_TYPE) {
                coreFound = true;
                long offset = 0;
                final int onDiskVersion = block.getInt(offset);
                requireSupportedFormatVersion(onDiskVersion, liveViewToken);
                offset += Integer.BYTES;
                CharSequence viewSqlCs = block.getStr(offset);
                offset += Vm.getStorageLength(viewSqlCs);
                viewSql = Chars.toString(viewSqlCs);

                CharSequence baseTableNameCs = block.getStr(offset);
                offset += Vm.getStorageLength(baseTableNameCs);
                baseTableName = Chars.toString(baseTableNameCs);

                baseTimestampType = block.getInt(offset);
                offset += Integer.BYTES;
                flushEveryInterval = block.getLong(offset);
                offset += Long.BYTES;
                flushEveryIntervalUnit = block.getChar(offset);
                offset += Character.BYTES;
                inMemoryInterval = block.getLong(offset);
                offset += Long.BYTES;
                inMemoryIntervalUnit = block.getChar(offset);
                offset += Character.BYTES;
                partitionBy = block.getInt(offset);
                offset += Integer.BYTES;
                viewLowerBoundTimestamp = block.getLong(offset);
                offset += Long.BYTES;
                startFromKind = block.getByte(offset);
                offset += Byte.BYTES;
                int depCount = block.getInt(offset);
                offset += Integer.BYTES;
                dependencyColumnNames = new ObjList<>(depCount);
                for (int i = 0; i < depCount; i++) {
                    CharSequence colNameCs = block.getStr(offset);
                    offset += Vm.getStorageLength(colNameCs);
                    dependencyColumnNames.add(Chars.toString(colNameCs));
                }
                // One int per dependency column: its compile-time type, parallel
                // to the names just read.
                dependencyColumnTypes = new IntList(depCount);
                for (int i = 0; i < depCount; i++) {
                    dependencyColumnTypes.add(block.getInt(offset));
                    offset += Integer.BYTES;
                }
            } else if (block.type() == LIVE_VIEW_DEFINITION_ANCHOR_MSG_TYPE) {
                // block.getStr returns a flyweight backed by the block's memory; subsequent
                // getStr calls reuse the same flyweight, so each string must be materialised
                // to a stable String *before* the next getStr.
                long offset = 0;
                CharSequence windowNameCs = block.getStr(offset);
                offset += Vm.getStorageLength(windowNameCs);
                String windowName = Chars.toString(windowNameCs);
                byte anchorKind = block.getByte(offset);
                offset += Byte.BYTES;
                CharSequence exprSqlCs = block.getStr(offset);
                offset += Vm.getStorageLength(exprSqlCs);
                String anchorExpressionSql = Chars.toString(exprSqlCs);
                long anchorDailyTimeUs = block.getLong(offset);
                offset += Long.BYTES;
                CharSequence dailyTzCs = block.getStr(offset);
                offset += Vm.getStorageLength(dailyTzCs);
                String anchorDailyTimeZone = dailyTzCs == null ? null : Chars.toString(dailyTzCs);
                int partitionColumnCount = block.getInt(offset);
                offset += Integer.BYTES;
                ObjList<String> partitionColumnNames = new ObjList<>(partitionColumnCount);
                for (int i = 0; i < partitionColumnCount; i++) {
                    CharSequence colNameCs = block.getStr(offset);
                    offset += Vm.getStorageLength(colNameCs);
                    partitionColumnNames.add(Chars.toString(colNameCs));
                }
                anchorSpec = new LvAnchorSpec(
                        windowName,
                        anchorKind,
                        anchorExpressionSql,
                        anchorDailyTimeUs,
                        anchorDailyTimeZone,
                        0,
                        partitionColumnNames
                );
            } else if (block.type() == LIVE_VIEW_DEFINITION_PARTITION_KEY_MSG_TYPE) {
                long offset = 0;
                final int keyCount = block.getInt(offset);
                offset += Integer.BYTES;
                final ObjList<String> translatedColumnNames = new ObjList<>(keyCount);
                for (int i = 0; i < keyCount; i++) {
                    // Same flyweight discipline as the anchor block above: materialise each
                    // name before the next getStr reuses the buffer behind it.
                    CharSequence colNameCs = block.getStr(offset);
                    offset += Vm.getStorageLength(colNameCs);
                    translatedColumnNames.add(Chars.toString(colNameCs));
                }
                partitionKeyDecision = LiveViewPartitionKeyDecision.of(translatedColumnNames);
            }
        }
        if (!coreFound) {
            throw CairoException.critical(0)
                    .put("cannot read live view definition, block not found [path=").put(path).put(']');
        }
        return new LiveViewDefinition(
                liveViewToken.getTableName(),
                viewSql,
                baseTableName,
                baseTableToken,
                baseTimestampType,
                flushEveryInterval,
                flushEveryIntervalUnit,
                inMemoryInterval,
                inMemoryIntervalUnit,
                partitionBy,
                viewLowerBoundTimestamp,
                startFromKind,
                anchorSpec,
                dependencyColumnNames,
                dependencyColumnTypes,
                partitionKeyDecision,
                metadata
        );
    }

    public @Nullable LvAnchorSpec getAnchorSpec() {
        return anchorSpec;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public TableToken getBaseTableToken() {
        return baseTableToken;
    }

    public int getBaseTimestampType() {
        return baseTimestampType;
    }

    public ObjList<String> getDependencyColumnNames() {
        return dependencyColumnNames;
    }

    public IntList getDependencyColumnTypes() {
        return dependencyColumnTypes;
    }

    public long getFlushEveryInterval() {
        return flushEveryInterval;
    }

    public char getFlushEveryIntervalUnit() {
        return flushEveryIntervalUnit;
    }

    public long getFlushEveryMicros() {
        return toMicros(flushEveryInterval, flushEveryIntervalUnit);
    }

    public long getInMemoryInterval() {
        return inMemoryInterval;
    }

    public char getInMemoryIntervalUnit() {
        return inMemoryIntervalUnit;
    }

    public long getInMemoryMicros() {
        return toMicros(inMemoryInterval, inMemoryIntervalUnit);
    }

    public GenericRecordMetadata getMetadata() {
        return metadata;
    }

    public int getPartitionBy() {
        return partitionBy;
    }

    /**
     * Which PARTITION BY terms this view keys as LV-private symbol ids, or null for a view
     * created before the decision was persisted - which re-derives the classification on
     * every compile, as it always has.
     */
    public @Nullable LiveViewPartitionKeyDecision getPartitionKeyDecision() {
        return partitionKeyDecision;
    }

    public byte getStartFromKind() {
        return startFromKind;
    }

    public String getViewName() {
        return viewName;
    }

    public String getViewSql() {
        return viewSql;
    }

    public long getViewLowerBoundTimestamp() {
        return viewLowerBoundTimestamp;
    }

    /**
     * Heals a definition whose base-table token was unresolved at registration time. On a
     * read-only replica the LV's files can download and register before its base table's, so
     * the registration-time name lookup returns null and the refresh scan would skip the view
     * forever. The scan calls this once the base table registers; the token for a name is
     * stable, so a concurrent duplicate resolve writes the same value.
     */
    public void resolveBaseTableToken(TableToken baseTableToken) {
        this.baseTableToken = baseTableToken;
    }

    /**
     * Re-points the definition at the view's new name after a rename. Only the replication
     * apply path renames a live view: a downloaded view whose real name is still taken
     * registers under a pending temp name, and {@code CairoEngine.applyTableRename} moves it
     * once the name frees up. Called from the registry's rename, which keys both maps under
     * the same write lock.
     */
    public void updateViewName(String viewName) {
        this.viewName = viewName;
    }

    /**
     * Rejects a CORE block whose stamped format version is newer than this build
     * supports, throwing {@link CairoException#LV_FILE_VERSION_UNSUPPORTED}. The
     * catalogue load path catches this and surfaces the view as
     * version_unsupported instead of hiding it.
     */
    private static void requireSupportedFormatVersion(int onDiskVersion, @NotNull TableToken liveViewToken) {
        // Reject both a too-new version (this build cannot read it) and a below-floor one
        // (version 1 is the first; 0 / negative is corruption, not a legacy v1), so the first
        // version bump cannot make a zeroed / torn header silently parse as v1.
        if (onDiskVersion < 1 || onDiskVersion > LIVE_VIEW_DEFINITION_FORMAT_VERSION) {
            throw CairoException.critical(CairoException.LV_FILE_VERSION_UNSUPPORTED)
                    .put("live view definition format version not supported [view=")
                    .put(liveViewToken.getTableName())
                    .put(", onDiskVersion=").put(onDiskVersion)
                    .put(", supportedVersion=").put(LIVE_VIEW_DEFINITION_FORMAT_VERSION)
                    .put(']');
        }
    }

    /**
     * Persisted shape of a single anchored named WINDOW. At most one is captured
     * per LV (multi-anchored-window LVs are rejected at CREATE). The runtime side
     * — {@link LiveViewWindow} — uses this to compile the anchor expression and
     * build the partition machinery without re-parsing the SELECT.
     * <p>
     * Encoding maps to the {@link LiveViewDefinition#LIVE_VIEW_DEFINITION_ANCHOR_MSG_TYPE}
     * block: {@code windowName}, {@code anchorKind} (matches
     * {@link io.questdb.griffin.model.WindowExpression#ANCHOR_KIND_EXPRESSION} /
     * {@code ANCHOR_KIND_DAILY}), {@code anchorExpressionSql} (the post-DAILY-desugar
     * expression text), {@code anchorDailyTimeUs} / {@code anchorDailyTimeZone}
     * (raw DAILY clause for round-tripping in SHOW CREATE), and
     * {@code partitionColumnNames}.
     */
    public static final class LvAnchorSpec {
        public final long anchorDailyTimeUs;
        public final @Nullable String anchorDailyTimeZone;
        public final String anchorExpressionSql;
        public final byte anchorKind;
        // CREATE-time-only: offset of the ANCHOR keyword in the user's
        // original CREATE SQL. Used by validateAnchorPurity to anchor reject
        // positions in the source the user typed rather than in the
        // re-parsed desugared expression. Not persisted - reset to 0 when an
        // LvAnchorSpec is rehydrated from disk, which is harmless because
        // validateAnchorPurity runs at CREATE only and never at restart.
        public final int anchorPosition;
        public final ObjList<String> partitionColumnNames;
        public final String windowName;

        public LvAnchorSpec(
                String windowName,
                byte anchorKind,
                String anchorExpressionSql,
                long anchorDailyTimeUs,
                @Nullable String anchorDailyTimeZone,
                int anchorPosition,
                ObjList<String> partitionColumnNames
        ) {
            this.windowName = windowName;
            this.anchorKind = anchorKind;
            this.anchorExpressionSql = anchorExpressionSql;
            this.anchorDailyTimeUs = anchorDailyTimeUs;
            this.anchorDailyTimeZone = anchorDailyTimeZone;
            this.anchorPosition = anchorPosition;
            this.partitionColumnNames = partitionColumnNames;
        }
    }
}
