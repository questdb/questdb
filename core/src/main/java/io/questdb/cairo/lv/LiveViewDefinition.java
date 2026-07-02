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
import io.questdb.cairo.MicrosTimestampDriver;
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
 * </ul>
 */
public class LiveViewDefinition {
    public static final String LIVE_VIEW_DEFINITION_FILE_NAME = "_lv";
    public static final int LIVE_VIEW_DEFINITION_ANCHOR_MSG_TYPE = 1;
    public static final int LIVE_VIEW_DEFINITION_CORE_MSG_TYPE = 0;
    // Format version stamped as the first field of the CORE block. A reader that
    // finds a higher value refuses to load the view and surfaces it as
    // version_unsupported. Bump this when the CORE layout changes incompatibly;
    // each bump needs explicit per-version read handling.
    //
    // Version history:
    //   1 - initial CORE layout.
    //   2 - appends the dependency columns' compile-time types after their names,
    //       so a referenced base column whose TYPE changes invalidates the view.
    //       A version-1 block has no types; readFrom leaves the type list empty and
    //       the invalidation gate falls back to name-only checking for it.
    public static final int LIVE_VIEW_DEFINITION_FORMAT_VERSION = 2;
    // _lv.drop is the durable "DROP in progress" sentinel. dropLiveView creates
    // it (and fsyncs it) before any in-memory or on-disk teardown so a crash
    // mid-drop leaves an unambiguous signal for the startup loader to reap.
    // Sits in the LV directory alongside _lv and _lv.s; its mere existence is
    // the signal, the file contents are unused.
    public static final String LIVE_VIEW_DROP_SENTINEL_FILE_NAME = "_lv.drop";

    private final @Nullable LvAnchorSpec anchorSpec;
    // BACKFILL was specified at CREATE.
    private final boolean backfillRequested;
    private final String baseTableName;
    private final TableToken baseTableToken;
    private final int baseTimestampType;
    // Base-column names the SELECT depends on (filter inputs + window inputs +
    // designated ts). ApplyWal2TableJob's schema-change hook narrows invalidation
    // using this set: only changes touching one of these columns mark the view
    // INVALID; unrelated ALTERs leave it ACTIVE.
    private final ObjList<String> dependencyColumnNames;
    // Compile-time types of the dependency columns, positionally parallel to
    // dependencyColumnNames. ApplyWal2TableJob's schema-change hook compares these
    // against the post-change base metadata: a referenced column whose TYPE changed
    // (same name, different stride) must invalidate the view, since the cached
    // compiled factory would keep reading the new on-disk bytes through the old
    // stride. Empty for a version-1 definition read from disk (no types persisted);
    // the gate then falls back to name-only checking for it.
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
    private final String viewName;
    private final String viewSql;
    // Earliest base-table ts the view promises to retain rows for. Non-BACKFILL
    // views set this to the view's CREATE timestamp; O3 rejects rows below this bound.
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
            boolean backfillRequested,
            @Nullable LvAnchorSpec anchorSpec,
            ObjList<String> dependencyColumnNames,
            IntList dependencyColumnTypes,
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
        this.backfillRequested = backfillRequested;
        this.anchorSpec = anchorSpec;
        this.dependencyColumnNames = dependencyColumnNames;
        this.dependencyColumnTypes = dependencyColumnTypes;
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
        block.putBool(definition.backfillRequested);
        final int depCount = definition.dependencyColumnNames.size();
        block.putInt(depCount);
        for (int i = 0; i < depCount; i++) {
            block.putStr(definition.dependencyColumnNames.getQuick(i));
        }
        // Version 2+: the dependency columns' compile-time types, positionally
        // parallel to the names above (same count). Read back only when the CORE
        // block stamps version >= 2.
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

        writer.commit();
    }

    public static long toMicros(long value, char unit) {
        if (value == 0) {
            return 0;
        }
        return switch (unit) {
            case 'U' -> value; // explicit micros
            case 'T' -> MicrosTimestampDriver.INSTANCE.fromMillis(value);
            case 's' -> MicrosTimestampDriver.INSTANCE.fromSeconds(value);
            case 'm' -> MicrosTimestampDriver.INSTANCE.fromMinutes((int) value);
            case 'h' -> MicrosTimestampDriver.INSTANCE.fromHours((int) value);
            case 'd' -> MicrosTimestampDriver.INSTANCE.fromDays((int) value);
            default -> value;
        };
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
        throw SqlException.$(position + len, "invalid duration qualifier ").put(tok);
    }

    private static int endOfDigits(CharSequence tok, int len, int position) throws SqlException {
        int k = 0;
        while (k < len && tok.charAt(k) >= '0' && tok.charAt(k) <= '9') {
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
        boolean backfillRequested = false;
        ObjList<String> dependencyColumnNames = new ObjList<>();
        IntList dependencyColumnTypes = new IntList();
        LvAnchorSpec anchorSpec = null;

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
                backfillRequested = block.getBool(offset);
                offset += Byte.BYTES;
                int depCount = block.getInt(offset);
                offset += Integer.BYTES;
                dependencyColumnNames = new ObjList<>(depCount);
                for (int i = 0; i < depCount; i++) {
                    CharSequence colNameCs = block.getStr(offset);
                    offset += Vm.getStorageLength(colNameCs);
                    dependencyColumnNames.add(Chars.toString(colNameCs));
                }
                // Version 2+ appends one int per dependency column: its compile-time
                // type, parallel to the names just read. A version-1 block has none,
                // so the type list stays empty and the invalidation gate degrades to
                // name-only checking for that view.
                if (onDiskVersion >= 2) {
                    dependencyColumnTypes = new IntList(depCount);
                    for (int i = 0; i < depCount; i++) {
                        dependencyColumnTypes.add(block.getInt(offset));
                        offset += Integer.BYTES;
                    }
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
                backfillRequested,
                anchorSpec,
                dependencyColumnNames,
                dependencyColumnTypes,
                metadata
        );
    }

    public @Nullable LvAnchorSpec getAnchorSpec() {
        return anchorSpec;
    }

    public boolean getBackfillRequested() {
        return backfillRequested;
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
     * Rejects a CORE block whose stamped format version is newer than this build
     * supports, throwing {@link CairoException#LV_FILE_VERSION_UNSUPPORTED}. The
     * catalogue load path catches this and surfaces the view as
     * version_unsupported instead of hiding it.
     */
    private static void requireSupportedFormatVersion(int onDiskVersion, @NotNull TableToken liveViewToken) {
        if (onDiskVersion > LIVE_VIEW_DEFINITION_FORMAT_VERSION) {
            throw CairoException.critical(CairoException.LV_FILE_VERSION_UNSUPPORTED)
                    .put("live view definition format version not supported [view=")
                    .put(liveViewToken.getTableName())
                    .put(", onDiskVersion=").put(onDiskVersion)
                    .put(", supportedVersion=").put(LIVE_VIEW_DEFINITION_FORMAT_VERSION)
                    .put(']');
        }
    }

    /**
     * Validates that {@code timestampType} is a TIMESTAMP variant supported as the
     * base-table designated timestamp.
     */
    public static int requireTimestampType(int timestampType, int position) {
        if (ColumnType.tagOf(timestampType) != ColumnType.TIMESTAMP) {
            throw CairoException.nonCritical()
                    .position(position)
                    .put("base table designated timestamp must be a TIMESTAMP column");
        }
        return timestampType;
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
