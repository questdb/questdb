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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.file.AppendableBlock;
import io.questdb.cairo.file.BlockFileReader;
import io.questdb.cairo.file.BlockFileWriter;
import io.questdb.cairo.file.ReadableBlock;
import io.questdb.cairo.vm.Vm;
import io.questdb.griffin.SqlException;
import io.questdb.std.Chars;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable definition of a live view, persisted in the {@code _lv} block file.
 * <p>
 * Mirrors {@link io.questdb.cairo.mv.MatViewDefinition} — written once at CREATE,
 * never rewritten in V1 (ALTER LIVE VIEW is deferred). New schema bumps land as
 * new block types; old readers ignore unknown blocks.
 * <p>
 * Phase 1 holds the CORE_DEFINITION block only. ANCHOR / NAMED_WINDOWS land in a
 * later block once Phase 1 ANCHOR work begins.
 */
public class LiveViewDefinition {
    public static final String LIVE_VIEW_DEFINITION_FILE_NAME = "_lv";
    // Block types ordered by lvSchemaVersion.
    // 0 = CORE_DEFINITION (V1).
    public static final int LIVE_VIEW_DEFINITION_CORE_MSG_TYPE = 0;

    private final String baseTableName;
    private final TableToken baseTableToken;
    private final int baseTimestampType;
    private final boolean backfillRequested;
    private final long flushEveryInterval;
    private final char flushEveryIntervalUnit;
    private final long inMemoryInterval;
    private final char inMemoryIntervalUnit;
    private final GenericRecordMetadata metadata;
    private final int partitionBy;
    private final String viewName;
    private final String viewSql;
    // Earliest base-table ts the view promises to retain rows for. Phase 1 (no BACKFILL)
    // sets this to the view's CREATE timestamp; O3 rejects rows below this bound.
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
        this.metadata = metadata;
    }

    public static void append(@NotNull LiveViewDefinition definition, @NotNull BlockFileWriter writer) {
        final AppendableBlock block = writer.append();
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
        block.commit(LIVE_VIEW_DEFINITION_CORE_MSG_TYPE);
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
        final BlockFileReader.BlockCursor cursor = reader.getCursor();
        while (cursor.hasNext()) {
            final ReadableBlock block = cursor.next();
            if (block.type() == LIVE_VIEW_DEFINITION_CORE_MSG_TYPE) {
                long offset = 0;

                CharSequence viewSqlCs = block.getStr(offset);
                offset += Vm.getStorageLength(viewSqlCs);
                String viewSql = Chars.toString(viewSqlCs);

                CharSequence baseTableNameCs = block.getStr(offset);
                offset += Vm.getStorageLength(baseTableNameCs);
                String baseTableName = Chars.toString(baseTableNameCs);

                int baseTimestampType = block.getInt(offset);
                offset += Integer.BYTES;
                long flushEveryInterval = block.getLong(offset);
                offset += Long.BYTES;
                char flushEveryIntervalUnit = block.getChar(offset);
                offset += Character.BYTES;
                long inMemoryInterval = block.getLong(offset);
                offset += Long.BYTES;
                char inMemoryIntervalUnit = block.getChar(offset);
                offset += Character.BYTES;
                int partitionBy = block.getInt(offset);
                offset += Integer.BYTES;
                long viewLowerBoundTimestamp = block.getLong(offset);
                offset += Long.BYTES;
                boolean backfillRequested = block.getBool(offset);

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
                        metadata
                );
            }
        }
        throw CairoException.critical(0)
                .put("cannot read live view definition, block not found [path=").put(path).put(']');
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

    public boolean isBackfillRequested() {
        return backfillRequested;
    }

    /**
     * Phase 1 has no ALTER LIVE VIEW, so {@link PartitionBy#NONE} stays as a sentinel
     * meaning "inherit base partition scheme". Resolved at CREATE against the base table
     * once the token is known.
     */
    public boolean inheritsPartitionByFromBase() {
        return partitionBy == PartitionBy.NONE;
    }

    public static int defaultPartitionBy() {
        // No PartitionBy.NONE constant means "inherit"; we use a NONE-shaped sentinel.
        // PartitionBy.NONE itself maps to "no partitioning at all", so be explicit at
        // CREATE: callers that want the inherit semantics must pass NONE here and the
        // engine resolves later.
        return PartitionBy.NONE;
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
}
