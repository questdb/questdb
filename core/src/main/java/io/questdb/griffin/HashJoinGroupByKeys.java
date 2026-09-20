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

package io.questdb.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.std.BitSet;
import io.questdb.std.IntList;

/**
 * The reconciled equality keys of one fused hash join, in the order the key sinks stage them.
 * {@link HashJoinGroupByCandidate} fills this from the analysed base tables, so the column
 * indexes address those tables until {@link HashJoinGroupByMetadata} compiles them against the
 * actual inputs. The class holds plain values and borrows nothing, so a cursor factory may keep it.
 * <p>
 * {@link #add(int, int, int, int)} applies the reconciliation rule that
 * {@code SqlCodeGenerator.processJoinContext()} and the horizon join share, so a pair this class
 * rejects is the pair the ordinary plan rejects with "join column type mismatch". The sink flags
 * are indexed by key position rather than by column, which keeps the two sides apart and lets
 * {@link HashJoinGroupByMetadata} turn them into the per-column bit sets that
 * {@code RecordSinkFactory} takes.
 * <p>
 * A lone INT pair and a lone SYMBOL pair keep the narrow INT layout ({@link #isIntKeyed()}).
 * Every other shape stages its key through a {@link io.questdb.cairo.RecordSink} into a map.
 * A SYMBOL pair compares as ints either way: the probe translates its symbol key into the
 * build's domain, so the pair reconciles to {@link ColumnType#SYMBOL} and neither side writes
 * its text. That is what {@code SqlCodeGenerator.convertSymbolJoinKeysToInt()} does for the
 * ordinary hash join whenever both symbol tables are static, which the planner also requires.
 */
public final class HashJoinGroupByKeys {
    private final IntList buildColumns = new IntList();
    private final BitSet buildStringAsVarchar = new BitSet();
    private final BitSet buildTimestampAsNanos = new BitSet();
    private final IntList buildTypes = new IntList();
    private final IntList probeColumns = new IntList();
    private final BitSet probeStringAsVarchar = new BitSet();
    private final BitSet probeTimestampAsNanos = new BitSet();
    private final IntList probeTypes = new IntList();
    private final BitSet symbolAsString = new BitSet();
    private final IntList types = new IntList();

    /**
     * Key types a {@link io.questdb.cairo.RecordSink} stages and the maps store. The set is wider
     * than {@link HashJoinGroupByCandidate#supportsValueType(int)}, which the payload and the
     * projection answer to, because a key column reaches the map and never the row heap. BINARY
     * and ARRAY keys keep the ordinary plan, which joins them itself.
     */
    public static boolean supportsKeyType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR,
                 ColumnType.INT, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP,
                 ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.SYMBOL, ColumnType.STRING,
                 ColumnType.VARCHAR, ColumnType.LONG128, ColumnType.LONG256, ColumnType.UUID,
                 ColumnType.IPv4, ColumnType.GEOBYTE, ColumnType.GEOSHORT, ColumnType.GEOINT,
                 ColumnType.GEOLONG, ColumnType.DECIMAL8, ColumnType.DECIMAL16, ColumnType.DECIMAL32,
                 ColumnType.DECIMAL64, ColumnType.DECIMAL128, ColumnType.DECIMAL256 -> true;
            default -> false;
        };
    }

    /**
     * Reconciles one equality and appends it. Returns false for a type the fused plan does not
     * stage, when the ordinary plan would reject the pair, and when a column that an earlier key
     * already stages would have to stage differently
     * here: one column reaches its sink through one encoding, so the shape keeps the ordinary plan.
     * A probe SYMBOL column that two keys would translate is the same kind of conflict, since
     * the translating record indexes its views by probe column and can hold only one per column.
     * The caller discards these keys when this returns false, so a rejected pair may stay appended.
     */
    public boolean add(int probeColumn, int probeType, int buildColumn, int buildType) {
        if (!supportsKeyType(probeType) || !supportsKeyType(buildType)) {
            return false;
        }
        if (probeType != buildType
                && !(ColumnType.isSymbolOrStringOrVarchar(probeType) && ColumnType.isSymbolOrStringOrVarchar(buildType))
                && !(ColumnType.isTimestamp(probeType) && ColumnType.isTimestamp(buildType))) {
            return false;
        }
        final int key = size();
        final int type;
        if (ColumnType.isVarchar(probeType) || ColumnType.isVarchar(buildType)) {
            type = ColumnType.VARCHAR;
            if (!ColumnType.isVarchar(probeType)) {
                probeStringAsVarchar.set(key);
            }
            if (!ColumnType.isVarchar(buildType)) {
                buildStringAsVarchar.set(key);
            }
            if (ColumnType.isSymbol(probeType) || ColumnType.isSymbol(buildType)) {
                symbolAsString.set(key);
            }
        } else if (ColumnType.isSymbol(probeType) && ColumnType.isSymbol(buildType)) {
            // Both dictionaries stay as they are and the probe translates its key into the
            // build's domain, so the pair compares as ints whether it is the lone key or one
            // column of a staged composite.
            type = ColumnType.SYMBOL;
        } else if (ColumnType.isSymbol(probeType) || ColumnType.isSymbol(buildType)) {
            type = ColumnType.STRING;
            symbolAsString.set(key);
        } else if (probeType != buildType) {
            // Both are timestamps of different units, the one pair the gate above still admits;
            // the coarser side widens to nanos. Two STRING keys fall through to the branch below.
            type = ColumnType.TIMESTAMP_NANO;
            if (!ColumnType.isTimestampNano(probeType)) {
                probeTimestampAsNanos.set(key);
            }
            if (!ColumnType.isTimestampNano(buildType)) {
                buildTimestampAsNanos.set(key);
            }
        } else {
            type = probeType;
        }
        probeColumns.add(probeColumn);
        probeTypes.add(probeType);
        buildColumns.add(buildColumn);
        buildTypes.add(buildType);
        types.add(type);
        return !hasEncodingConflict(key);
    }

    public int getBuildColumn(int key) {
        return buildColumns.getQuick(key);
    }

    public int getBuildType(int key) {
        return buildTypes.getQuick(key);
    }

    public int getProbeColumn(int key) {
        return probeColumns.getQuick(key);
    }

    public int getProbeType(int key) {
        return probeTypes.getQuick(key);
    }

    /** The type the map stores, which is neither side's type when the two reconcile to a third. */
    public int getType(int key) {
        return types.getQuick(key);
    }

    /** The build side writes its STRING key as a VARCHAR, because the probe side is a VARCHAR. */
    public boolean isBuildStringAsVarchar(int key) {
        return buildStringAsVarchar.get(key);
    }

    /** The build side writes its TIMESTAMP key in nanos, because the probe side is in nanos. */
    public boolean isBuildTimestampAsNanos(int key) {
        return buildTimestampAsNanos.get(key);
    }

    /** True for the narrow INT layout: one INT pair, or one SYMBOL pair the probe translates. */
    public boolean isIntKeyed() {
        return size() == 1 && (types.getQuick(0) == ColumnType.INT || types.getQuick(0) == ColumnType.SYMBOL);
    }

    /** The probe side writes its STRING key as a VARCHAR, because the build side is a VARCHAR. */
    public boolean isProbeStringAsVarchar(int key) {
        return probeStringAsVarchar.get(key);
    }

    /** The probe side writes its TIMESTAMP key in nanos, because the build side is in nanos. */
    public boolean isProbeTimestampAsNanos(int key) {
        return probeTimestampAsNanos.get(key);
    }

    /** Both sides write a SYMBOL key as its text, so keys from different dictionaries compare. */
    public boolean isSymbolAsString(int key) {
        return symbolAsString.get(key);
    }

    /** The single SYMBOL pair of the INT layout, whose probe keys translate into the build's domain. */
    public boolean isSymbolKey() {
        return size() == 1 && types.getQuick(0) == ColumnType.SYMBOL;
    }

    /** A SYMBOL pair, whose probe keys translate into the build's domain instead of comparing as text. */
    public boolean isTranslatedSymbol(int key) {
        return types.getQuick(key) == ColumnType.SYMBOL;
    }

    public int size() {
        return types.size();
    }

    private boolean hasEncodingConflict(int key) {
        for (int i = 0; i < key; i++) {
            if (probeColumns.getQuick(i) == probeColumns.getQuick(key)
                    && (probeStringAsVarchar.get(i) != probeStringAsVarchar.get(key)
                    || probeTimestampAsNanos.get(i) != probeTimestampAsNanos.get(key)
                    || symbolAsString.get(i) != symbolAsString.get(key)
                    // One probe SYMBOL column translates once, because the translating record
                    // the probe sink reads indexes its views by probe column.
                    || (isTranslatedSymbol(i) && isTranslatedSymbol(key)))) {
                return true;
            }
            if (buildColumns.getQuick(i) == buildColumns.getQuick(key)
                    && (buildStringAsVarchar.get(i) != buildStringAsVarchar.get(key)
                    || buildTimestampAsNanos.get(i) != buildTimestampAsNanos.get(key)
                    || symbolAsString.get(i) != symbolAsString.get(key))) {
                return true;
            }
        }
        return false;
    }
}
