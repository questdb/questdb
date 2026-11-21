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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.plan.logical.BoundExpression;
import io.questdb.griffin.plan.logical.ColumnExpression;
import io.questdb.griffin.plan.logical.FunctionExpression;
import io.questdb.griffin.plan.logical.HashJoinAggregation;
import io.questdb.griffin.plan.logical.OutputSchema;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Resolves bound column ids into the actual probe projection and pruned build
 * payload. Joined columns are the physical probe columns followed by payload fields;
 * expressions retain their logical aliases even when RIGHT becomes LEFT OUTER.
 * Input mappings pair compiled-record indexes with logical column ids.
 * Build SYMBOL payloads keep the build input's symbol keys and its static symbol tables.
 * Construct before the compiler releases the bound plans of the {@link HashJoinAggregation}.
 * <p>
 * A SYMBOL key column compares as an int on both layouts: the probe translates its symbol key
 * into the build's domain, so this class publishes those key columns for the atom to bind a
 * translator to.
 * <p>
 * A key the narrow INT layout cannot carry reaches its map through a {@link RecordSink}
 * per input. This class generates the two sink classes and hands out instances, one per
 * worker, through {@link #newProbeKeySink()} and {@link #newBuildKeySink()}; it borrows
 * both input metadatas for that, so instantiate while the inputs are alive. The INT
 * layout stages nothing, so it gets no key types and no sinks.
 */
public final class HashJoinGroupByMetadata implements Closeable {
    private final IntList buildColumns = new IntList();
    // Borrowed for the looping sink, which reads the source types when a caller instantiates it.
    private final RecordMetadata buildInputMetadata;
    private final IntList buildKeyColumns = new IntList();
    private final ListColumnFilter buildKeyFilter = new ListColumnFilter();
    @Nullable
    private final Class<RecordSink> buildKeySinkClass;
    private final BitSet buildKeyStringAsVarchar = new BitSet();
    private final BitSet buildKeySymbolAsString = new BitSet();
    private final BitSet buildKeyTimestampAsNanos = new BitSet();
    private final OutputSchema input = new OutputSchema();
    private final String condition;
    private final boolean isKeyCapacityPresized;
    private final boolean isKeyStaged;
    private final boolean isSymbolKey;
    private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
    private final JoinRecordMetadata joinedMetadata;
    private final GenericRecordMetadata payloadMetadata = new GenericRecordMetadata();
    private final int probeColumnCount;
    private final RecordMetadata probeInputMetadata;
    private final IntList probeKeyColumns = new IntList();
    private final ListColumnFilter probeKeyFilter = new ListColumnFilter();
    @Nullable
    private final Class<RecordSink> probeKeySinkClass;
    // Per-column encoding flags of one input, indexed the way RecordSinkFactory indexes them. The
    // two inputs keep their own sets, because one set shared by both would force an encoding onto
    // whatever column happens to sit at the same index on the other side.
    private final BitSet probeKeyStringAsVarchar = new BitSet();
    private final BitSet probeKeySymbolAsString = new BitSet();
    private final BitSet probeKeyTimestampAsNanos = new BitSet();
    // Compiled indexes of the SYMBOL key columns, in key order, one entry per SYMBOL pair.
    private final IntList symbolKeyBuildColumns = new IntList();
    private final IntList symbolKeyProbeColumns = new IntList();

    public HashJoinGroupByMetadata(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            HashJoinAggregation hashJoin,
            RecordMetadata probeMetadata,
            IntList probeBaseColumns,
            RecordMetadata buildMetadata,
            IntList buildBaseColumns
    ) {
        if (probeBaseColumns.size() != probeMetadata.getColumnCount()
                || buildBaseColumns.size() != buildMetadata.getColumnCount()) {
            throw new IllegalStateException("hash join input mapping size mismatch");
        }
        probeColumnCount = probeMetadata.getColumnCount();
        final HashJoinGroupByKeys keys = hashJoin.getKeys();
        isKeyStaged = !keys.isIntKeyed();
        isSymbolKey = keys.isSymbolKey();
        isKeyCapacityPresized = hashJoin.isKeyCapacityPresized();
        StringSink conditionSink = Misc.getThreadLocalSink();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final int probeColumn = requireColumn(probeBaseColumns, keys.getProbeColumn(i));
            final int buildColumn = requireColumn(buildBaseColumns, keys.getBuildColumn(i));
            if (probeMetadata.getColumnType(probeColumn) != keys.getProbeType(i)
                    || buildMetadata.getColumnType(buildColumn) != keys.getBuildType(i)) {
                throw new IllegalStateException("hash join input key type mismatch");
            }
            probeKeyColumns.add(probeColumn);
            buildKeyColumns.add(buildColumn);
            if (keys.isTranslatedSymbol(i)) {
                symbolKeyProbeColumns.add(probeColumn);
                symbolKeyBuildColumns.add(buildColumn);
            }
            if (i > 0) {
                conditionSink.putAscii(" and ");
            }
            conditionSink.put(hashJoin.getProbeAlias()).putAscii('.').put(probeMetadata.getColumnName(probeColumn))
                    .putAscii('=').put(hashJoin.getBuildAlias()).putAscii('.').put(buildMetadata.getColumnName(buildColumn));
        }
        condition = conditionSink.toString();
        probeInputMetadata = probeMetadata;
        buildInputMetadata = buildMetadata;
        if (!isKeyStaged) {
            // The INT layout reads its key straight off the probe record, translating it when the
            // key is a SYMBOL pair, so it stages nothing and needs no sink.
            probeKeySinkClass = null;
            buildKeySinkClass = null;
        } else {
            for (int i = 0, n = keys.size(); i < n; i++) {
                final int probeColumn = probeKeyColumns.getQuick(i);
                final int buildColumn = buildKeyColumns.getQuick(i);
                keyTypes.add(keys.getType(i));
                // A ListColumnFilter entry is a 1-based index whose sign asks for a skip; a key
                // column is always copied, so every entry is positive.
                probeKeyFilter.add(probeColumn + 1);
                buildKeyFilter.add(buildColumn + 1);
                if (keys.isSymbolAsString(i)) {
                    // Both sides write the symbol's text, so keys from two dictionaries compare.
                    probeKeySymbolAsString.set(probeColumn);
                    buildKeySymbolAsString.set(buildColumn);
                }
                if (keys.isProbeStringAsVarchar(i)) {
                    probeKeyStringAsVarchar.set(probeColumn);
                }
                if (keys.isBuildStringAsVarchar(i)) {
                    buildKeyStringAsVarchar.set(buildColumn);
                }
                if (keys.isProbeTimestampAsNanos(i)) {
                    probeKeyTimestampAsNanos.set(probeColumn);
                }
                if (keys.isBuildTimestampAsNanos(i)) {
                    buildKeyTimestampAsNanos.set(buildColumn);
                }
            }
            probeKeySinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, probeMetadata,
                    probeKeyFilter, null, null, probeKeySymbolAsString, probeKeyStringAsVarchar, probeKeyTimestampAsNanos);
            buildKeySinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, buildMetadata,
                    buildKeyFilter, null, null, buildKeySymbolAsString, buildKeyStringAsVarchar, buildKeyTimestampAsNanos);
        }
        joinedMetadata = new JoinRecordMetadata(configuration,
                probeColumnCount + hashJoin.getRequiredBuildColumns().size());
        try {
            final OutputSchema probeOutput = hashJoin.getProbeOutput();
            for (int i = 0; i < probeColumnCount; i++) {
                joinedMetadata.add(hashJoin.getProbeAlias(), copyColumn(probeMetadata, i, probeMetadata.isSymbolTableStatic(i)));
                int sourceIndex = probeOutput.getColumnIndexById(probeBaseColumns.getQuick(i));
                if (sourceIndex >= 0) {
                    if (probeOutput.getColumnType(sourceIndex) != probeMetadata.getColumnType(i)) {
                        throw new IllegalStateException("hash join input mapping type mismatch");
                    }
                    input.addColumnFrom(probeOutput, sourceIndex);
                } else {
                    input.addColumnFrom(probeMetadata, i, probeBaseColumns.getQuick(i));
                }
            }
            final OutputSchema buildOutput = hashJoin.getBuildOutput();
            final IntList required = hashJoin.getRequiredBuildColumns();
            for (int i = 0; i < required.size(); i++) {
                int column = requireColumn(buildBaseColumns, required.getQuick(i));
                buildColumns.add(column);
                // The payload stores the input's symbol keys, and probes resolve them through
                // the open build cursor's tables, so the input's capability carries over.
                TableColumnMetadata metadata = copyColumn(buildMetadata, column, buildMetadata.isSymbolTableStatic(column));
                payloadMetadata.add(metadata);
                joinedMetadata.add(hashJoin.getBuildAlias(), metadata);
                final int sourceIndex = buildOutput.getColumnIndexById(required.getQuick(i));
                if (buildOutput.getColumnType(sourceIndex) != buildMetadata.getColumnType(column)) {
                    throw new IllegalStateException("hash join input mapping type mismatch");
                }
                input.addColumnFrom(buildOutput, sourceIndex);
            }
            if (!hasInputs(hashJoin, input)) {
                throw new IllegalStateException("hash join input mapping misses a column");
            }
        } catch (Throwable th) {
            Misc.free(joinedMetadata, th);
            throw th;
        }
    }

    @Override
    public void close() {
        joinedMetadata.close();
    }

    /**
     * Compiled build-record indexes, in payload-field order.
     */
    public IntList getBuildColumns() {
        return buildColumns;
    }

    /**
     * Compiled build-record index of the INT layout's only key column.
     */
    public int getBuildKeyColumn() {
        return buildKeyColumns.getQuick(0);
    }

    /**
     * Compiled build-record indexes of every key column, in sink order.
     */
    public IntList getBuildKeyColumns() {
        return buildKeyColumns;
    }

    public String getCondition() {
        return condition;
    }

    public RecordMetadata getJoinedMetadata() {
        return joinedMetadata;
    }

    /**
     * Column types of the staged key, in sink order, which is the layout both key sinks write and
     * the layout the build's map stores. Empty for the INT layout, which stages no key.
     */
    public ColumnTypes getKeyTypes() {
        return keyTypes;
    }

    public RecordMetadata getPayloadMetadata() {
        return payloadMetadata;
    }

    /**
     * Column count of the probe input, which bounds every probe index a key sink reads.
     */
    public int getProbeColumnCount() {
        return probeColumnCount;
    }

    /**
     * Compiled probe-record index of the INT layout's only key column.
     */
    public int getProbeKeyColumn() {
        return probeKeyColumns.getQuick(0);
    }

    /**
     * Compiled probe-record indexes of every key column, in sink order.
     */
    public IntList getProbeKeyColumns() {
        return probeKeyColumns;
    }

    /**
     * Compiled build-record indexes of the SYMBOL key columns, in key order.
     */
    public IntList getSymbolKeyBuildColumns() {
        return symbolKeyBuildColumns;
    }

    /**
     * Compiled probe-record indexes of the SYMBOL key columns, in key order.
     */
    public IntList getSymbolKeyProbeColumns() {
        return symbolKeyProbeColumns;
    }

    /**
     * The INT layout's lone SYMBOL pair, whose probe keys translate into the build's domain.
     */
    public boolean isSymbolKey() {
        return isSymbolKey;
    }

    /**
     * True when the build may size its key table by its row count; see
     * {@link HashJoinAggregation#isKeyCapacityPresized()}.
     */
    public boolean isKeyCapacityPresized() {
        return isKeyCapacityPresized;
    }

    /**
     * True when the key reaches its map through the two key sinks rather than the INT layout.
     */
    public boolean isKeyStaged() {
        return isKeyStaged;
    }

    /**
     * A key sink over the build input's records. Sinks hold scratch state, so the owner's build
     * takes one of its own; see the {@link RecordSink} javadoc. Null for the INT layout.
     */
    public RecordSink newBuildKeySink() {
        return isKeyStaged() ? newKeySink(buildKeySinkClass, buildInputMetadata, buildKeyFilter,
                buildKeySymbolAsString, buildKeyStringAsVarchar, buildKeyTimestampAsNanos) : null;
    }

    /**
     * A key sink over the probe input's records. Sinks hold scratch state, so every worker takes
     * one of its own; see the {@link RecordSink} javadoc. Null for the INT layout.
     */
    public RecordSink newProbeKeySink() {
        return isKeyStaged() ? newKeySink(probeKeySinkClass, probeInputMetadata, probeKeyFilter,
                probeKeySymbolAsString, probeKeyStringAsVarchar, probeKeyTimestampAsNanos) : null;
    }

    public HashJoinGroupByRecord newRecord() {
        return new HashJoinGroupByRecord(probeColumnCount, payloadMetadata);
    }

    public OutputSchema getInput() {
        return input;
    }

    /**
     * Build payload types: the row heap copies a build column, so only a build column answers to
     * this set. A probe column reaches the joined record straight off its page frame and takes no
     * type check at all, and a key column and a grouping expression answer to
     * {@link HashJoinGroupByKeys#isSupportedKeyType(int)} instead. A build column that only the
     * build's filters read, its scan's WHERE filter and an outer join's ON condition on the build
     * alone, takes no type check either: the build runs them over its page frames, before the row
     * heap copies a row.
     */
    public static boolean isSupportedValueType(int type) {
        return switch (ColumnType.tagOf(type)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR,
                 ColumnType.INT, ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP,
                 ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.SYMBOL, ColumnType.IPv4,
                 ColumnType.UUID, ColumnType.LONG256, ColumnType.GEOBYTE, ColumnType.GEOSHORT,
                 ColumnType.GEOINT, ColumnType.GEOLONG, ColumnType.DECIMAL8, ColumnType.DECIMAL16,
                 ColumnType.DECIMAL32, ColumnType.DECIMAL64, ColumnType.DECIMAL128,
                 ColumnType.DECIMAL256 -> true;
            // STRING, VARCHAR, BINARY and ARRAY are variable-size, so the row heap cannot copy
            // them. INTERVAL is not persisted, so no base-table column carries one.
            default -> false;
        };
    }

    private static boolean hasInputs(HashJoinAggregation hashJoin, OutputSchema input) {
        final ObjList<BoundExpression> keys = hashJoin.getAggregate().getGroupingExpressions();
        for (int i = 0, n = keys.size(); i < n; i++) {
            if (!hasInputs(keys.getQuick(i), input)) {
                return false;
            }
        }
        final ObjList<FunctionExpression> aggregates = hashJoin.getAggregate().getAggregates();
        for (int i = 0, n = aggregates.size(); i < n; i++) {
            if (!hasInputs(aggregates.getQuick(i), input)) {
                return false;
            }
        }
        return hashJoin.getPostJoinFilter() == null || hasInputs(hashJoin.getPostJoinFilter(), input);
    }

    private static boolean hasInputs(BoundExpression expression, OutputSchema input) {
        if (expression instanceof ColumnExpression column) {
            final int index = input.getColumnIndexById(column.getColumnId());
            return index >= 0 && input.getColumnType(index) == column.getDataType();
        }
        if (expression instanceof FunctionExpression call) {
            for (int i = 0, n = call.getArgumentCount(); i < n; i++) {
                if (!hasInputs(call.argumentAt(i), input)) {
                    return false;
                }
            }
        }
        return true;
    }

    // The generated class is nullable by contract: null asks for the looping sink, which
    // RecordSinkFactory builds from the same filter and flags. Never branch on the class.
    private static RecordSink newKeySink(
            @Nullable Class<RecordSink> clazz,
            RecordMetadata inputMetadata,
            ListColumnFilter filter,
            BitSet symbolAsString,
            BitSet stringAsVarchar,
            BitSet timestampAsNanos
    ) {
        return RecordSinkFactory.getInstance(clazz, inputMetadata, filter, null, null,
                symbolAsString, stringAsVarchar, timestampAsNanos);
    }

    private static TableColumnMetadata copyColumn(RecordMetadata metadata, int column, boolean isSymbolTableStatic) {
        return new TableColumnMetadata(metadata.getColumnName(column), metadata.getColumnType(column),
                IndexType.NONE, 0, isSymbolTableStatic, null);
    }

    private static int indexOf(IntList columns, int value) {
        return columns.indexOf(value, 0, columns.size());
    }

    private static int requireColumn(IntList columns, int baseColumn) {
        int index = indexOf(columns, baseColumn);
        if (index < 0) {
            throw new IllegalStateException("missing hash join input column");
        }
        return index;
    }
}
