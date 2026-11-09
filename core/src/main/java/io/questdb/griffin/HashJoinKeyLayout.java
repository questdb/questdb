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
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

/**
 * Compiles the reconciled equality keys of one hash join against its two compiled inputs: the
 * probe input, whose rows look keys up, and the build input, whose rows the join hashes. The
 * keys' column indexes address whatever columns the caller's input mappings name; each mapping
 * lists, per compiled-record index of its input, the column that index holds, so this class turns
 * every key column into a compiled-record index.
 * <p>
 * A lone INT pair and a lone SYMBOL pair keep the narrow INT layout, which reads the key straight
 * off the input record. A SYMBOL key column compares as an int on both layouts: the probe
 * translates its symbol key into the build's domain, so this class publishes those key columns for
 * the caller to bind a translator to.
 * <p>
 * A key the narrow INT layout cannot carry reaches its map through a {@link RecordSink} per input.
 * This class generates the two sink classes and hands out instances, one per worker, through
 * {@link #newProbeKeySink()} and {@link #newBuildKeySink()}; it borrows both input metadatas for
 * that, so instantiate while the inputs are alive. The INT layout stages nothing, so it gets no key
 * types and no sinks. The class holds no native memory.
 */
public final class HashJoinKeyLayout {
    // Borrowed for the looping sink, which reads the source types when a caller instantiates it.
    private final RecordMetadata buildInputMetadata;
    private final IntList buildKeyColumns = new IntList();
    private final ListColumnFilter buildKeyFilter = new ListColumnFilter();
    @Nullable
    private final Class<RecordSink> buildKeySinkClass;
    private final BitSet buildKeyStringAsVarchar = new BitSet();
    private final BitSet buildKeySymbolAsString = new BitSet();
    private final BitSet buildKeyTimestampAsNanos = new BitSet();
    private final String condition;
    private final boolean hasStaticSymbolTables;
    private final HashJoinGroupByKeys keys;
    private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
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

    /**
     * @param keys              the reconciled keys, whose column indexes address the columns the two
     *                          mappings name; borrowed for the layout's lifetime
     * @param probeAlias        the probe input's alias, which the condition text names
     * @param probeMetadata     the compiled probe input's metadata, borrowed for the key sinks
     * @param probeBaseColumns  per compiled probe-record index, the column that index holds
     * @param buildAlias        the build input's alias, which the condition text names
     * @param buildMetadata     the compiled build input's metadata, borrowed for the key sinks
     * @param buildBaseColumns  per compiled build-record index, the column that index holds
     */
    public HashJoinKeyLayout(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            HashJoinGroupByKeys keys,
            CharSequence probeAlias,
            RecordMetadata probeMetadata,
            IntList probeBaseColumns,
            CharSequence buildAlias,
            RecordMetadata buildMetadata,
            IntList buildBaseColumns
    ) throws SqlException {
        if (probeBaseColumns.size() != probeMetadata.getColumnCount()
                || buildBaseColumns.size() != buildMetadata.getColumnCount()) {
            throw SqlException.$(0, "hash join input mapping size mismatch");
        }
        this.keys = keys;
        // Symbol keys translate through the inputs' static tables.
        boolean hasStaticSymbolTables = true;
        StringSink conditionSink = Misc.getThreadLocalSink();
        for (int i = 0, n = keys.size(); i < n; i++) {
            final int probeColumn = requireColumn(probeBaseColumns, keys.getProbeColumn(i));
            final int buildColumn = requireColumn(buildBaseColumns, keys.getBuildColumn(i));
            // The inputs compiled after the analysis, so they carry the analysed types or nothing.
            if (probeMetadata.getColumnType(probeColumn) != keys.getProbeType(i)
                    || buildMetadata.getColumnType(buildColumn) != keys.getBuildType(i)) {
                throw SqlException.$(0, "hash join input key type mismatch");
            }
            if (ColumnType.isSymbol(keys.getProbeType(i)) && !probeMetadata.isSymbolTableStatic(probeColumn)) {
                hasStaticSymbolTables = false;
            }
            if (ColumnType.isSymbol(keys.getBuildType(i)) && !buildMetadata.isSymbolTableStatic(buildColumn)) {
                hasStaticSymbolTables = false;
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
            conditionSink.put(probeAlias).putAscii('.').put(probeMetadata.getColumnName(probeColumn))
                    .putAscii('=').put(buildAlias).putAscii('.').put(buildMetadata.getColumnName(buildColumn));
        }
        this.hasStaticSymbolTables = hasStaticSymbolTables;
        condition = conditionSink.toString();
        probeInputMetadata = probeMetadata;
        buildInputMetadata = buildMetadata;
        if (keys.isIntKeyed()) {
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

    /**
     * The equality keys as EXPLAIN prints them: {@code probe.a=build.b and ...}, by compiled
     * column name.
     */
    public String getCondition() {
        return condition;
    }

    /**
     * Column types of the staged key, in sink order, which is the layout both key sinks write and
     * the layout the build's map stores. Empty for the INT layout, which stages no key.
     */
    public ColumnTypes getKeyTypes() {
        return keyTypes;
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
     * False when a SYMBOL key column of either input lacks a static symbol table. Both sides of a
     * SYMBOL key need one: the probe resolves its key's text and the build answers
     * {@code keyOf()} for it, whichever layout carries the key.
     */
    public boolean hasStaticSymbolTables() {
        return hasStaticSymbolTables;
    }

    /**
     * True when the key reaches its map through the two key sinks rather than the INT layout.
     */
    public boolean isKeyStaged() {
        return !keys.isIntKeyed();
    }

    /**
     * The INT layout's lone SYMBOL pair, whose probe keys translate into the build's domain.
     */
    public boolean isSymbolKey() {
        return keys.isSymbolKey();
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

    /**
     * The compiled-record index whose mapping entry is {@code baseColumn}.
     */
    static int requireColumn(IntList columns, int baseColumn) throws SqlException {
        int index = columns.indexOf(baseColumn, 0, columns.size());
        if (index < 0) {
            throw SqlException.$(0, "missing hash join input column: ").put(baseColumn);
        }
        return index;
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
}
