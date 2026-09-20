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
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Compiles base-table references into the actual probe projection and pruned build
 * payload. Joined columns are the physical probe columns followed by payload fields;
 * expressions retain their logical aliases even when RIGHT becomes LEFT OUTER.
 * Input mappings are compiled-record index -> base-table index, not writer indexes.
 * Build SYMBOL payloads keep the build input's symbol keys and its static symbol tables.
 * Construct before the candidate's borrowed models are mutated or the compiler reused.
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
    private final ExpressionNode buildOnFilter;
    private final ObjList<QueryColumn> columns = new ObjList<>();
    private final String condition;
    private final boolean hasStaticSymbolTables;
    private final HashJoinGroupByKeys keys;
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
    private ExpressionNode postJoinFilter;

    public HashJoinGroupByMetadata(
            CairoConfiguration configuration,
            BytecodeAssembler asm,
            HashJoinGroupByCandidate candidate,
            RecordMetadata probeMetadata,
            IntList probeBaseColumns,
            RecordMetadata buildMetadata,
            IntList buildBaseColumns
    ) throws SqlException {
        if (probeBaseColumns.size() != probeMetadata.getColumnCount()
                || buildBaseColumns.size() != buildMetadata.getColumnCount()) {
            throw SqlException.$(0, "hash join input mapping size mismatch");
        }
        probeColumnCount = probeMetadata.getColumnCount();
        keys = candidate.getKeys();
        // Symbol keys translate through, and payload symbols resolve with, the inputs' static tables.
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
            conditionSink.put(candidate.getProbeModel().getName()).putAscii('.').put(probeMetadata.getColumnName(probeColumn))
                    .putAscii('=').put(candidate.getBuildModel().getName()).putAscii('.').put(buildMetadata.getColumnName(buildColumn));
        }
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
        joinedMetadata = new JoinRecordMetadata(configuration,
                probeColumnCount + candidate.getRequiredBuildColumns().size());
        try {
            for (int i = 0; i < probeColumnCount; i++) {
                joinedMetadata.add(candidate.getProbeModel().getName(), copyColumn(probeMetadata, i, probeMetadata.isSymbolTableStatic(i)));
            }
            IntList required = candidate.getRequiredBuildColumns();
            for (int i = 0; i < required.size(); i++) {
                int column = requireColumn(buildBaseColumns, required.getQuick(i));
                buildColumns.add(column);
                // The payload stores the input's symbol keys, and probes resolve them through
                // the open build cursor's tables, so the input's capability carries over.
                boolean isSymbolTableStatic = buildMetadata.isSymbolTableStatic(column);
                if (ColumnType.isSymbol(buildMetadata.getColumnType(column)) && !isSymbolTableStatic) {
                    hasStaticSymbolTables = false;
                }
                TableColumnMetadata metadata = copyColumn(buildMetadata, column, isSymbolTableStatic);
                payloadMetadata.add(metadata);
                joinedMetadata.add(candidate.getBuildModel().getName(), metadata);
            }
            IntList resolvedToJoined = new IntList();
            RecordMetadata resolved = candidate.getResolvedMetadata();
            for (int i = 0; i < resolved.getColumnCount(); i++) {
                int baseColumn = candidate.getBaseColumnIndex(i);
                boolean build = candidate.isBuildColumn(i);
                int column = indexOf(build ? buildBaseColumns : probeBaseColumns, baseColumn);
                if (column >= 0 && (build ? buildMetadata : probeMetadata).getColumnType(column) != resolved.getColumnType(i)) {
                    throw SqlException.$(0, "hash join input mapping type mismatch [column=").put(baseColumn).put(']');
                }
                int payload = build ? indexOf(required, baseColumn) : -1;
                resolvedToJoined.add(build ? (payload < 0 ? -1 : probeColumnCount + payload) : column);
            }
            IntList resolvedToBuild = new IntList();
            for (int i = 0; i < resolved.getColumnCount(); i++) {
                resolvedToBuild.add(candidate.isBuildColumn(i) ? indexOf(buildBaseColumns, candidate.getBaseColumnIndex(i)) : -1);
            }
            buildOnFilter = remap(candidate.getResolvedBuildOnFilter(), resolved, resolvedToBuild, buildMetadata);
            for (int i = 0; i < candidate.getResolvedColumns().size(); i++) {
                QueryColumn column = candidate.getResolvedColumns().getQuick(i);
                columns.add(QueryColumn.FACTORY.newInstance().of(column.getAlias(),
                        remap(column.getAst(), resolved, resolvedToJoined, joinedMetadata)));
            }
            for (int i = 0; i < candidate.getResolvedPostJoinFilters().size(); i++) {
                ExpressionNode filter = remap(candidate.getResolvedPostJoinFilters().getQuick(i), resolved, resolvedToJoined, joinedMetadata);
                if (postJoinFilter == null) {
                    postJoinFilter = filter;
                } else {
                    ExpressionNode and = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.OPERATION, "and", 0, filter.position);
                    and.paramCount = 2;
                    and.lhs = postJoinFilter;
                    and.rhs = filter;
                    postJoinFilter = and;
                }
            }
            this.hasStaticSymbolTables = hasStaticSymbolTables;
        } catch (Throwable th) {
            Misc.free(joinedMetadata, th);
            throw th;
        }
    }

    @Override
    public void close() {
        joinedMetadata.close();
    }

    /** Compiled build-record indexes, in payload-field order. */
    public IntList getBuildColumns() {
        return buildColumns;
    }

    /** Compiled build-record index of the INT layout's only key column. */
    public int getBuildKeyColumn() {
        return buildKeyColumns.getQuick(0);
    }

    /** Compiled build-record indexes of every key column, in sink order. */
    public IntList getBuildKeyColumns() {
        return buildKeyColumns;
    }

    public String getCondition() {
        return condition;
    }

    public RecordMetadata getJoinedMetadata() {
        return joinedMetadata;
    }

    /** Reconciled equality keys, whose column indexes address the analysed base tables. */
    public HashJoinGroupByKeys getKeys() {
        return keys;
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

    /** Column count of the probe input, which bounds every probe index a key sink reads. */
    public int getProbeColumnCount() {
        return probeColumnCount;
    }

    /** Compiled probe-record index of the INT layout's only key column. */
    public int getProbeKeyColumn() {
        return probeKeyColumns.getQuick(0);
    }

    /** Compiled probe-record indexes of every key column, in sink order. */
    public IntList getProbeKeyColumns() {
        return probeKeyColumns;
    }

    /** Compiled build-record indexes of the SYMBOL key columns, in key order. */
    public IntList getSymbolKeyBuildColumns() {
        return symbolKeyBuildColumns;
    }

    /** Compiled probe-record indexes of the SYMBOL key columns, in key order. */
    public IntList getSymbolKeyProbeColumns() {
        return symbolKeyProbeColumns;
    }

    /**
     * False when a SYMBOL key column of either input, or a build SYMBOL payload column, lacks a
     * static symbol table. Base-table columns always have one; the planner keeps the ordinary
     * plan otherwise. Both sides of a SYMBOL key need one: the probe resolves its key's text and
     * the build answers {@code keyOf()} for it, whichever layout carries the key.
     */
    public boolean hasStaticSymbolTables() {
        return hasStaticSymbolTables;
    }

    /** The INT layout's lone SYMBOL pair, whose probe keys translate into the build's domain. */
    public boolean isSymbolKey() {
        return keys.isSymbolKey();
    }

    /** True when the key reaches its map through the two key sinks rather than the INT layout. */
    public boolean isKeyStaged() {
        return !keys.isIntKeyed();
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

    ExpressionNode getBuildOnFilter() {
        return buildOnFilter;
    }

    ObjList<QueryColumn> getColumns() {
        return columns;
    }

    ExpressionNode getPostJoinFilter() {
        return postJoinFilter;
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

    private static TableColumnMetadata copyColumn(RecordMetadata metadata, int column, boolean symbolTableStatic) {
        return new TableColumnMetadata(metadata.getColumnName(column), metadata.getColumnType(column),
                IndexType.NONE, 0, symbolTableStatic, null);
    }

    private static int indexOf(IntList columns, int value) {
        return columns.indexOf(value, 0, columns.size());
    }

    private static ExpressionNode remap(ExpressionNode node, RecordMetadata resolved, IntList indexes, RecordMetadata target) throws SqlException {
        if (node == null) {
            return null;
        }
        CharSequence token = node.token;
        if (node.type == ExpressionNode.LITERAL) {
            int index = indexes.getQuick(resolved.getColumnIndex(node.token));
            if (index < 0) {
                throw SqlException.$(node.position, "missing hash join input column: ").put(node.token);
            }
            token = target.getColumnName(index);
        }
        ExpressionNode copy = ExpressionNode.FACTORY.newInstance().of(node.type, token, node.precedence, node.position);
        copy.paramCount = node.paramCount;
        copy.lhs = remap(node.lhs, resolved, indexes, target);
        copy.rhs = remap(node.rhs, resolved, indexes, target);
        for (int i = 0; i < node.args.size(); i++) {
            copy.args.add(remap(node.args.getQuick(i), resolved, indexes, target));
        }
        return copy;
    }

    private static int requireColumn(IntList columns, int baseColumn) throws SqlException {
        int index = indexOf(columns, baseColumn);
        if (index < 0) {
            throw SqlException.$(0, "missing hash join input column: ").put(baseColumn);
        }
        return index;
    }
}
