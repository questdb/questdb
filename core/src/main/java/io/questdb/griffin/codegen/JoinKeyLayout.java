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

package io.questdb.griffin.codegen;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.RecordSinkTemplate;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.LogicalPlans;
import io.questdb.griffin.engine.join.SymbolJoinKeyMapping;
import io.questdb.griffin.engine.join.SymbolKeyMappingRecordCopier;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;

import static io.questdb.cairo.ColumnType.*;

/**
 * The equality keys of a join: the key columns of both sides, the map key types, and the per-column conversions the
 * key sinks apply so that both sides write every key with its map key type.
 */
final class JoinKeyLayout implements Mutable {
    final ArrayColumnTypes keyTypes;
    final ListColumnFilter masterColumns;
    final IntList masterIndexes;
    final ListColumnFilter slaveColumns;
    final IntList slaveIndexes;
    private final BitSet masterStringAsVarchar = new BitSet();
    private final BitSet masterSymbolAsString;
    private final IntList masterSymbolColumns;
    private final BitSet masterTimestampAsNanos = new BitSet();
    private final BitSet slaveStringAsVarchar = new BitSet();
    private final BitSet slaveSymbolAsString = new BitSet();
    private final IntList slaveSymbolColumns;
    private final BitSet slaveTimestampAsNanos = new BitSet();
    private final IntList symbolKeyFlags;
    RecordSink masterSink;
    int[] masterSymbolKeys;
    RecordSink slaveSink;
    int[] slaveSymbolKeys;

    JoinKeyLayout(IntList symbolKeyFlags, IntList masterSymbolColumns, IntList slaveSymbolColumns) {
        this(new ListColumnFilter(), new ListColumnFilter(), new ArrayColumnTypes(), new IntList(), new IntList(), new BitSet(),
                symbolKeyFlags, masterSymbolColumns, slaveSymbolColumns);
    }

    JoinKeyLayout(
            ListColumnFilter slaveColumns,
            ListColumnFilter masterColumns,
            ArrayColumnTypes keyTypes,
            IntList masterIndexes,
            IntList slaveIndexes,
            BitSet masterSymbolAsString,
            IntList symbolKeyFlags,
            IntList masterSymbolColumns,
            IntList slaveSymbolColumns
    ) {
        this.slaveColumns = slaveColumns;
        this.masterColumns = masterColumns;
        this.keyTypes = keyTypes;
        this.masterIndexes = masterIndexes;
        this.slaveIndexes = slaveIndexes;
        this.masterSymbolAsString = masterSymbolAsString;
        this.symbolKeyFlags = symbolKeyFlags;
        this.masterSymbolColumns = masterSymbolColumns;
        this.slaveSymbolColumns = slaveSymbolColumns;
    }

    @Override
    public void clear() {
        keyTypes.clear();
        masterColumns.clear();
        slaveColumns.clear();
        masterIndexes.clear();
        slaveIndexes.clear();
        masterStringAsVarchar.clear();
        masterSymbolAsString.clear();
        masterTimestampAsNanos.clear();
        slaveStringAsVarchar.clear();
        slaveSymbolAsString.clear();
        slaveTimestampAsNanos.clear();
        clearSinks();
    }

    private void alignSharedKeyTypes(RecordMetadata masterMetadata, RecordMetadata slaveMetadata) {
        boolean isChanged = true;
        while (isChanged) {
            isChanged = false;
            for (int k = 0, m = slaveColumns.getColumnCount(); k < m; k++) {
                final int columnIndexA = slaveColumns.getColumnIndexFactored(k);
                final int columnIndexB = masterColumns.getColumnIndexFactored(k);
                final int keyType = keyTypes.getColumnType(k);
                if (keyType == STRING || keyType == ColumnType.SYMBOL) {
                    if (slaveStringAsVarchar.get(columnIndexA) || masterStringAsVarchar.get(columnIndexB)) {
                        keyTypes.set(k, VARCHAR);
                        if (!isVarchar(slaveMetadata.getColumnType(columnIndexA))) {
                            slaveStringAsVarchar.set(columnIndexA);
                        }
                        if (!isVarchar(masterMetadata.getColumnType(columnIndexB))) {
                            masterStringAsVarchar.set(columnIndexB);
                        }
                        slaveSymbolAsString.set(columnIndexA);
                        masterSymbolAsString.set(columnIndexB);
                        isChanged = true;
                    } else if (keyType == ColumnType.SYMBOL
                            && (slaveSymbolAsString.get(columnIndexA) || masterSymbolAsString.get(columnIndexB))) {
                        keyTypes.set(k, STRING);
                        slaveSymbolAsString.set(columnIndexA);
                        masterSymbolAsString.set(columnIndexB);
                        isChanged = true;
                    }
                } else if (isTimestamp(keyType) && !isTimestampNano(keyType)
                        && (slaveTimestampAsNanos.get(columnIndexA) || masterTimestampAsNanos.get(columnIndexB))) {
                    keyTypes.set(k, TIMESTAMP_NANO);
                    slaveTimestampAsNanos.set(columnIndexA);
                    masterTimestampAsNanos.set(columnIndexB);
                    isChanged = true;
                }
            }
        }
    }

    private void clearSinks() {
        masterSink = null;
        masterSymbolKeys = null;
        slaveSink = null;
        slaveSymbolKeys = null;
    }

    /**
     * Converts SYMBOL-SYMBOL join key pairs from string-based comparison to integer-based
     * comparison using SymbolTranslatingRecord. For each SYMBOL-SYMBOL pair where both
     * sides currently write the symbol as a string (i.e., non-self-join pairs), this method:
     * <ul>
     *   <li>Unsets the symbol-as-string conversion of the master and the slave column</li>
     *   <li>Changes the keyTypes entry from STRING to INT</li>
     *   <li>Collects master/slave column indices into {@link #masterSymbolKeys} and {@link #slaveSymbolKeys},
     *   which stay null when no pair converts</li>
     * </ul>
     * Must be called after the symbol short circuit is created and before the key sinks.
     */
    void convertSymbolKeysToInt(RecordMetadata masterMetadata, RecordMetadata slaveMetadata) {
        final int keyCount = slaveColumns.getColumnCount();
        symbolKeyFlags.setAll(keyCount, 0);
        for (int k = 0; k < keyCount; k++) {
            final int slaveColIndex = slaveColumns.getColumnIndexFactored(k);
            final int masterColIndex = masterColumns.getColumnIndexFactored(k);
            if (keyTypes.getColumnType(k) == STRING
                    && masterMetadata.getColumnType(masterColIndex) == ColumnType.SYMBOL
                    && slaveMetadata.getColumnType(slaveColIndex) == ColumnType.SYMBOL
                    && masterMetadata.isSymbolTableStatic(masterColIndex)
                    && slaveMetadata.isSymbolTableStatic(slaveColIndex)
                    && masterSymbolAsString.get(masterColIndex)
                    && slaveSymbolAsString.get(slaveColIndex)) {
                symbolKeyFlags.setQuick(k, 1);
            }
        }
        // SymbolTranslatingRecord keeps one translation per column of the translated side,
        // which is the master or, after a hash join swap, the slave. A column paired with two
        // different partner columns needs two translations, so its pairs keep string comparison.
        for (int k = 0; k < keyCount; k++) {
            if (symbolKeyFlags.getQuick(k) == 1) {
                final int slaveColIndex = slaveColumns.getColumnIndexFactored(k);
                final int masterColIndex = masterColumns.getColumnIndexFactored(k);
                for (int j = k + 1; j < keyCount; j++) {
                    if (symbolKeyFlags.getQuick(j) == 1) {
                        final boolean isSameSlave = slaveColumns.getColumnIndexFactored(j) == slaveColIndex;
                        final boolean isSameMaster = masterColumns.getColumnIndexFactored(j) == masterColIndex;
                        if (isSameSlave != isSameMaster) {
                            symbolKeyFlags.setQuick(k, 0);
                            symbolKeyFlags.setQuick(j, 0);
                        }
                    }
                }
            }
        }
        // Record sinks convert per column, so a column shared with a key pair that keeps
        // string comparison must keep it in every pair.
        boolean isChanged = true;
        while (isChanged) {
            isChanged = false;
            for (int k = 0; k < keyCount; k++) {
                if (symbolKeyFlags.getQuick(k) == 1) {
                    for (int j = 0; j < keyCount; j++) {
                        if (symbolKeyFlags.getQuick(j) == 0
                                && (slaveColumns.getColumnIndexFactored(j) == slaveColumns.getColumnIndexFactored(k)
                                || masterColumns.getColumnIndexFactored(j) == masterColumns.getColumnIndexFactored(k))) {
                            symbolKeyFlags.setQuick(k, 0);
                            isChanged = true;
                            break;
                        }
                    }
                }
            }
        }
        masterSymbolColumns.clear();
        slaveSymbolColumns.clear();
        for (int k = 0; k < keyCount; k++) {
            if (symbolKeyFlags.getQuick(k) == 1) {
                keyTypes.set(k, ColumnType.INT);
                masterSymbolColumns.add(masterColumns.getColumnIndexFactored(k));
                slaveSymbolColumns.add(slaveColumns.getColumnIndexFactored(k));
            }
        }
        if (masterSymbolColumns.size() > 0) {
            // Unset the bits AFTER the loop, so that a column used by more than one
            // key pair keeps its bit until the loop has checked every pair
            for (int i = 0, n = masterSymbolColumns.size(); i < n; i++) {
                masterSymbolAsString.unset(masterSymbolColumns.getQuick(i));
                slaveSymbolAsString.unset(slaveSymbolColumns.getQuick(i));
            }
            masterSymbolKeys = masterSymbolColumns.toArray();
            slaveSymbolKeys = slaveSymbolColumns.toArray();
        } else {
            masterSymbolKeys = null;
            slaveSymbolKeys = null;
        }
    }

    RecordSink createMasterSink(CairoConfiguration configuration, BytecodeAssembler asm, RecordMetadata masterMetadata) {
        return RecordSinkFactory.getInstance(configuration, asm, masterMetadata, masterColumns, masterSymbolAsString,
                masterStringAsVarchar, masterTimestampAsNanos);
    }

    RecordSink createSlaveSink(CairoConfiguration configuration, BytecodeAssembler asm, RecordMetadata slaveMetadata) {
        return RecordSinkFactory.getInstance(configuration, asm, slaveMetadata, slaveColumns, slaveSymbolAsString,
                slaveStringAsVarchar, slaveTimestampAsNanos);
    }

    /**
     * The template of master key sinks, which keeps this layout's filter and bit sets, so the layout must not be
     * reused while the template is live.
     */
    RecordSinkTemplate masterSinkTemplate(CairoConfiguration configuration, BytecodeAssembler asm, RecordMetadata masterMetadata) {
        return new RecordSinkTemplate(configuration, asm, masterMetadata, masterColumns, masterSymbolAsString,
                masterStringAsVarchar, masterTimestampAsNanos);
    }

    /**
     * Lays out the keys pairing each master column index with the slave column index at the same position.
     */
    void of(RecordMetadata masterMetadata, RecordMetadata slaveMetadata, IntList masterIndexes, IntList slaveIndexes, boolean isSelfJoin) {
        slaveColumns.clear();
        masterColumns.clear();
        for (int i = 0, n = slaveIndexes.size(); i < n; i++) {
            slaveColumns.add(slaveIndexes.getQuick(i) + 1);
            masterColumns.add(masterIndexes.getQuick(i) + 1);
        }
        keyTypes.clear();
        slaveSymbolAsString.clear();
        masterSymbolAsString.clear();
        slaveStringAsVarchar.clear();
        masterStringAsVarchar.clear();
        slaveTimestampAsNanos.clear();
        masterTimestampAsNanos.clear();
        clearSinks();
        for (int k = 0, m = slaveColumns.getColumnCount(); k < m; k++) {
            // Don't use tagOf(columnType) to compare the types.
            // Key types have too much exactly except SYMBOL and STRING special case
            final int columnIndexA = slaveColumns.getColumnIndexFactored(k);
            final int columnIndexB = masterColumns.getColumnIndexFactored(k);
            final int columnTypeA = slaveMetadata.getColumnType(columnIndexA);
            final String columnNameA = slaveMetadata.getColumnName(columnIndexA);
            final int columnTypeB = masterMetadata.getColumnType(columnIndexB);
            final String columnNameB = masterMetadata.getColumnName(columnIndexB);
            assert LogicalPlans.isJoinKeyTypeCompatible(columnTypeB, columnTypeA);
            if (isVarchar(columnTypeA) || isVarchar(columnTypeB)) {
                keyTypes.add(VARCHAR);
                if (isVarchar(columnTypeA)) {
                    masterStringAsVarchar.set(columnIndexB);
                } else {
                    slaveStringAsVarchar.set(columnIndexA);
                }
                slaveSymbolAsString.set(columnIndexA);
                masterSymbolAsString.set(columnIndexB);
            } else if (columnTypeB == ColumnType.SYMBOL) {
                if (isSelfJoin && Chars.equalsIgnoreCase(columnNameA, columnNameB)) {
                    keyTypes.add(ColumnType.SYMBOL);
                } else {
                    keyTypes.add(STRING);
                    slaveSymbolAsString.set(columnIndexA);
                    masterSymbolAsString.set(columnIndexB);
                }
            } else if (isString(columnTypeA) || isString(columnTypeB)) {
                keyTypes.add(columnTypeB);
                slaveSymbolAsString.set(columnIndexA);
                masterSymbolAsString.set(columnIndexB);
            } else if (columnTypeA != columnTypeB &&
                    isTimestamp(columnTypeA) && isTimestamp(columnTypeB)
            ) {
                keyTypes.add(TIMESTAMP_NANO);
                // Mark columns that need conversion to nanoseconds
                if (!isTimestampNano(columnTypeA)) {
                    slaveTimestampAsNanos.set(columnIndexA);
                }
                if (!isTimestampNano(columnTypeB)) {
                    masterTimestampAsNanos.set(columnIndexB);
                }
            } else {
                keyTypes.add(columnTypeB);
            }
        }
        alignSharedKeyTypes(masterMetadata, slaveMetadata);
    }

    /**
     * Converts the SYMBOL-SYMBOL key pairs to INT where it can and creates both key sinks into {@link #masterSink}
     * and {@link #slaveSink}.
     */
    void prepareSinks(CairoConfiguration configuration, BytecodeAssembler asm, RecordMetadata masterMetadata, RecordMetadata slaveMetadata) {
        convertSymbolKeysToInt(masterMetadata, slaveMetadata);
        masterSink = createMasterSink(configuration, asm, masterMetadata);
        slaveSink = createSlaveSink(configuration, asm, slaveMetadata);
    }

    /**
     * Creates the key sinks of the single static SYMBOL key that the mapping translates from the master's symbol keys
     * to the slave's, so both sides write the slave's symbol key as an INT.
     */
    void prepareSymbolMappingSinks(CairoConfiguration configuration, BytecodeAssembler asm, RecordMetadata slaveMetadata, SymbolJoinKeyMapping mapping) {
        slaveSymbolAsString.unset(slaveColumns.getColumnIndexFactored(0));
        keyTypes.clear();
        keyTypes.add(ColumnType.INT);
        masterSymbolKeys = null;
        slaveSymbolKeys = null;
        masterSink = new SymbolKeyMappingRecordCopier(mapping);
        slaveSink = createSlaveSink(configuration, asm, slaveMetadata);
    }

    /**
     * The template of slave key sinks, which keeps this layout's filter and bit sets, so the layout must not be
     * reused while the template is live.
     */
    RecordSinkTemplate slaveSinkTemplate(CairoConfiguration configuration, BytecodeAssembler asm, RecordMetadata slaveMetadata) {
        return new RecordSinkTemplate(configuration, asm, slaveMetadata, slaveColumns, slaveSymbolAsString,
                slaveStringAsVarchar, slaveTimestampAsNanos);
    }
}
