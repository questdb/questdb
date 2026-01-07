package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Chars;
import io.questdb.std.IntList;

public final class FuzzBuildContext {
    private final CairoConfiguration configuration;
    private final BytecodeAssembler asm;
    private final RecordComparatorCompiler comparatorCompiler;

    private final ListColumnFilter masterKeyColumns = new ListColumnFilter();
    private final ListColumnFilter slaveKeyColumns = new ListColumnFilter();
    private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
    private final BitSet writeSymbolAsString = new BitSet();
    private final BitSet writeStringAsVarcharA = new BitSet();
    private final BitSet writeStringAsVarcharB = new BitSet();
    private final BitSet writeTimestampAsNanosA = new BitSet();
    private final BitSet writeTimestampAsNanosB = new BitSet();

    public FuzzBuildContext(CairoConfiguration configuration) {
        this.configuration = configuration;
        this.asm = new BytecodeAssembler();
        this.comparatorCompiler = new RecordComparatorCompiler(asm);
    }

    /**
     * Exposes the configuration so factories can share engine defaults during construction.
     */
    public CairoConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Returns a shared assembler for building record sinks and comparators.
     */
    public BytecodeAssembler getAsm() {
        return asm;
    }

    /**
     * Returns a shared comparator compiler to keep ORDER BY construction consistent.
     */
    public RecordComparatorCompiler getComparatorCompiler() {
        return comparatorCompiler;
    }

    /**
     * Builds join key sinks and type coercion metadata for the chosen key columns.
     * This mirrors planner logic so the oracle observes the same join semantics.
     */
    public JoinKeyPlan planJoinKeys(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            IntList leftKeys,
            IntList rightKeys,
            boolean isSelfJoin
    ) {
        masterKeyColumns.clear();
        slaveKeyColumns.clear();
        keyTypes.clear();
        writeSymbolAsString.clear();
        writeStringAsVarcharA.clear();
        writeStringAsVarcharB.clear();
        writeTimestampAsNanosA.clear();
        writeTimestampAsNanosB.clear();

        for (int i = 0, n = leftKeys.size(); i < n; i++) {
            int masterIndex = leftKeys.getQuick(i);
            int slaveIndex = rightKeys.getQuick(i);
            masterKeyColumns.add(masterIndex + 1);
            slaveKeyColumns.add(slaveIndex + 1);

            int columnTypeA = slaveMetadata.getColumnType(slaveIndex);
            String columnNameA = slaveMetadata.getColumnName(slaveIndex);
            int columnTypeB = masterMetadata.getColumnType(masterIndex);
            String columnNameB = masterMetadata.getColumnName(masterIndex);

            if (columnTypeB != columnTypeA &&
                    !(ColumnType.isSymbolOrStringOrVarchar(columnTypeB) && ColumnType.isSymbolOrStringOrVarchar(columnTypeA)) &&
                    !(ColumnType.isTimestamp(columnTypeB) && ColumnType.isTimestamp(columnTypeA))
            ) {
                throw new IllegalArgumentException("join column type mismatch");
            }
            if (ColumnType.isVarchar(columnTypeA) || ColumnType.isVarchar(columnTypeB)) {
                keyTypes.add(ColumnType.VARCHAR);
                if (ColumnType.isVarchar(columnTypeA)) {
                    writeStringAsVarcharB.set(masterIndex);
                } else {
                    writeStringAsVarcharA.set(slaveIndex);
                }
                writeSymbolAsString.set(slaveIndex);
                writeSymbolAsString.set(masterIndex);
            } else if (columnTypeB == ColumnType.SYMBOL) {
                if (isSelfJoin && Chars.equalsIgnoreCase(columnNameA, columnNameB)) {
                    keyTypes.add(ColumnType.SYMBOL);
                } else {
                    keyTypes.add(ColumnType.STRING);
                    writeSymbolAsString.set(slaveIndex);
                    writeSymbolAsString.set(masterIndex);
                }
            } else if (ColumnType.isString(columnTypeA) || ColumnType.isString(columnTypeB)) {
                keyTypes.add(columnTypeB);
                writeSymbolAsString.set(slaveIndex);
                writeSymbolAsString.set(masterIndex);
            } else if (columnTypeA != columnTypeB && ColumnType.isTimestamp(columnTypeA) && ColumnType.isTimestamp(columnTypeB)) {
                keyTypes.add(ColumnType.TIMESTAMP_NANO);
                if (!ColumnType.isTimestampNano(columnTypeA)) {
                    writeTimestampAsNanosA.set(slaveIndex);
                }
                if (!ColumnType.isTimestampNano(columnTypeB)) {
                    writeTimestampAsNanosB.set(masterIndex);
                }
            } else {
                keyTypes.add(columnTypeB);
            }
        }

        ListColumnFilter masterCopy = masterKeyColumns.copy();
        ListColumnFilter slaveCopy = slaveKeyColumns.copy();
        int maxColumns = Math.max(masterMetadata.getColumnCount(), slaveMetadata.getColumnCount());
        BitSet writeSymbolCopy = copyBitSet(writeSymbolAsString, maxColumns);
        BitSet writeStringAsVarcharACopy = copyBitSet(writeStringAsVarcharA, maxColumns);
        BitSet writeStringAsVarcharBCopy = copyBitSet(writeStringAsVarcharB, maxColumns);
        BitSet writeTimestampAsNanosACopy = copyBitSet(writeTimestampAsNanosA, maxColumns);
        BitSet writeTimestampAsNanosBCopy = copyBitSet(writeTimestampAsNanosB, maxColumns);
        RecordSink masterKeySink = RecordSinkFactory.getInstance(
                asm,
                masterMetadata,
                masterCopy,
                writeSymbolCopy,
                writeStringAsVarcharBCopy,
                writeTimestampAsNanosBCopy,
                configuration
        );
        RecordSink slaveKeySink = RecordSinkFactory.getInstance(
                asm,
                slaveMetadata,
                slaveCopy,
                writeSymbolCopy,
                writeStringAsVarcharACopy,
                writeTimestampAsNanosACopy,
                configuration
        );
        return new JoinKeyPlan(
                masterCopy,
                slaveCopy,
                new ArrayColumnTypes().addAll(keyTypes),
                masterKeySink,
                slaveKeySink
        );
    }

    private static BitSet copyBitSet(BitSet source, int maxColumns) {
        BitSet copy = new BitSet(maxColumns + 1);
        for (int i = 0; i < maxColumns; i++) {
            if (source.get(i)) {
                copy.set(i);
            }
        }
        return copy;
    }

    public JoinRecordMetadata createJoinMetadata(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            int timestampIndex
    ) {
        return createJoinMetadata(masterMetadata, slaveMetadata, timestampIndex, "slave");
    }

    /**
     * Builds join output metadata with the provided alias to match engine projection.
     */
    public JoinRecordMetadata createJoinMetadata(
            RecordMetadata masterMetadata,
            RecordMetadata slaveMetadata,
            int timestampIndex,
            String slaveAlias
    ) {
        JoinRecordMetadata metadata = new JoinRecordMetadata(
                configuration,
                masterMetadata.getColumnCount() + slaveMetadata.getColumnCount()
        );
        metadata.copyColumnMetadataFrom(null, masterMetadata);
        metadata.copyColumnMetadataFrom(slaveAlias, slaveMetadata);
        if (timestampIndex != -1) {
            metadata.setTimestampIndex(timestampIndex);
        }
        return metadata;
    }
}
