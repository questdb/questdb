package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PriorityMetadata;
import io.questdb.griffin.engine.functions.columns.ArrayColumn;
import io.questdb.griffin.engine.functions.columns.BinColumn;
import io.questdb.griffin.engine.functions.columns.BooleanColumn;
import io.questdb.griffin.engine.functions.columns.ByteColumn;
import io.questdb.griffin.engine.functions.columns.CharColumn;
import io.questdb.griffin.engine.functions.columns.DateColumn;
import io.questdb.griffin.engine.functions.columns.DecimalColumn;
import io.questdb.griffin.engine.functions.columns.DoubleColumn;
import io.questdb.griffin.engine.functions.columns.FloatColumn;
import io.questdb.griffin.engine.functions.columns.GeoByteColumn;
import io.questdb.griffin.engine.functions.columns.GeoIntColumn;
import io.questdb.griffin.engine.functions.columns.GeoLongColumn;
import io.questdb.griffin.engine.functions.columns.GeoShortColumn;
import io.questdb.griffin.engine.functions.columns.IntColumn;
import io.questdb.griffin.engine.functions.columns.IPv4Column;
import io.questdb.griffin.engine.functions.columns.Long128Column;
import io.questdb.griffin.engine.functions.columns.Long256Column;
import io.questdb.griffin.engine.functions.columns.LongColumn;
import io.questdb.griffin.engine.functions.columns.ShortColumn;
import io.questdb.griffin.engine.functions.columns.StrColumn;
import io.questdb.griffin.engine.functions.columns.SymbolColumn;
import io.questdb.griffin.engine.functions.columns.TimestampColumn;
import io.questdb.griffin.engine.functions.columns.UuidColumn;
import io.questdb.griffin.engine.functions.columns.VarcharColumn;
import io.questdb.griffin.engine.table.VirtualRecordCursorFactory;
import io.questdb.std.ObjList;

final class VirtualRecordCursorFactorySupport {
    private VirtualRecordCursorFactorySupport() {
    }

    /**
     * Builds a VirtualRecordCursorFactory projection over all columns of the base factory.
     * This forces a non-page-frame cursor path while preserving schema and values.
     */
    static RecordCursorFactory wrap(RecordCursorFactory base) {
        RecordMetadata baseMetadata = base.getMetadata();
        int columnCount = baseMetadata.getColumnCount();
        int virtualColumnReservedSlots = columnCount;
        PriorityMetadata priorityMetadata = new PriorityMetadata(virtualColumnReservedSlots, baseMetadata);
        GenericRecordMetadata virtualMetadata = new GenericRecordMetadata();
        ObjList<Function> functions = new ObjList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            TableColumnMetadata column = copyColumn(baseMetadata, i);
            virtualMetadata.add(column);
            priorityMetadata.add(column);
            // Offset base column references so virtual functions read from the slave record.
            functions.add(createColumnFunction(i, virtualColumnReservedSlots, column.getColumnType(), baseMetadata));
        }

        virtualMetadata.setTimestampIndex(baseMetadata.getTimestampIndex());
        return new VirtualRecordCursorFactory(
                virtualMetadata,
                priorityMetadata,
                functions,
                base,
                virtualColumnReservedSlots,
                true
        );
    }

    private static TableColumnMetadata copyColumn(RecordMetadata metadata, int columnIndex) {
        TableColumnMetadata column = metadata.getColumnMetadata(columnIndex);
        return new TableColumnMetadata(
                column.getColumnName(),
                column.getColumnType(),
                column.isSymbolIndexFlag(),
                column.getIndexValueBlockCapacity(),
                column.isSymbolTableStatic(),
                column.getMetadata(),
                column.getWriterIndex(),
                column.isDedupKeyFlag()
        );
    }

    private static Function createColumnFunction(
            int baseColumnIndex,
            int virtualColumnReservedSlots,
            int columnType,
            RecordMetadata metadata
    ) {
        int columnIndex = baseColumnIndex + virtualColumnReservedSlots;
        if (ColumnType.isArray(columnType)) {
            return ArrayColumn.newInstance(columnIndex, columnType);
        }
        if (ColumnType.isDecimal(columnType)) {
            return DecimalColumn.newInstance(columnIndex, columnType);
        }
        if (ColumnType.isTimestamp(columnType)) {
            return TimestampColumn.newInstance(columnIndex, columnType);
        }
        return switch (ColumnType.tagOf(columnType)) {
            case ColumnType.BOOLEAN -> BooleanColumn.newInstance(columnIndex);
            case ColumnType.BYTE -> ByteColumn.newInstance(columnIndex);
            case ColumnType.SHORT -> ShortColumn.newInstance(columnIndex);
            case ColumnType.CHAR -> new CharColumn(columnIndex);
            case ColumnType.INT -> IntColumn.newInstance(columnIndex);
            case ColumnType.LONG -> LongColumn.newInstance(columnIndex);
            case ColumnType.DATE -> DateColumn.newInstance(columnIndex);
            case ColumnType.FLOAT -> FloatColumn.newInstance(columnIndex);
            case ColumnType.DOUBLE -> DoubleColumn.newInstance(columnIndex);
            case ColumnType.STRING -> new StrColumn(columnIndex);
            case ColumnType.SYMBOL -> new SymbolColumn(columnIndex, metadata.isSymbolTableStatic(baseColumnIndex));
            case ColumnType.VARCHAR -> new VarcharColumn(columnIndex);
            case ColumnType.BINARY -> BinColumn.newInstance(columnIndex);
            case ColumnType.LONG128 -> Long128Column.newInstance(columnIndex);
            case ColumnType.LONG256 -> Long256Column.newInstance(columnIndex);
            case ColumnType.UUID -> UuidColumn.newInstance(columnIndex);
            case ColumnType.IPv4 -> IPv4Column.newInstance(columnIndex);
            case ColumnType.GEOBYTE -> GeoByteColumn.newInstance(columnIndex, columnType);
            case ColumnType.GEOSHORT -> GeoShortColumn.newInstance(columnIndex, columnType);
            case ColumnType.GEOINT -> GeoIntColumn.newInstance(columnIndex, columnType);
            case ColumnType.GEOLONG -> GeoLongColumn.newInstance(columnIndex, columnType);
            default -> throw new UnsupportedOperationException("virtual column type not supported: " + columnType);
        };
    }
}
