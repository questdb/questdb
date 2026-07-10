/*******************************************************************************
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

package io.questdb.cairo;

import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.Decimal128;
import io.questdb.std.Decimal256;
import io.questdb.std.Decimal64;
import io.questdb.std.Decimals;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;

final class ParquetColumnTypeConverter {
    // Write-time hint mirrored by Rust's parquet encoder. It does not change
    // parquet schema repetition; SYMBOL columns remain Optional.
    static final int PARQUET_SYMBOL_NOT_NULL_HINT = Integer.MIN_VALUE;

    private ParquetColumnTypeConverter() {
    }

    private static void writeFixedNull(int targetType, long targetAddress, int rowIndex) {
        switch (ColumnType.tagOf(targetType)) {
            case ColumnType.BOOLEAN, ColumnType.BYTE -> Unsafe.putByte(targetAddress + rowIndex, (byte) 0);
            case ColumnType.SHORT -> Unsafe.putShort(targetAddress + ((long) rowIndex << 1), (short) 0);
            case ColumnType.CHAR -> Unsafe.putChar(targetAddress + ((long) rowIndex << 1), (char) 0);
            case ColumnType.INT -> Unsafe.putInt(targetAddress + ((long) rowIndex << 2), Numbers.INT_NULL);
            case ColumnType.IPv4 -> Unsafe.putInt(targetAddress + ((long) rowIndex << 2), Numbers.IPv4_NULL);
            case ColumnType.LONG, ColumnType.DATE, ColumnType.TIMESTAMP ->
                    Unsafe.putLong(targetAddress + ((long) rowIndex << 3), Numbers.LONG_NULL);
            case ColumnType.FLOAT -> Unsafe.putFloat(targetAddress + ((long) rowIndex << 2), Float.NaN);
            case ColumnType.DOUBLE -> Unsafe.putDouble(targetAddress + ((long) rowIndex << 3), Double.NaN);
            case ColumnType.UUID -> {
                final long address = targetAddress + ((long) rowIndex << 4);
                Unsafe.putLong(address, Numbers.LONG_NULL);
                Unsafe.putLong(address + Long.BYTES, Numbers.LONG_NULL);
            }
            case ColumnType.DECIMAL8 -> Unsafe.putByte(targetAddress + rowIndex, Decimals.DECIMAL8_NULL);
            case ColumnType.DECIMAL16 ->
                    Unsafe.putShort(targetAddress + ((long) rowIndex << 1), Decimals.DECIMAL16_NULL);
            case ColumnType.DECIMAL32 -> Unsafe.putInt(targetAddress + ((long) rowIndex << 2), Decimals.DECIMAL32_NULL);
            case ColumnType.DECIMAL64 ->
                    Unsafe.putLong(targetAddress + ((long) rowIndex << 3), Decimals.DECIMAL64_NULL);
            case ColumnType.DECIMAL128 -> {
                final long address = targetAddress + ((long) rowIndex << 4);
                Unsafe.putLong(address, Decimals.DECIMAL128_HI_NULL);
                Unsafe.putLong(address + Long.BYTES, Decimals.DECIMAL128_LO_NULL);
            }
            case ColumnType.DECIMAL256 -> {
                final long address = targetAddress + ((long) rowIndex << 5);
                Unsafe.putLong(address, Decimals.DECIMAL256_HH_NULL);
                Unsafe.putLong(address + Long.BYTES, Decimals.DECIMAL256_HL_NULL);
                Unsafe.putLong(address + 2L * Long.BYTES, Decimals.DECIMAL256_LH_NULL);
                Unsafe.putLong(address + 3L * Long.BYTES, Decimals.DECIMAL256_LL_NULL);
            }
        }
    }

    private static void writeFixedParsedValue(
            int targetType,
            long targetAddress,
            int rowIndex,
            CharSequence value,
            Decimal64 decimal64,
            Decimal128 decimal128,
            Decimal256 decimal256
    ) {
        try {
            switch (ColumnType.tagOf(targetType)) {
                case ColumnType.BOOLEAN ->
                        Unsafe.putByte(targetAddress + rowIndex, (byte) (SqlKeywords.isTrueKeyword(value) ? 1 : 0));
                case ColumnType.BYTE -> Unsafe.putByte(targetAddress + rowIndex, (byte) Numbers.parseInt(value));
                case ColumnType.SHORT ->
                        Unsafe.putShort(targetAddress + ((long) rowIndex << 1), (short) Numbers.parseInt(value));
                case ColumnType.CHAR ->
                        Unsafe.putChar(targetAddress + ((long) rowIndex << 1), !value.isEmpty() ? value.charAt(0) : 0);
                case ColumnType.INT -> Unsafe.putInt(targetAddress + ((long) rowIndex << 2), Numbers.parseInt(value));
                case ColumnType.LONG ->
                        Unsafe.putLong(targetAddress + ((long) rowIndex << 3), Numbers.parseLong(value));
                case ColumnType.FLOAT ->
                        Unsafe.putFloat(targetAddress + ((long) rowIndex << 2), Numbers.parseFloat(value));
                case ColumnType.DOUBLE ->
                        Unsafe.putDouble(targetAddress + ((long) rowIndex << 3), Numbers.parseDouble(value));
                case ColumnType.DATE -> Unsafe.putLong(
                        targetAddress + ((long) rowIndex << 3),
                        MillisTimestampDriver.INSTANCE.parseFloorLiteral(value)
                );
                case ColumnType.TIMESTAMP -> Unsafe.putLong(
                        targetAddress + ((long) rowIndex << 3),
                        ColumnType.getTimestampDriver(targetType).parseFloorLiteral(value)
                );
                case ColumnType.IPv4 ->
                        Unsafe.putInt(targetAddress + ((long) rowIndex << 2), Numbers.parseIPv4Quiet(value));
                case ColumnType.UUID -> {
                    Uuid.checkDashesAndLength(value);
                    final long address = targetAddress + ((long) rowIndex << 4);
                    Unsafe.putLong(address, Uuid.parseLo(value));
                    Unsafe.putLong(address + Long.BYTES, Uuid.parseHi(value));
                }
                case ColumnType.DECIMAL8 -> {
                    decimal64.ofString(value, ColumnType.getDecimalPrecision(targetType), ColumnType.getDecimalScale(targetType));
                    Unsafe.putByte(targetAddress + rowIndex, (byte) decimal64.getValue());
                }
                case ColumnType.DECIMAL16 -> {
                    decimal64.ofString(value, ColumnType.getDecimalPrecision(targetType), ColumnType.getDecimalScale(targetType));
                    Unsafe.putShort(targetAddress + ((long) rowIndex << 1), (short) decimal64.getValue());
                }
                case ColumnType.DECIMAL32 -> {
                    decimal64.ofString(value, ColumnType.getDecimalPrecision(targetType), ColumnType.getDecimalScale(targetType));
                    Unsafe.putInt(targetAddress + ((long) rowIndex << 2), (int) decimal64.getValue());
                }
                case ColumnType.DECIMAL64 -> {
                    decimal64.ofString(value, ColumnType.getDecimalPrecision(targetType), ColumnType.getDecimalScale(targetType));
                    Unsafe.putLong(targetAddress + ((long) rowIndex << 3), decimal64.getValue());
                }
                case ColumnType.DECIMAL128 -> {
                    decimal128.ofString(value, ColumnType.getDecimalPrecision(targetType), ColumnType.getDecimalScale(targetType));
                    final long address = targetAddress + ((long) rowIndex << 4);
                    Unsafe.putLong(address, decimal128.getHigh());
                    Unsafe.putLong(address + Long.BYTES, decimal128.getLow());
                }
                case ColumnType.DECIMAL256 -> {
                    decimal256.ofString(value, ColumnType.getDecimalPrecision(targetType), ColumnType.getDecimalScale(targetType));
                    final long address = targetAddress + ((long) rowIndex << 5);
                    Unsafe.putLong(address, decimal256.getHh());
                    Unsafe.putLong(address + Long.BYTES, decimal256.getHl());
                    Unsafe.putLong(address + 2L * Long.BYTES, decimal256.getLh());
                    Unsafe.putLong(address + 3L * Long.BYTES, decimal256.getLl());
                }
                default -> writeFixedNull(targetType, targetAddress, rowIndex);
            }
        } catch (NumericException e) {
            writeFixedNull(targetType, targetAddress, rowIndex);
        }
    }

    /**
     * Chooses the representation Rust should decode before Java applies any
     * fixed/variable-width crossing conversion.
     */
    static int chooseDecodeType(ParquetPartitionDecoder decoder, int parquetIndex, int targetType) {
        return chooseDecodeType(decoder.metadata().getColumnType(parquetIndex), targetType);
    }

    static int chooseDecodeType(int sourceType, int targetType) {
        final int sourceTag = ColumnType.tagOf(sourceType);
        final int targetTag = ColumnType.tagOf(targetType);
        if (ColumnType.isVarSize(sourceTag) && !ColumnType.isVarSize(targetTag) && !ColumnType.isSymbol(targetTag)) {
            // VARCHAR_SLICE carries absolute data pointers consumed by the Java converter.
            return sourceTag == ColumnType.VARCHAR ? ColumnType.VARCHAR_SLICE : sourceType;
        }
        if (ColumnType.isSymbol(sourceTag) && !ColumnType.isSymbol(targetTag)) {
            if (ColumnType.isVarSize(targetTag)) {
                // Native var-size layout is required when no Java conversion follows.
                return targetTag == ColumnType.STRING ? ColumnType.STRING : ColumnType.VARCHAR;
            }
            return ColumnType.VARCHAR_SLICE;
        }
        if (!ColumnType.isVarSize(sourceTag)
                && !ColumnType.isSymbol(sourceTag)
                && ColumnType.isVarSize(targetTag)) {
            // Rust cannot directly produce var-size output from fixed input.
            return sourceType;
        }
        return targetType;
    }

    static void convertFixedColumnToString(
            int sourceType,
            long sourceDataAddress,
            int rowCount,
            int columnTop,
            long auxAddress,
            long dataAddress,
            long dataSize,
            StringSink sink
    ) {
        long dataOffset = 0;
        Unsafe.putLong(auxAddress, 0L);

        final ColumnTypeConverter.Fixed2VarConverter converter =
                ColumnTypeConverter.getFixedToVarConverter(sourceType, ColumnType.STRING);
        final long elementSize = ColumnType.sizeOf(sourceType);
        final int argument1 = ColumnType.isDecimal(sourceType) ? ColumnType.getDecimalPrecision(sourceType) : 0;
        final int argument2 = ColumnType.isDecimal(sourceType) ? ColumnType.getDecimalScale(sourceType) : 0;
        final int leadingNulls = ColumnType.isNoNullSentinelFixedType(sourceType) ? columnTop : 0;

        for (int i = 0; i < rowCount; i++) {
            sink.clear();
            final boolean hasValue = i >= leadingNulls
                    && converter.convert(sourceDataAddress + i * elementSize, sink, argument1, argument2);
            if (!hasValue) {
                Unsafe.putInt(dataAddress + dataOffset, -1);
                dataOffset += Integer.BYTES;
            } else {
                final int charCount = sink.length();
                Unsafe.putInt(dataAddress + dataOffset, charCount);
                dataOffset += Integer.BYTES;
                for (int j = 0; j < charCount; j++) {
                    Unsafe.putChar(dataAddress + dataOffset + (long) j * Character.BYTES, sink.charAt(j));
                }
                dataOffset += (long) charCount * Character.BYTES;
            }
            Unsafe.putLong(auxAddress + (long) (i + 1) * Long.BYTES, dataOffset);
        }
        assert dataOffset <= dataSize
                : "STRING conversion overflow: dataOffset=" + dataOffset
                + " dataSize=" + dataSize
                + " sourceType=" + ColumnType.nameOf(sourceType);
    }

    static void convertFixedColumnToVarchar(
            int sourceType,
            long sourceDataAddress,
            int rowCount,
            int columnTop,
            long auxAddress,
            long dataAddress,
            long dataSize,
            Utf8StringSink sink
    ) {
        long dataOffset = 0;
        final ColumnTypeConverter.Fixed2VarConverter converter =
                ColumnTypeConverter.getFixedToVarConverter(sourceType, ColumnType.VARCHAR);
        final long elementSize = ColumnType.sizeOf(sourceType);
        final int argument1 = ColumnType.isDecimal(sourceType) ? ColumnType.getDecimalPrecision(sourceType) : 0;
        final int argument2 = ColumnType.isDecimal(sourceType) ? ColumnType.getDecimalScale(sourceType) : 0;
        final int leadingNulls = ColumnType.isNoNullSentinelFixedType(sourceType) ? columnTop : 0;

        for (int i = 0; i < rowCount; i++) {
            sink.clear();
            final boolean hasValue = i >= leadingNulls
                    && converter.convert(sourceDataAddress + i * elementSize, sink, argument1, argument2);
            final long auxEntryAddress = auxAddress + (long) i * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;

            if (!hasValue) {
                Unsafe.putInt(auxEntryAddress, VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL);
                Unsafe.putInt(auxEntryAddress + 4, 0);
                Unsafe.putShort(auxEntryAddress + 8, (short) 0);
                Unsafe.putShort(auxEntryAddress + 10, (short) dataOffset);
                Unsafe.putInt(auxEntryAddress + 12, (int) (dataOffset >> 16));
            } else {
                final int size = sink.size();
                final boolean isAscii = sink.isAscii();
                if (size <= VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED) {
                    int flags = 1;
                    if (isAscii) {
                        flags |= 2;
                    }
                    Unsafe.putByte(auxEntryAddress, (byte) ((size << 4) | flags));
                    for (int j = 0; j < size; j++) {
                        Unsafe.putByte(auxEntryAddress + 1 + j, sink.byteAt(j));
                    }
                    for (int j = size; j < VarcharTypeDriver.VARCHAR_MAX_BYTES_FULLY_INLINED; j++) {
                        Unsafe.putByte(auxEntryAddress + 1 + j, (byte) 0);
                    }
                    Unsafe.putShort(auxEntryAddress + 10, (short) dataOffset);
                    Unsafe.putInt(auxEntryAddress + 12, (int) (dataOffset >> 16));
                } else {
                    final int flags = isAscii ? 2 : 0;
                    Unsafe.putInt(auxEntryAddress, (size << 4) | flags);
                    for (int j = 0; j < VarcharTypeDriver.VARCHAR_INLINED_PREFIX_BYTES; j++) {
                        Unsafe.putByte(auxEntryAddress + 4 + j, sink.byteAt(j));
                    }
                    Unsafe.putShort(auxEntryAddress + 10, (short) dataOffset);
                    Unsafe.putInt(auxEntryAddress + 12, (int) (dataOffset >> 16));
                    for (int j = 0; j < size; j++) {
                        Unsafe.putByte(dataAddress + dataOffset + j, sink.byteAt(j));
                    }
                    dataOffset += size;
                }
            }
        }
        assert dataOffset <= dataSize
                : "VARCHAR conversion overflow: dataOffset=" + dataOffset
                + " dataSize=" + dataSize
                + " sourceType=" + ColumnType.nameOf(sourceType);
    }

    static void convertVarColumnToFixed(
            int sourceType,
            int targetType,
            long dataAddress,
            long auxAddress,
            int rowCount,
            long targetAddress,
            Utf8StringSink utf8Sink,
            StringSink utf16Sink,
            Decimal64 decimal64,
            Decimal128 decimal128,
            Decimal256 decimal256
    ) {
        final boolean isVarchar = ColumnType.isVarchar(sourceType);
        for (int i = 0; i < rowCount; i++) {
            final CharSequence value;
            if (isVarchar) {
                final long auxEntryAddress = auxAddress + (long) i * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES;
                final int header = Unsafe.getInt(auxEntryAddress);
                if ((header & VarcharTypeDriver.VARCHAR_HEADER_FLAG_NULL) != 0) {
                    writeFixedNull(targetType, targetAddress, i);
                    continue;
                }
                final int size = header >>> 4;
                final long valueAddress = Unsafe.getLong(auxEntryAddress + 8);
                utf8Sink.clear();
                for (int j = 0; j < size; j++) {
                    utf8Sink.putAny(Unsafe.getByte(valueAddress + j));
                }
                value = Utf8s.utf8ToUtf16OrView(utf8Sink, utf16Sink);
            } else {
                final long offset = Unsafe.getLong(auxAddress + (long) i * Long.BYTES);
                final int length = Unsafe.getInt(dataAddress + offset);
                if (length < 0) {
                    writeFixedNull(targetType, targetAddress, i);
                    continue;
                }
                utf16Sink.clear();
                final long charAddress = dataAddress + offset + Integer.BYTES;
                for (int j = 0; j < length; j++) {
                    utf16Sink.put(Unsafe.getChar(charAddress + (long) j * Character.BYTES));
                }
                value = utf16Sink;
            }
            writeFixedParsedValue(targetType, targetAddress, i, value, decimal64, decimal128, decimal256);
        }
    }

    static long estimateStringDataSize(int sourceType, int rowCount) {
        final int maxCharsPerRow = ColumnType.isDecimal(sourceType)
                ? ColumnType.getDecimalPrecision(sourceType) + 3
                : switch (sourceType) {
            case ColumnType.BOOLEAN -> 5;
            case ColumnType.BYTE -> 4;
            case ColumnType.SHORT -> 6;
            case ColumnType.CHAR -> 1;
            case ColumnType.INT -> 11;
            case ColumnType.LONG -> 20;
            case ColumnType.FLOAT -> 15;
            case ColumnType.DOUBLE -> 25;
            case ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> 30;
            case ColumnType.IPv4 -> 15;
            case ColumnType.UUID -> 36;
            default -> 40;
        };
        return (long) (Integer.BYTES + maxCharsPerRow * Character.BYTES) * rowCount;
    }

    static long estimateVarcharDataSize(int sourceType, int rowCount) {
        final int maxBytesPerRow = ColumnType.isDecimal(sourceType)
                ? ColumnType.getDecimalPrecision(sourceType) + 3
                : switch (sourceType) {
            case ColumnType.BOOLEAN, ColumnType.BYTE, ColumnType.SHORT, ColumnType.CHAR -> 0;
            case ColumnType.INT -> 11;
            case ColumnType.LONG -> 20;
            case ColumnType.FLOAT -> 15;
            case ColumnType.DOUBLE -> 25;
            case ColumnType.DATE, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP_NANO -> 30;
            case ColumnType.IPv4 -> 15;
            case ColumnType.UUID -> 36;
            default -> 40;
        };
        return (long) maxBytesPerRow * rowCount;
    }

    /**
     * Materializes one decoded parquet column as a target-typed source. Any
     * native allocation is recorded in {@code ownedBuffers} for deterministic
     * release by the row-group materializer.
     */
    static void prepareSourceColumn(
            ParquetPartitionDecoder decoder,
            RowGroupBuffers rowGroupBuffers,
            IntList tableToParquetIndex,
            int columnIndex,
            int columnType,
            int decodeIndex,
            int rowGroupSize,
            ParquetConversionContext context,
            LongList ownedBuffers,
            LongList targetPointers,
            int slot
    ) {
        final int memoryTag = context.getMemoryTag();
        if (ColumnType.isVarSize(columnType)) {
            final ColumnTypeDriver driver = ColumnType.getDriver(columnType);
            long columnDataAddress = decodeIndex >= 0 ? rowGroupBuffers.getChunkDataPtr(decodeIndex) : 0;
            long columnDataSize = decodeIndex >= 0 ? rowGroupBuffers.getChunkDataSize(decodeIndex) : 0;
            long columnAuxAddress = decodeIndex >= 0 ? rowGroupBuffers.getChunkAuxPtr(decodeIndex) : 0;
            long columnAuxSize = decodeIndex >= 0 ? rowGroupBuffers.getChunkAuxSize(decodeIndex) : 0;

            if (columnAuxSize == 0) {
                if (decodeIndex >= 0 && columnDataAddress != 0) {
                    final int parquetIndex = tableToParquetIndex.getQuick(columnIndex);
                    final int sourceType = decoder.metadata().getColumnType(parquetIndex);
                    final long auxSize = driver.getAuxVectorSize(rowGroupSize);
                    final long auxBuffer = Unsafe.malloc(auxSize, memoryTag);
                    ownedBuffers.setQuick(slot, auxBuffer);
                    ownedBuffers.setQuick(slot + 1, auxSize);

                    final int conversionColumnTop = (int) rowGroupBuffers.getChunkColumnTop(decodeIndex);
                    if (ColumnType.isVarchar(columnType)) {
                        final long dataSize = estimateVarcharDataSize(sourceType, rowGroupSize);
                        final long dataBuffer = dataSize > 0 ? Unsafe.malloc(dataSize, memoryTag) : 0;
                        if (dataBuffer != 0) {
                            ownedBuffers.setQuick(slot + 2, dataBuffer);
                            ownedBuffers.setQuick(slot + 3, dataSize);
                        }
                        convertFixedColumnToVarchar(
                                sourceType,
                                columnDataAddress,
                                rowGroupSize,
                                conversionColumnTop,
                                auxBuffer,
                                dataBuffer,
                                dataSize,
                                context.getUtf8Sink()
                        );
                        columnAuxAddress = auxBuffer;
                        columnAuxSize = auxSize;
                        columnDataAddress = dataBuffer;
                        columnDataSize = dataSize;
                    } else {
                        final long dataSize = estimateStringDataSize(sourceType, rowGroupSize);
                        final long dataBuffer = Unsafe.malloc(dataSize, memoryTag);
                        ownedBuffers.setQuick(slot + 2, dataBuffer);
                        ownedBuffers.setQuick(slot + 3, dataSize);
                        convertFixedColumnToString(
                                sourceType,
                                columnDataAddress,
                                rowGroupSize,
                                conversionColumnTop,
                                auxBuffer,
                                dataBuffer,
                                dataSize,
                                context.getUtf16Sink()
                        );
                        columnAuxAddress = auxBuffer;
                        columnAuxSize = auxSize;
                        columnDataAddress = dataBuffer;
                        columnDataSize = dataSize;
                    }
                } else {
                    final long nullAuxSize = driver.getAuxVectorSize(rowGroupSize);
                    final long nullAuxBuffer = Unsafe.malloc(nullAuxSize, memoryTag);
                    driver.setFullAuxVectorNull(nullAuxBuffer, rowGroupSize);
                    columnAuxAddress = nullAuxBuffer;
                    columnAuxSize = nullAuxSize;
                    ownedBuffers.setQuick(slot, nullAuxBuffer);
                    ownedBuffers.setQuick(slot + 1, nullAuxSize);

                    final long nullDataSize = driver.getDataVectorSizeAt(nullAuxBuffer, rowGroupSize - 1);
                    if (nullDataSize > 0) {
                        final long nullDataBuffer = Unsafe.malloc(nullDataSize, memoryTag);
                        driver.setDataVectorEntriesToNull(nullDataBuffer, rowGroupSize);
                        columnDataAddress = nullDataBuffer;
                        columnDataSize = nullDataSize;
                        ownedBuffers.setQuick(slot + 2, nullDataBuffer);
                        ownedBuffers.setQuick(slot + 3, nullDataSize);
                    }
                }
            }
            targetPointers.setQuick(slot, columnDataAddress);
            targetPointers.setQuick(slot + 1, columnDataSize);
            targetPointers.setQuick(slot + 2, columnAuxAddress);
            targetPointers.setQuick(slot + 3, columnAuxSize);
        } else {
            long columnDataAddress = decodeIndex >= 0 ? rowGroupBuffers.getChunkDataPtr(decodeIndex) : 0;
            final long columnAuxSize = decodeIndex >= 0 ? rowGroupBuffers.getChunkAuxSize(decodeIndex) : 0;

            if (columnAuxSize > 0 && columnDataAddress != 0) {
                final long columnAuxAddress = rowGroupBuffers.getChunkAuxPtr(decodeIndex);
                final int parquetIndex = tableToParquetIndex.getQuick(columnIndex);
                int sourceType = decoder.metadata().getColumnType(parquetIndex);
                if (ColumnType.isSymbol(ColumnType.tagOf(sourceType))) {
                    sourceType = ColumnType.VARCHAR;
                }

                final long fixedSize = (long) rowGroupSize * ColumnType.sizeOf(columnType);
                final long fixedBuffer = Unsafe.malloc(fixedSize, memoryTag);
                ownedBuffers.setQuick(slot, fixedBuffer);
                ownedBuffers.setQuick(slot + 1, fixedSize);
                convertVarColumnToFixed(
                        sourceType,
                        columnType,
                        columnDataAddress,
                        columnAuxAddress,
                        rowGroupSize,
                        fixedBuffer,
                        context.getUtf8Sink(),
                        context.getUtf16Sink(),
                        context.getDecimal64Buf(),
                        context.getDecimal128Buf(),
                        context.getDecimal256Buf()
                );
                columnDataAddress = fixedBuffer;
            } else if (columnDataAddress == 0) {
                final long nullFixedSize = (long) rowGroupSize * ColumnType.sizeOf(columnType);
                final long nullFixedBuffer = Unsafe.malloc(nullFixedSize, memoryTag);
                TableUtils.setNull(columnType, nullFixedBuffer, rowGroupSize);
                columnDataAddress = nullFixedBuffer;
                ownedBuffers.setQuick(slot, nullFixedBuffer);
                ownedBuffers.setQuick(slot + 1, nullFixedSize);
            }
            targetPointers.setQuick(slot, columnDataAddress);
            targetPointers.setQuick(slot + 1, (long) rowGroupSize * ColumnType.sizeOf(columnType));
            targetPointers.setQuick(slot + 2, 0);
            targetPointers.setQuick(slot + 3, 0);
        }
    }
}
