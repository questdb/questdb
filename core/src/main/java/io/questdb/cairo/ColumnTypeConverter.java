/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCMORImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.griffin.ColumnConversionOffsetSink;
import io.questdb.griffin.ConvertersNative;
import io.questdb.griffin.SqlKeywords;
import io.questdb.griffin.SymbolMapWriterLite;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import io.questdb.std.Uuid;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

public class ColumnTypeConverter {
    private static final Log LOG = LogFactory.getLog(ColumnTypeConverter.class);
    private static final Fixed2VarConverter converterFromBoolean2String = ColumnTypeConverter::stringFromBoolean;
    private static final Fixed2VarConverter converterFromByte2String = ColumnTypeConverter::stringFromByte;
    private static final Fixed2VarConverter converterFromChar2String = ColumnTypeConverter::stringFromChar;
    private static final Fixed2VarConverter converterFromDate2String = ColumnTypeConverter::stringFromDate;
    private static final Fixed2VarConverter converterFromDouble2String = ColumnTypeConverter::stringFromDouble;
    private static final Fixed2VarConverter converterFromFloat2String = ColumnTypeConverter::stringFromFloat;
    private static final Fixed2VarConverter converterFromIPv42String = ColumnTypeConverter::stringFromIPv4;
    private static final Fixed2VarConverter converterFromInt2String = ColumnTypeConverter::stringFromInt;
    private static final Fixed2VarConverter converterFromLong2String = ColumnTypeConverter::stringFromLong;
    private static final Fixed2VarConverter converterFromShort2String = ColumnTypeConverter::stringFromShort;
    private static final Fixed2VarConverter converterFromUuid2String = ColumnTypeConverter::stringFromUuid;
    private static final Var2FixedConverter<CharSequence> converterStr2Boolean = ColumnTypeConverter::str2Boolean;
    private static final Var2FixedConverter<CharSequence> converterStr2Byte = ColumnTypeConverter::str2Byte;
    private static final Var2FixedConverter<CharSequence> converterStr2Char = ColumnTypeConverter::str2Char;
    private static final Var2FixedConverter<CharSequence> converterStr2Date = ColumnTypeConverter::str2Date;
    private static final Var2FixedConverter<CharSequence> converterStr2Double = ColumnTypeConverter::str2Double;
    private static final Var2FixedConverter<CharSequence> converterStr2Float = ColumnTypeConverter::str2Float;
    private static final Var2FixedConverter<CharSequence> converterStr2IPv4 = ColumnTypeConverter::str2IpV4;
    private static final Var2FixedConverter<CharSequence> converterStr2Int = ColumnTypeConverter::str2Int;
    private static final Var2FixedConverter<CharSequence> converterStr2Long = ColumnTypeConverter::str2Long;
    private static final Var2FixedConverter<CharSequence> converterStr2Short = ColumnTypeConverter::str2Short;
    private static final Var2FixedConverter<CharSequence> converterStr2Uuid = ColumnTypeConverter::str2Uuid;
    private static final ThreadLocal<MemoryCMARW> dstFixMemTL = new ThreadLocal<>(io.questdb.cairo.vm.MemoryCMARWImpl::new);
    private static final ThreadLocal<MemoryCMARW> dstVarMemTL = new ThreadLocal<>(io.questdb.cairo.vm.MemoryCMARWImpl::new);
    private static final int memoryTag = MemoryTag.MMAP_TABLE_WRITER;
    private static final ThreadLocal<StringSink> sinkUtf16TL = new ThreadLocal<>(StringSink::new);
    private static final ThreadLocal<Utf8StringSink> sinkUtf8TL = new ThreadLocal<>(Utf8StringSink::new);
    private static final ThreadLocal<MemoryCMORImpl> srcFixMemTL = new ThreadLocal<>(MemoryCMORImpl::new);
    private static final ThreadLocal<MemoryCMORImpl> srcVarMemTL = new ThreadLocal<>(MemoryCMORImpl::new);

    public static boolean convertColumn(
            long skipRows,
            long rowCount,
            int srcColumnType,
            long srcFixFd,
            long srcVarFd,
            @Nullable SymbolTable symbolTable,
            int dstColumnType,
            long dstFixFd,
            long dstVarFd,
            @Nullable SymbolMapWriterLite symbolMapWriter,
            FilesFacade ff,
            long appendPageSize,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        assert skipRows > -1 && rowCount > -1;
        if (ColumnType.isSymbol(srcColumnType)) {
            assert symbolTable != null;
            convertFromSymbol(skipRows, rowCount, srcFixFd, symbolTable, dstColumnType, dstFixFd, dstVarFd, ff, appendPageSize, columnSizesSink);
            return true;
        } else if (ColumnType.isFixedSize(srcColumnType) && ColumnType.isFixedSize(dstColumnType)) {
            return convertFixedToFixed(rowCount, skipRows, srcFixFd, dstFixFd, srcColumnType, dstColumnType, ff, columnSizesSink);
        } else if (ColumnType.isVarSize(srcColumnType)) {
            switch (srcColumnType) {
                case ColumnType.STRING:
                    return convertFromString(skipRows, rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, dstColumnType, ff, appendPageSize, symbolMapWriter, columnSizesSink);
                case ColumnType.VARCHAR:
                    return convertFromVarchar(skipRows, rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, dstColumnType, ff, appendPageSize, symbolMapWriter, columnSizesSink);
                default:
                    throw unsupportedConversion(srcColumnType, dstColumnType);
            }
        } else if (ColumnType.isFixedSize(srcColumnType) && ColumnType.isVarSize(dstColumnType)) {
            switch (dstColumnType) {
                case ColumnType.STRING:
                    return convertFixedToString(skipRows, rowCount, srcFixFd, srcColumnType, dstFixFd, dstVarFd, ff, appendPageSize, columnSizesSink);
                case ColumnType.VARCHAR:
                    return convertFixedToVarchar(skipRows, rowCount, srcFixFd, srcColumnType, dstFixFd, dstVarFd, ff, appendPageSize, columnSizesSink);
                default:
                    throw unsupportedConversion(srcColumnType, dstColumnType);
            }
        } else if (ColumnType.isFixedSize(srcColumnType) && dstColumnType == ColumnType.SYMBOL) {
            assert symbolMapWriter != null;
            return convertFixedToSymbol(skipRows, rowCount, srcFixFd, srcColumnType, dstFixFd, symbolMapWriter, ff, appendPageSize, columnSizesSink);
        } else {
            throw unsupportedConversion(srcColumnType, dstColumnType);
        }
    }

    public static Var2FixedConverter<CharSequence> getConverterFromVarToFixed(short srcType, int dstColumnType) {
        switch (ColumnType.tagOf(dstColumnType)) {
            case ColumnType.IPv4:
                return converterStr2IPv4;
            case ColumnType.UUID:
                return converterStr2Uuid;
            case ColumnType.INT:
                return converterStr2Int;
            case ColumnType.SHORT:
                return converterStr2Short;
            case ColumnType.BYTE:
                return converterStr2Byte;
            case ColumnType.CHAR:
                return converterStr2Char;
            case ColumnType.LONG:
                return converterStr2Long;
            case ColumnType.DOUBLE:
                return converterStr2Double;
            case ColumnType.FLOAT:
                return converterStr2Float;
            case ColumnType.DATE:
                return converterStr2Date;
            case ColumnType.TIMESTAMP:
                return ColumnType.getTimestampDriver(dstColumnType).getConverterStr2Timestamp();
            case ColumnType.BOOLEAN:
                return converterStr2Boolean;
            default:
                throw unsupportedConversion(srcType, dstColumnType);
        }
    }

    private static boolean convertFixedToFixed(
            long rowCount,
            long skipRows,
            long srcFixFd,
            long dstFixFd,
            int srcColumnType,
            int dstColumnType,
            FilesFacade ff,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        final long srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        final long dstColumnTypeSize = ColumnType.sizeOf(dstColumnType);
        long srcMapAddress = 0;
        long dstMapAddress = 0;

        long skipBytes = skipRows * srcColumnTypeSize;
        long mapBytes = rowCount * srcColumnTypeSize;
        long dstMapBytes = rowCount * dstColumnTypeSize;

        try {
            srcMapAddress = TableUtils.mapAppendColumnBuffer(ff, srcFixFd, skipBytes, mapBytes, false, memoryTag);
            columnSizesSink.setSrcOffsets(skipBytes, -1);

            if (!ff.truncate(dstFixFd, dstMapBytes)) {
                throw CairoException.critical(ff.errno()).put("Cannot allocate fd: ").put(dstFixFd).put(", size: ").put(dstMapBytes);
            }
            dstMapAddress = TableUtils.mapAppendColumnBuffer(ff, dstFixFd, 0, dstMapBytes, true, memoryTag);
            columnSizesSink.setDestSizes(dstMapBytes, -1);

            long succeeded = ConvertersNative.fixedToFixed(srcMapAddress, srcColumnType, dstMapAddress, dstColumnType, rowCount);
            switch ((int) succeeded) {
                case ConvertersNative.ConversionError.NONE:
                    return true;
                case ConvertersNative.ConversionError.UNSUPPORTED_CAST:
                    throw unsupportedConversion(srcColumnType, dstColumnType);
                default:
                    throw CairoException.critical(0).put("Unknown return code from native call: ").put(succeeded);
            }
        } finally {
            if (srcMapAddress != 0) {
                TableUtils.mapAppendColumnBufferRelease(ff, srcMapAddress, skipBytes, mapBytes, memoryTag);
            }
            if (dstMapAddress != 0) {
                TableUtils.mapAppendColumnBufferRelease(ff, dstMapAddress, 0, dstMapBytes, memoryTag);
            }
        }
    }

    private static boolean convertFixedToString(
            long skipRows,
            long rowCount,
            long srcFixFd,
            int srcColumnType,
            long dstFixFd,
            long dstVarFd,
            FilesFacade ff,
            long appendPageSize,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        final long srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        assert srcColumnTypeSize > 0;
        long srcMapAddress;

        long skipBytes = skipRows * srcColumnTypeSize;
        long mapBytes = rowCount * srcColumnTypeSize;
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();

        srcMapAddress = TableUtils.mapAppendColumnBuffer(ff, srcFixFd, skipBytes, mapBytes, false, memoryTag);
        try {
            dstVarMem.of(ff, dstVarFd, true, null, appendPageSize, appendPageSize, memoryTag);
            dstVarMem.jumpTo(0);
            dstFixMem.of(ff, dstFixFd, true, null, appendPageSize, StringTypeDriver.INSTANCE.getAuxVectorSize(rowCount), memoryTag);
            dstFixMem.jumpTo(0);
            dstFixMem.putLong(0L);
            StringSink sink = sinkUtf16TL.get();
            columnSizesSink.setSrcOffsets(skipBytes, -1);

            Fixed2VarConverter converter = getFixedToVarConverter(srcColumnType, ColumnType.STRING);
            convertFixedToString0(rowCount, srcMapAddress, dstFixMem, dstVarMem, sink, srcColumnType, converter);
            columnSizesSink.setDestSizes(dstVarMem.getAppendOffset(), dstFixMem.getAppendOffset());
        } finally {
            TableUtils.mapAppendColumnBufferRelease(ff, srcMapAddress, skipBytes, mapBytes, memoryTag);
            dstFixMem.detachFdClose();
            dstVarMem.detachFdClose();
        }
        return true;
    }

    private static void convertFixedToString0(
            long rowCount,
            long srcMapAddress,
            MemoryCMARW dstFixMem,
            MemoryCMARW dstVarMem,
            StringSink sink,
            int srcColumnType,
            Fixed2VarConverter converterInt2String
    ) {
        int srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        long hi = srcMapAddress + srcColumnTypeSize * rowCount;
        sink.clear();
        for (long addr = srcMapAddress; addr < hi; addr += srcColumnTypeSize) {
            if (converterInt2String.convert(addr, sink)) {
                StringTypeDriver.appendValue(dstFixMem, dstVarMem, sink);
                sink.clear();
            } else {
                StringTypeDriver.INSTANCE.appendNull(dstFixMem, dstVarMem);
            }
        }
    }

    private static boolean convertFixedToSymbol(
            long skipRows,
            long rowCount,
            long srcFixFd,
            int srcColumnType,
            long dstFixFd,
            SymbolMapWriterLite symbolMapWriter,
            FilesFacade ff,
            long appendPageSize,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        final long srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        assert srcColumnTypeSize > 0;
        long srcMapAddress;

        long skipBytes = skipRows * srcColumnTypeSize;
        long mapBytes = rowCount * srcColumnTypeSize;
        MemoryCMARW dstFixMem = dstFixMemTL.get();

        srcMapAddress = TableUtils.mapAppendColumnBuffer(ff, srcFixFd, skipBytes, mapBytes, false, memoryTag);
        try {
            dstFixMem.of(ff, dstFixFd, true, null, appendPageSize, StringTypeDriver.INSTANCE.getAuxVectorSize(rowCount), memoryTag);
            dstFixMem.jumpTo(0);
            StringSink sink = sinkUtf16TL.get();
            columnSizesSink.setSrcOffsets(skipBytes, -1);

            Fixed2VarConverter converter = getFixedToVarConverter(srcColumnType, ColumnType.SYMBOL);
            convertFixedToSymbol0(rowCount, srcMapAddress, dstFixMem, symbolMapWriter, sink, srcColumnType, converter);
            columnSizesSink.setDestSizes(dstFixMem.getAppendOffset(), -1);
        } finally {
            TableUtils.mapAppendColumnBufferRelease(ff, srcMapAddress, skipBytes, mapBytes, memoryTag);
            dstFixMem.detachFdClose();
        }
        return true;
    }

    private static void convertFixedToSymbol0(
            long rowCount,
            long srcMapAddress,
            MemoryCMARW dstFixMem,
            SymbolMapWriterLite symbolMapWriter,
            StringSink sink,
            int srcColumnType,
            Fixed2VarConverter converterInt2String
    ) {
        int srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        long hi = srcMapAddress + srcColumnTypeSize * rowCount;
        sink.clear();
        for (long addr = srcMapAddress; addr < hi; addr += srcColumnTypeSize) {
            if (converterInt2String.convert(addr, sink)) {
                int value = symbolMapWriter.resolveSymbol(sink);
                dstFixMem.putInt(value);
                sink.clear();
            } else {
                int value = symbolMapWriter.resolveSymbol(null);
                dstFixMem.putInt(value);
            }
        }
    }

    private static boolean convertFixedToVarchar(
            long skipRows,
            long rowCount,
            long srcFixFd,
            int srcColumnType,
            long dstFixFd,
            long dstVarFd,
            FilesFacade ff,
            long appendPageSize,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        final long srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        assert srcColumnTypeSize > 0;
        long srcMapAddress;

        long skipBytes = skipRows * srcColumnTypeSize;
        long mapBytes = rowCount * srcColumnTypeSize;
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();

        srcMapAddress = TableUtils.mapAppendColumnBuffer(ff, srcFixFd, skipBytes, mapBytes, false, memoryTag);
        try {
            dstVarMem.of(ff, dstVarFd, true, null, appendPageSize, appendPageSize, memoryTag);
            dstVarMem.jumpTo(0);
            dstFixMem.of(ff, dstFixFd, true, null, appendPageSize, StringTypeDriver.INSTANCE.getAuxVectorSize(rowCount), memoryTag);
            dstFixMem.jumpTo(0);
            Utf8StringSink sink = sinkUtf8TL.get();
            columnSizesSink.setSrcOffsets(skipBytes, -1);

            Fixed2VarConverter converter = getFixedToVarConverter(srcColumnType, ColumnType.VARCHAR);
            convertFixedToVarchar0(rowCount, srcMapAddress, dstFixMem, dstVarMem, sink, srcColumnType, converter);
            columnSizesSink.setDestSizes(dstVarMem.getAppendOffset(), dstFixMem.getAppendOffset());
        } finally {
            TableUtils.mapAppendColumnBufferRelease(ff, srcMapAddress, skipBytes, mapBytes, memoryTag);
            dstFixMem.detachFdClose();
            dstVarMem.detachFdClose();
        }
        return true;
    }

    private static void convertFixedToVarchar0(
            long rowCount,
            long srcMapAddress,
            MemoryCMARW dstFixMem,
            MemoryCMARW dstVarMem,
            Utf8StringSink sink,
            int srcColumnType,
            Fixed2VarConverter converterInt2String
    ) {
        int srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        long hi = srcMapAddress + srcColumnTypeSize * rowCount;
        sink.clear();
        for (long addr = srcMapAddress; addr < hi; addr += srcColumnTypeSize) {
            if (converterInt2String.convert(addr, sink)) {
                VarcharTypeDriver.appendValue(dstFixMem, dstVarMem, sink);
                sink.clear();
            } else {
                VarcharTypeDriver.INSTANCE.appendNull(dstFixMem, dstVarMem);
            }
        }
    }

    private static boolean convertFromString(
            long skipRows,
            long rowCount,
            long srcFixFd,
            long srcVarFd,
            long dstFixFd,
            long dstVarFd,
            int dstColumnType,
            FilesFacade ff,
            long appendPageSize,
            SymbolMapWriterLite symbolMapWriter,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        long skipDataSize;
        long dataSize;
        StringTypeDriver typeDriver = StringTypeDriver.INSTANCE;
        try {
            skipDataSize = skipRows > 0 ? typeDriver.getDataVectorSizeAtFromFd(ff, srcFixFd, skipRows - 1) : 0;
            dataSize = typeDriver.getDataVectorSizeAtFromFd(ff, srcFixFd, skipRows + rowCount - 1);
            if (dataSize < typeDriver.getDataVectorMinEntrySize()) {
                throw CairoException.nonCritical().put("String column data vector size is less than minimum entry size [size=").put(dataSize)
                        .put(", srcFixFd=").put(srcFixFd).put(']');
            }
        } catch (CairoException ex) {
            LOG.error().$("cannot read STRING column data vector size, column data is corrupt will fall back reading file sizes [srcFixFd=").$(srcFixFd)
                    .$(", msg=").$safe(ex.getFlyweightMessage())
                    .$(", errno=").$(ex.getErrno())
                    .I$();
            return false;
        }

        columnSizesSink.setSrcOffsets(skipDataSize, typeDriver.getAuxVectorSize(skipRows));
        MemoryCMORImpl srcVarMem = srcVarMemTL.get();

        try {
            srcVarMem.ofOffset(ff, srcVarFd, true, null, skipDataSize, dataSize, memoryTag, CairoConfiguration.O_NONE);
            switch (dstColumnType) {
                case ColumnType.VARCHAR:
                    convertStringToVarchar(skipDataSize, rowCount, dstFixFd, dstVarFd, ff, appendPageSize, srcVarMem, columnSizesSink);
                    return true;
                case ColumnType.SYMBOL:
                    convertStringToSymbol(skipDataSize, rowCount, dstFixFd, ff, symbolMapWriter, srcVarMem, columnSizesSink);
                    return true;
            }
            Var2FixedConverter<CharSequence> converter = getConverterFromVarToFixed(ColumnType.STRING, dstColumnType);
            convertStringToFixed(skipDataSize, rowCount, dstFixFd, ff, srcVarMem, columnSizesSink, dstColumnType, converter);
        } finally {
            srcVarMem.detachFdClose();
        }
        return true;
    }

    private static void convertFromSymbol(
            long skipRows,
            long rowCount,
            long srcFixFd,
            SymbolTable symbolTable,
            int dstColumnType,
            long dstFixFd,
            long dstVarFd,
            FilesFacade ff,
            long appendPageSize,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        columnSizesSink.setSrcOffsets(skipRows * Integer.BYTES, -1);
        long symbolMapAddress = TableUtils.mapAppendColumnBuffer(ff, srcFixFd, skipRows * Integer.BYTES, rowCount * Integer.BYTES, false, memoryTag);

        try {
            switch (ColumnType.tagOf(dstColumnType)) {
                case ColumnType.STRING:
                    convertSymbolToString(rowCount, symbolMapAddress, dstFixFd, dstVarFd, ff, appendPageSize, symbolTable, columnSizesSink);
                    return;
                case ColumnType.VARCHAR:
                    convertSymbolToVarchar(rowCount, symbolMapAddress, dstFixFd, dstVarFd, ff, appendPageSize, symbolTable, columnSizesSink);
                    return;
            }
            Var2FixedConverter<CharSequence> converter = getConverterFromVarToFixed(ColumnType.SYMBOL, dstColumnType);
            convertSymbolToFixed(rowCount, symbolMapAddress, dstFixFd, ff, appendPageSize, symbolTable, columnSizesSink, dstColumnType, converter);
        } finally {
            TableUtils.mapAppendColumnBufferRelease(ff, symbolMapAddress, skipRows * Integer.BYTES, rowCount * Integer.BYTES, memoryTag);
        }
    }

    private static boolean convertFromVarchar(
            long skipRows,
            long rowCount,
            long srcFixFd,
            long srcVarFd,
            long dstFixFd,
            long dstVarFd,
            int dstColumnType,
            FilesFacade ff,
            long appendPageSize,
            SymbolMapWriterLite symbolMapWriter,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        long dataHi, skipDataSize;
        final VarcharTypeDriver driverInstance = VarcharTypeDriver.INSTANCE;
        try {
            skipDataSize = skipRows > 0 ? driverInstance.getDataVectorSizeAtFromFd(ff, srcFixFd, skipRows - 1) : 0;
            dataHi = driverInstance.getDataVectorSizeAtFromFd(ff, srcFixFd, skipRows + rowCount - 1);
        } catch (CairoException ex) {
            LOG.error().$("cannot read VARCHAR column data vector size, column data is corrupt will fall back reading file sizes [srcFixFd=").$(srcFixFd).I$();
            return false;
        }

        MemoryCMORImpl srcVarMem = null;
        MemoryCMORImpl srcFixMem = srcFixMemTL.get();
        long skipAuxOffset = driverInstance.getAuxVectorSize(skipRows);
        columnSizesSink.setSrcOffsets(skipDataSize, skipAuxOffset);

        try {
            if (dataHi > skipDataSize) {
                // Data can be fully inlined then no need to open / map data file
                srcVarMem = srcVarMemTL.get();
                srcVarMem.ofOffset(ff, srcVarFd, true, null, skipDataSize, dataHi, memoryTag, CairoConfiguration.O_NONE);
            }
            srcFixMem.ofOffset(ff, srcFixFd, true, null, skipAuxOffset, skipAuxOffset + driverInstance.getAuxVectorSize(rowCount), memoryTag, CairoConfiguration.O_NONE);

            switch (ColumnType.tagOf(dstColumnType)) {
                case ColumnType.STRING:
                    convertFromVarcharToString(
                            skipRows,
                            skipRows + rowCount,
                            dstFixFd,
                            dstVarFd,
                            ff,
                            appendPageSize,
                            srcVarMem,
                            srcFixMem,
                            columnSizesSink
                    );
                    return true;
                case ColumnType.SYMBOL:
                    convertFromVarcharToSymbol(skipRows, skipRows + rowCount, dstFixFd, ff, symbolMapWriter, srcVarMem, srcFixMem, columnSizesSink);
                    return true;
            }
            Var2FixedConverter<CharSequence> converter = getConverterFromVarToFixed(ColumnType.VARCHAR, dstColumnType);
            convertFromVarcharToFixed(skipRows, skipRows + rowCount, dstFixFd, ff, srcVarMem, srcFixMem, columnSizesSink, dstColumnType, converter);
        } finally {
            if (srcVarMem != null) {
                srcVarMem.detachFdClose();
            }
            srcFixMem.detachFdClose();
        }
        return true;
    }

    private static void convertFromVarcharToFixed(
            long rowLo,
            long rowHi,
            long dstFixFd,
            FilesFacade ff,
            @Nullable MemoryCMORImpl srcVarMem,
            MemoryCMORImpl srcFixMem,
            ColumnConversionOffsetSink columnSizesSink,
            int dstColumnType,
            Var2FixedConverter<CharSequence> converter
    ) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        int dstTypeSize = ColumnType.sizeOf(dstColumnType);

        try {
            dstFixMem.of(ff, dstFixFd, true, null, Files.PAGE_SIZE, (rowHi - rowLo) * dstTypeSize, memoryTag);
            dstFixMem.jumpTo(0);
            for (long i = rowLo; i < rowHi; i++) {
                Utf8Sequence utf8 = VarcharTypeDriver.getSplitValue(srcFixMem, srcVarMem, i, 1);
                converter.convert(utf8 != null ? utf8.asAsciiCharSequence() : null, dstFixMem);
            }
            assert dstFixMem.getAppendOffset() == (rowHi - rowLo) * dstTypeSize;
            columnSizesSink.setDestSizes(dstFixMem.getAppendOffset(), -1);
        } finally {
            dstFixMem.detachFdClose();
        }
    }

    private static void convertFromVarcharToString(
            long rowLo,
            long rowHi,
            long dstFixFd,
            long dstVarFd,
            FilesFacade ff,
            long appendPageSize,
            @Nullable MemoryCMORImpl srcVarMem,
            MemoryCMORImpl srcFixMem,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        StringSink sink = sinkUtf16TL.get();

        try {
            dstVarMem.of(ff, dstVarFd, true, null, appendPageSize, appendPageSize, memoryTag);
            dstVarMem.jumpTo(0);
            dstFixMem.of(ff, dstFixFd, true, null, appendPageSize, StringTypeDriver.INSTANCE.getAuxVectorSize(rowHi - rowLo), memoryTag);
            dstFixMem.jumpTo(0);
            dstFixMem.putLong(0L);

            for (long i = rowLo; i < rowHi; i++) {
                Utf8Sequence utf8 = VarcharTypeDriver.getSplitValue(srcFixMem, srcVarMem, i, 1);

                if (utf8 != null) {
                    sink.clear();
                    sink.put(utf8);
                    StringTypeDriver.appendValue(dstFixMem, dstVarMem, sink);
                } else {
                    StringTypeDriver.INSTANCE.appendNull(dstFixMem, dstVarMem);
                }
            }
            columnSizesSink.setDestSizes(dstVarMem.getAppendOffset(), dstFixMem.getAppendOffset());
        } finally {
            dstVarMem.detachFdClose();
            dstFixMem.detachFdClose();
        }
    }

    private static void convertFromVarcharToSymbol(
            long rowLo,
            long rowHi,
            long dstFixFd,
            FilesFacade ff,
            SymbolMapWriterLite symbolMapWriterLite,
            @Nullable MemoryCMORImpl srcVarMem,
            MemoryCMORImpl srcFixMem,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        StringSink sink = sinkUtf16TL.get();

        try {
            dstFixMem.of(ff, dstFixFd, true, null, Files.PAGE_SIZE, (rowHi - rowLo) * Integer.BYTES, memoryTag);
            dstFixMem.jumpTo(0);
            for (long i = rowLo; i < rowHi; i++) {
                Utf8Sequence utf8 = VarcharTypeDriver.getSplitValue(srcFixMem, srcVarMem, i, 1);

                if (utf8 != null) {
                    sink.clear();
                    sink.put(utf8);
                    int symbol = symbolMapWriterLite.resolveSymbol(sink);
                    dstFixMem.putInt(symbol);
                } else {
                    int symbol = symbolMapWriterLite.resolveSymbol(null);
                    dstFixMem.putInt(symbol);
                }
            }
            columnSizesSink.setDestSizes(dstFixMem.getAppendOffset(), -1);
        } finally {
            dstFixMem.detachFdClose();
        }
    }

    private static void convertStringToFixed(
            long skipOffset,
            long rowCount,
            long dstFixFd,
            FilesFacade ff,
            MemoryCMORImpl srcVarMem,
            ColumnConversionOffsetSink columnSizesSink,
            int dstColumnType,
            Var2FixedConverter<CharSequence> converter
    ) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        int dstTypeSize = ColumnType.sizeOf(dstColumnType);
        assert dstTypeSize > 0;
        try {
            dstFixMem.of(ff, dstFixFd, true, null, Files.PAGE_SIZE, rowCount * dstTypeSize, memoryTag);
            dstFixMem.jumpTo(0);

            long offset = skipOffset;
            for (long i = 0; i < rowCount; i++) {
                CharSequence str = srcVarMem.getStrA(offset);
                offset += Vm.getStorageLength(str);
                converter.convert(str, dstFixMem);
            }
            assert dstFixMem.getAppendOffset() == rowCount * dstTypeSize;
            columnSizesSink.setDestSizes(dstFixMem.getAppendOffset(), -1);
        } finally {
            dstFixMem.detachFdClose();
        }
    }

    private static void convertStringToSymbol(
            long skipOffset,
            long rowCount,
            long dstFixFd,
            FilesFacade ff,
            SymbolMapWriterLite symbolMapWriter,
            MemoryCMORImpl srcVarMem,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        try {
            dstFixMem.of(ff, dstFixFd, true, null, Files.PAGE_SIZE, rowCount * Integer.BYTES, memoryTag);
            dstFixMem.jumpTo(0);
            long offset = skipOffset;
            for (long i = 0; i < rowCount; i++) {
                CharSequence str = srcVarMem.getStrA(offset);
                offset += Vm.getStorageLength(str);
                int symbolId = symbolMapWriter.resolveSymbol(str);
                dstFixMem.putInt(symbolId);
            }
            columnSizesSink.setDestSizes(dstFixMem.getAppendOffset(), -1);
        } finally {
            dstFixMem.detachFdClose();
        }
    }

    private static void convertStringToVarchar(
            long skipOffset,
            long rowCount,
            long dstFixFd,
            long dstVarFd,
            FilesFacade ff,
            long appendPageSize,
            MemoryCMORImpl srcVarMem,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        Utf8StringSink sink = sinkUtf8TL.get();

        try {
            dstVarMem.of(ff, dstVarFd, true, null, appendPageSize, appendPageSize, memoryTag);
            dstVarMem.jumpTo(0);

            dstFixMem.of(ff, dstFixFd, true, null, appendPageSize, rowCount * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, memoryTag);
            dstFixMem.jumpTo(0);

            long offset = skipOffset;
            for (long i = 0; i < rowCount; i++) {
                CharSequence str = srcVarMem.getStrA(offset);
                offset += Vm.getStorageLength(str);

                if (str != null) {
                    sink.clear();
                    sink.put(str);

                    VarcharTypeDriver.appendValue(dstFixMem, dstVarMem, sink);
                } else {
                    VarcharTypeDriver.appendValue(dstFixMem, dstVarMem, null);
                }
            }
            columnSizesSink.setDestSizes(dstVarMem.getAppendOffset(), dstFixMem.getAppendOffset());
        } finally {
            sink.clear();
            sink.resetCapacity();
            dstVarMem.detachFdClose();
            dstFixMem.detachFdClose();
            srcVarMem.detachFdClose();
        }
    }

    private static void convertSymbolToFixed(
            long rowCount,
            long symbolMapAddress,
            long dstFixFd,
            FilesFacade ff,
            long appendPageSize,
            SymbolTable symbolTable,
            ColumnConversionOffsetSink columnSizesSink,
            int dstColumnType,
            Var2FixedConverter<CharSequence> converter
    ) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        int dstSize = ColumnType.sizeOf(dstColumnType);
        assert dstSize > 0;

        try {
            dstFixMem.of(ff, dstFixFd, true, null, appendPageSize, rowCount * dstSize, memoryTag);
            dstFixMem.jumpTo(0);
            for (long lo = symbolMapAddress, hi = symbolMapAddress + rowCount * Integer.BYTES; lo < hi; lo += Integer.BYTES) {
                int symbol = Unsafe.getUnsafe().getInt(lo);
                CharSequence str = symbolTable.valueOf(symbol);
                converter.convert(str, dstFixMem);
            }
            assert dstFixMem.getAppendOffset() == rowCount * dstSize;
            columnSizesSink.setDestSizes(dstFixMem.getAppendOffset(), -1);
        } finally {
            dstFixMem.detachFdClose();
        }
    }

    private static void convertSymbolToString(
            long rowCount,
            long symbolMapAddress,
            long dstFixFd,
            long dstVarFd,
            FilesFacade ff,
            long appendPageSize,
            SymbolTable symbolTable,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();

        ColumnTypeDriver typeDriver = StringTypeDriver.INSTANCE;
        long dstFixSize = typeDriver.getAuxVectorSize(rowCount);

        try {
            dstFixMem.of(ff, dstFixFd, true, null, appendPageSize, dstFixSize, memoryTag);
            dstFixMem.jumpTo(0);
            dstFixMem.putLong(0);

            dstVarMem.of(ff, dstVarFd, true, null, appendPageSize, appendPageSize, memoryTag);
            dstVarMem.jumpTo(0);

            for (long lo = symbolMapAddress, hi = symbolMapAddress + rowCount * Integer.BYTES; lo < hi; lo += Integer.BYTES) {
                int symbol = Unsafe.getUnsafe().getInt(lo);
                CharSequence str = symbolTable.valueOf(symbol);
                if (str != null) {
                    StringTypeDriver.appendValue(dstFixMem, dstVarMem, str);
                } else {
                    typeDriver.appendNull(dstFixMem, dstVarMem);
                }
            }
            columnSizesSink.setDestSizes(dstVarMem.getAppendOffset(), dstFixMem.getAppendOffset());
        } finally {
            dstFixMem.detachFdClose();
            dstVarMem.detachFdClose();
        }
    }

    private static void convertSymbolToVarchar(
            long rowCount,
            long symbolMapAddress,
            long dstFixFd,
            long dstVarFd,
            FilesFacade ff,
            long appendPageSize,
            SymbolTable symbolTable,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        Utf8StringSink sink = sinkUtf8TL.get();

        try {
            ColumnTypeDriver typeDriver = VarcharTypeDriver.INSTANCE;
            dstFixMem.of(ff, dstFixFd, true, null, appendPageSize, typeDriver.getAuxVectorSize(rowCount), memoryTag);
            dstFixMem.jumpTo(0);

            dstVarMem.of(ff, dstVarFd, true, null, appendPageSize, appendPageSize, memoryTag);
            dstVarMem.jumpTo(0);

            for (long lo = symbolMapAddress, hi = symbolMapAddress + rowCount * Integer.BYTES; lo < hi; lo += Integer.BYTES) {
                int symbol = Unsafe.getUnsafe().getInt(lo);
                CharSequence str = symbolTable.valueOf(symbol);
                if (str != null) {
                    sink.clear();
                    sink.put(str);
                    VarcharTypeDriver.appendValue(dstFixMem, dstVarMem, sink);
                } else {
                    typeDriver.appendNull(dstFixMem, dstVarMem);
                }
            }
            columnSizesSink.setDestSizes(dstVarMem.getAppendOffset(), dstFixMem.getAppendOffset());
        } finally {
            sink.clear();
            sink.resetCapacity();
            dstFixMem.detachFdClose();
            dstVarMem.detachFdClose();
        }
    }

    private static Fixed2VarConverter getFixedToVarConverter(int srcColumnType, int dstColumnType) {
        switch (ColumnType.tagOf(srcColumnType)) {
            case ColumnType.INT:
                return converterFromInt2String;
            case ColumnType.UUID:
                return converterFromUuid2String;
            case ColumnType.IPv4:
                return converterFromIPv42String;
            case ColumnType.SHORT:
                return converterFromShort2String;
            case ColumnType.BYTE:
                return converterFromByte2String;
            case ColumnType.CHAR:
                return converterFromChar2String;
            case ColumnType.LONG:
                return converterFromLong2String;
            case ColumnType.DOUBLE:
                return converterFromDouble2String;
            case ColumnType.FLOAT:
                return converterFromFloat2String;
            case ColumnType.DATE:
                return converterFromDate2String;
            case ColumnType.TIMESTAMP:
                return ColumnType.getTimestampDriver(srcColumnType).getConverterTimestamp2Str();
            case ColumnType.BOOLEAN:
                return converterFromBoolean2String;
            default:
                throw unsupportedConversion(srcColumnType, dstColumnType);
        }
    }

    private static void str2Boolean(CharSequence str, MemoryA mem) {
        mem.putBool(str != null && SqlKeywords.isTrueKeyword(str));
    }

    private static void str2Byte(CharSequence str, MemoryA memoryCMARW) {
        // Same as CAST(str as BYTE), same null, overflow problems
        byte num = (byte) Numbers.parseIntQuiet(str);
        memoryCMARW.putByte(num);
    }

    private static void str2Char(CharSequence str, MemoryA mem) {
        mem.putChar(str == null || str.length() == 0 ? 0 : str.charAt(0));
    }

    private static void str2Date(CharSequence str, MemoryA mem) {
        if (str != null) {
            try {
                mem.putLong(MicrosTimestampDriver.floor(str) / 1000);
                return;
            } catch (NumericException e) {
                // Fall through
            }
        }
        mem.putLong(Numbers.LONG_NULL);
    }

    private static void str2Double(CharSequence str, MemoryA mem) {
        try {
            if (str != null) {
                mem.putDouble(Numbers.parseDouble(str));
                return;
            }
        } catch (NumericException e) {
            // Fall through
        }
        mem.putDouble(Double.NaN);
    }

    private static void str2Float(CharSequence str, MemoryA mem) {
        try {
            if (str != null) {
                mem.putFloat(Numbers.parseFloat(str));
                return;
            }
        } catch (NumericException e) {
            // Fall through
        }
        mem.putFloat(Float.NaN);
    }

    private static void str2Int(CharSequence str, MemoryA mem) {
        if (str != null) {
            try {
                mem.putInt(Numbers.parseInt(str));
                return;
            } catch (NumericException e) {
                // Fall through
            }
        }
        mem.putInt(Numbers.INT_NULL);
    }

    private static void str2IpV4(CharSequence str, MemoryA mem) {
        mem.putInt(Numbers.parseIPv4Quiet(str));
    }

    private static void str2Long(CharSequence str, MemoryA mem) {
        if (str != null) {
            try {
                long num = Numbers.parseLong(str);
                mem.putLong(num);
                return;
            } catch (NumericException e) {
                // Fall through
            }
        }
        mem.putLong(Numbers.LONG_NULL);
    }

    private static void str2Short(CharSequence value, MemoryA mem) {
        // Same as CAST(str as SHORT), same null, overflow problems
        try {
            if (value != null) {
                mem.putShort((short) Numbers.parseInt(value));
                return;
            }
        } catch (NumericException e) {
            // Fall through
        }
        mem.putShort((short) 0);
    }

    private static void str2Uuid(CharSequence str, MemoryA mem) {
        if (str != null) {
            try {
                Uuid.checkDashesAndLength(str);
                long uuidHi = Uuid.parseHi(str);
                long uuidLo = Uuid.parseLo(str);
                mem.putLong(uuidLo);
                mem.putLong(uuidHi);
                return;
            } catch (NumericException e) {
                // Fall through
            }
        }

        mem.putLong(Numbers.LONG_NULL);
        mem.putLong(Numbers.LONG_NULL);
    }

    private static boolean stringFromBoolean(long srcAddr, CharSink<?> sink) {
        byte value = Unsafe.getUnsafe().getByte(srcAddr);
        sink.put(value != 0);
        return true;
    }

    private static boolean stringFromByte(long srcAddr, CharSink<?> sink) {
        byte value = Unsafe.getUnsafe().getByte(srcAddr);
        sink.put(value);
        return true;
    }

    private static boolean stringFromChar(long srcAddr, CharSink<?> sink) {
        char value = Unsafe.getUnsafe().getChar(srcAddr);
        if (value != 0) {
            sink.put(value);
            return true;
        }
        return false;
    }

    private static boolean stringFromDate(long srcAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(srcAddr);
        if (value != Numbers.LONG_NULL) {
            sink.putISODateMillis(value);
            return true;
        }
        return false;
    }

    private static boolean stringFromDouble(long srcAddr, CharSink<?> sink) {
        double value = Unsafe.getUnsafe().getDouble(srcAddr);
        if (!Numbers.isNull(value)) {
            sink.put(value);
            return true;
        }
        return false;
    }

    private static boolean stringFromFloat(long srcAddr, CharSink<?> sink) {
        float value = Unsafe.getUnsafe().getFloat(srcAddr);
        if (!Numbers.isNull(value)) {
            sink.put(value);
            return true;
        }
        return false;
    }

    private static boolean stringFromIPv4(long srcAddr, CharSink<?> sink) {
        int value = Unsafe.getUnsafe().getInt(srcAddr);
        if (value != Numbers.IPv4_NULL) {
            Numbers.intToIPv4Sink(sink, value);
            return true;
        }
        return false;
    }

    private static boolean stringFromInt(long srcAddr, CharSink<?> sink) {
        int value = Unsafe.getUnsafe().getInt(srcAddr);
        if (value != Numbers.INT_NULL) {
            sink.put(value);
            return true;
        }
        return false;
    }

    private static boolean stringFromLong(long srcAddr, CharSink<?> sink) {
        long value = Unsafe.getUnsafe().getLong(srcAddr);
        if (value != Numbers.LONG_NULL) {
            sink.put(value);
            return true;
        }
        return false;
    }

    private static boolean stringFromShort(long srcAddr, CharSink<?> sink) {
        short value = Unsafe.getUnsafe().getShort(srcAddr);
        sink.put(value);
        return true;
    }

    private static boolean stringFromUuid(long srcAddr, CharSink<?> sink) {
        long lo = Unsafe.getUnsafe().getLong(srcAddr);
        long hi = Unsafe.getUnsafe().getLong(srcAddr + 8L);
        if (lo != Numbers.LONG_NULL || hi != Numbers.LONG_NULL) {
            Numbers.appendUuid(lo, hi, sink);
            return true;
        }
        return false;
    }

    private static CairoException unsupportedConversion(int srcColumnType, int dstColumnType) {
        return CairoException.critical(0)
                .put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType))
                .put(" to ").put(ColumnType.nameOf(dstColumnType));
    }

    @FunctionalInterface
    public interface Fixed2VarConverter {
        boolean convert(long fixedAddr, CharSink<?> stringSink);
    }

    @FunctionalInterface
    public interface Var2FixedConverter<T> {
        void convert(T srcVar, MemoryA dstFixMem);
    }
}
