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

package io.questdb.griffin;

import io.questdb.cairo.*;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.vm.MemoryCMORImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;
import org.jetbrains.annotations.Nullable;

public class ColumnTypeConverter {
    private static final Log LOG = LogFactory.getLog(ColumnTypeConverter.class);
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
            int srcFixFd,
            int srcVarFd,
            @Nullable SymbolTable symbolTable,
            int dstColumnType,
            int dstFixFd,
            int dstVarFd,
            @Nullable SymbolMapWriterLite symbolMapWriter,
            FilesFacade ff,
            long appendPageSize,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        if (ColumnType.isSymbol(srcColumnType)) {
            assert symbolTable != null;
            return convertFromSymbol(skipRows, rowCount, srcFixFd, symbolTable, dstColumnType, dstFixFd, dstVarFd, ff, appendPageSize, columnSizesSink);
        }
        if (ColumnType.isFixedSize(srcColumnType) && ColumnType.isFixedSize(dstColumnType)) {
            return convertFixedToFixedRaw(rowCount, skipRows, srcFixFd, dstFixFd, srcColumnType, dstColumnType, ff, appendPageSize);
        } else if (ColumnType.isVarSize(srcColumnType)) {
            switch (srcColumnType) {
                case ColumnType.STRING:
                    return convertFromString(skipRows, rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, dstColumnType, ff, appendPageSize, symbolMapWriter, columnSizesSink);
                case ColumnType.VARCHAR:
                    return convertFromVarchar(skipRows, rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, dstColumnType, ff, appendPageSize, symbolMapWriter, columnSizesSink);
                default:
                    throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
            }
        } else {
            throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
        }
    }

    private static boolean convertFixedToFixed(long rowCount, long skipRows, int srcFixFd, int dstFixFd, int srcColumnType, int dstColumnType, FilesFacade ff, long appendPageSize) {
        MemoryCMORImpl srcFixMem = srcFixMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        final long srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        final long dstColumnTypeSize = ColumnType.sizeOf(dstColumnType);

        try {
            srcFixMem.ofOffset(ff, srcFixFd, null, srcColumnTypeSize * skipRows, srcColumnTypeSize * rowCount, memoryTag, CairoConfiguration.O_NONE);
            dstFixMem.of(ff, dstFixFd, null, appendPageSize, dstColumnTypeSize * rowCount, memoryTag);
            dstFixMem.jumpTo(0);
            long succeeded = ConvertersNative.fixedToFixed(srcFixMem.getPageAddress(0), srcColumnType, dstFixMem.getPageAddress(0), dstColumnType, rowCount);
            switch ((int) succeeded) {
                case ConvertersNative.ConversionError.NONE:
                    return true;
                case ConvertersNative.ConversionError.UNSUPPORTED_CAST:
                    throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));

                default:
                    throw CairoException.critical(0).put("Unknown return code from native call: ").put(succeeded);
            }
        } finally {
            srcFixMem.detachFdClose();
            dstFixMem.detachFdClose();
        }
    }

    private static boolean convertFixedToFixedRaw(long rowCount, long skipRows, int srcFixFd, int dstFixFd, int srcColumnType, int dstColumnType, FilesFacade ff, long appendPageSize) {
        final long srcColumnTypeSize = ColumnType.sizeOf(srcColumnType);
        final long dstColumnTypeSize = ColumnType.sizeOf(dstColumnType);
        long srcMapAddressRaw = 0;
        long dstMapAddressRaw = 0;

        try {
            srcMapAddressRaw = rowCount > 0 ?
                    TableUtils.mapAppendColumnBuffer(ff, srcFixFd, skipRows * srcColumnTypeSize, rowCount * srcColumnType, false, memoryTag)
                    : 0;

            if (rowCount > 0) {
                ff.truncate(dstFixFd, rowCount * dstColumnTypeSize);
                dstMapAddressRaw = TableUtils.mapAppendColumnBuffer(ff, dstFixFd, 0, rowCount * dstColumnTypeSize, true, memoryTag);
            }

            long succeeded = ConvertersNative.fixedToFixed(Math.abs(srcMapAddressRaw), srcColumnType, Math.abs(dstMapAddressRaw), dstColumnType, rowCount);
            switch ((int) succeeded) {
                case ConvertersNative.ConversionError.NONE:
                    return true;
                case ConvertersNative.ConversionError.UNSUPPORTED_CAST:
                    throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
                default:
                    throw CairoException.critical(0).put("Unknown return code from native call: ").put(succeeded);
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            TableUtils.mapAppendColumnBufferRelease(ff, srcMapAddressRaw, skipRows * srcColumnTypeSize, rowCount * srcColumnType, memoryTag);
            TableUtils.mapAppendColumnBufferRelease(ff, dstMapAddressRaw, 0, rowCount * dstColumnType, memoryTag);
        }
    }


    private static boolean convertFromString(long skipRows, long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, int dstColumnType, FilesFacade ff, long appendPageSize, SymbolMapWriterLite symbolMapWriter, ColumnConversionOffsetSink columnSizesSink) {
        long skipDataSize;
        long dataSize;
        StringTypeDriver typeDriver = StringTypeDriver.INSTANCE;
        try {
            skipDataSize = skipRows > 0 ? typeDriver.getDataVectorSizeAtFromFd(ff, srcFixFd, skipRows - 1) : 0;
            dataSize = typeDriver.getDataVectorSizeAtFromFd(ff, srcFixFd, skipRows + rowCount - 1);
            if (dataSize < typeDriver.getDataVectorMinEntrySize() && rowCount > 0) {
                throw CairoException.nonCritical().put("String column data vector size is less than minimum entry size [size=").put(dataSize)
                        .put(", srcFixFd=").put(srcFixFd).put(']');
            }
        } catch (CairoException ex) {
            LOG.error().$("cannot read STRING column data vector size, column data is corrupt will fall back reading file sizes [srcFixFd=").$(srcFixFd)
                    .$(", errno=").$(ex.getErrno())
                    .$(", error=").$(ex.getFlyweightMessage())
                    .I$();
            return false;
        }

        columnSizesSink.setSrcOffsets(skipDataSize, typeDriver.getAuxVectorSize(skipRows));
        MemoryCMORImpl srcVarMem = srcVarMemTL.get();

        try {
            srcVarMem.ofOffset(ff, srcVarFd, null, skipDataSize, dataSize, memoryTag, CairoConfiguration.O_NONE);

            switch (dstColumnType) {
                case ColumnType.VARCHAR:
                    convertStringToVarchar(skipDataSize, rowCount, dstFixFd, dstVarFd, ff, appendPageSize, srcVarMem, columnSizesSink);
                    break;
                case ColumnType.SYMBOL:
                    convertStringToSymbol(skipDataSize, rowCount, dstFixFd, ff, symbolMapWriter, srcVarMem, columnSizesSink);
                    break;
                default:
                    throw CairoException.critical(0).put("Unsupported conversion from STRING to ").put(ColumnType.nameOf(dstColumnType));
            }
        } finally {
            srcVarMem.detachFdClose();
        }
        return true;
    }

    private static boolean convertFromSymbol(
            long skipRows,
            long rowCount,
            int srcFixFd,
            SymbolTable symbolTable,
            int dstColumnType,
            int dstFixFd,
            int dstVarFd,
            FilesFacade ff,
            long appendPageSize,
            ColumnConversionOffsetSink columnSizesSink
    ) {
        long symbolMapAddressRaw;
        columnSizesSink.setSrcOffsets(skipRows * Integer.BYTES, -1);
        symbolMapAddressRaw = rowCount > 0 ?
                TableUtils.mapAppendColumnBuffer(ff, srcFixFd, skipRows * Integer.BYTES, rowCount * Integer.BYTES, false, memoryTag)
                : 0;

        try {
            long symbolMapAddress = Math.abs(symbolMapAddressRaw);
            switch (ColumnType.tagOf(dstColumnType)) {
                case ColumnType.STRING:
                    convertSymbolToString(rowCount, symbolMapAddress, dstFixFd, dstVarFd, ff, appendPageSize, symbolTable, columnSizesSink);
                    break;
                case ColumnType.VARCHAR:
                    convertSymbolToVarchar(rowCount, symbolMapAddress, dstFixFd, dstVarFd, ff, appendPageSize, symbolTable, columnSizesSink);
                    break;
                default:
                    throw CairoException.critical(0).put("Unsupported conversion from SYMBOL to ").put(ColumnType.nameOf(dstColumnType));
            }
        } finally {
            TableUtils.mapAppendColumnBufferRelease(ff, symbolMapAddressRaw, skipRows * Integer.BYTES, rowCount * Integer.BYTES, memoryTag);
        }
        return true;
    }

    private static boolean convertFromVarchar(long skipRows, long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, int dstColumnType, FilesFacade ff, long appendPageSize, SymbolMapWriterLite symbolMapWriter, ColumnConversionOffsetSink columnSizesSink) {
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
                srcVarMem.ofOffset(ff, srcVarFd, null, skipDataSize, dataHi, memoryTag, CairoConfiguration.O_NONE);
            }
            srcFixMem.ofOffset(ff, srcFixFd, null, skipAuxOffset, skipAuxOffset + driverInstance.getAuxVectorSize(rowCount), memoryTag, CairoConfiguration.O_NONE);

            switch (ColumnType.tagOf(dstColumnType)) {
                case ColumnType.STRING:
                    convertFromVarcharToString(skipRows, skipRows + rowCount, dstFixFd, dstVarFd, ff, appendPageSize, srcVarMem, srcFixMem, columnSizesSink);
                    break;
                case ColumnType.SYMBOL:
                    convertFromVarcharToSymbol(skipRows, skipRows + rowCount, dstFixFd, ff, symbolMapWriter, srcVarMem, srcFixMem, columnSizesSink);
                    break;
                default:
                    throw CairoException.critical(0).put("Unsupported conversion from VARCHAR to ").put(ColumnType.nameOf(dstColumnType));
            }
        } finally {
            if (srcVarMem != null) {
                srcVarMem.detachFdClose();
            }
            srcFixMem.detachFdClose();
        }
        return true;
    }

    private static void convertFromVarcharToString(long rowLo, long rowHi, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize,
                                                   @Nullable MemoryCMORImpl srcVarMem, MemoryCMORImpl srcFixMem, ColumnConversionOffsetSink columnSizesSink) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();

        dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, memoryTag);
        dstVarMem.jumpTo(0);
        dstFixMem.of(ff, dstFixFd, null, appendPageSize, StringTypeDriver.INSTANCE.getAuxVectorSize(rowHi - rowLo), memoryTag);
        dstFixMem.jumpTo(0);
        dstFixMem.putLong(0L);
        StringSink sink = sinkUtf16TL.get();

        try {
            for (long i = rowLo; i < rowHi; i++) {
                Utf8Sequence utf8 = VarcharTypeDriver.getValue(i, srcVarMem, srcFixMem, 1);

                if (utf8 != null) {
                    sink.clear();
                    sink.put(utf8);
                    StringTypeDriver.appendValue(dstVarMem, dstFixMem, sink);
                } else {
                    StringTypeDriver.INSTANCE.appendNull(dstVarMem, dstFixMem);
                }
            }
            columnSizesSink.setDestSizes(dstVarMem.getAppendOffset(), dstFixMem.getAppendOffset());
        } finally {
            dstVarMem.detachFdClose();
            dstFixMem.detachFdClose();
        }
    }

    private static void convertFromVarcharToSymbol(long rowLo, long rowHi, int dstFixFd, FilesFacade ff, SymbolMapWriterLite symbolMapWriterLite,
                                                   @Nullable MemoryCMORImpl srcVarMem, MemoryCMORImpl srcFixMem, ColumnConversionOffsetSink columnSizesSink) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();

        dstFixMem.of(ff, dstFixFd, null, Files.PAGE_SIZE, (rowHi - rowLo) * Integer.BYTES, memoryTag);
        dstFixMem.jumpTo(0);
        StringSink sink = sinkUtf16TL.get();

        try {
            for (long i = rowLo; i < rowHi; i++) {
                Utf8Sequence utf8 = VarcharTypeDriver.getValue(i, srcVarMem, srcFixMem, 1);

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

    private static void convertStringToSymbol(long skipOffset, long rowCount, int dstFixFd, FilesFacade ff, SymbolMapWriterLite symbolMapWriter, MemoryCMORImpl srcVarMem, ColumnConversionOffsetSink columnSizesSink) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        dstFixMem.of(ff, dstFixFd, null, Files.PAGE_SIZE, rowCount * Integer.BYTES, memoryTag);
        dstFixMem.jumpTo(0);
        try {
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

    private static void convertStringToVarchar(long skipOffset, long rowCount, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize, MemoryCMORImpl srcVarMem, ColumnConversionOffsetSink columnSizesSink) {
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        Utf8StringSink sink = sinkUtf8TL.get();

        try {
            dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, memoryTag);
            dstVarMem.jumpTo(0);

            dstFixMem.of(ff, dstFixFd, null, appendPageSize, rowCount * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, memoryTag);
            dstFixMem.jumpTo(0);

            long offset = skipOffset;
            for (long i = 0; i < rowCount; i++) {
                CharSequence str = srcVarMem.getStrA(offset);
                offset += Vm.getStorageLength(str);

                if (str != null) {
                    sink.clear();
                    sink.put(str);

                    VarcharTypeDriver.appendValue(dstVarMem, dstFixMem, sink);
                } else {
                    VarcharTypeDriver.appendValue(dstVarMem, dstFixMem, null);
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

//    private static boolean convertStringToVarcharCorrupt(long rowCount, int srcVarFd, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize) {
//        MemoryCMORImpl srcVarMem = srcVarMemTL.get();
//        MemoryCMARW dstVarMem = dstVarMemTL.get();
//        MemoryCMARW dstFixMem = dstFixMemTL.get();
//        var sink = sinkUtf8TL.get();
//
//        boolean corrupt = false;
//        try {
//            srcVarMem.ofOffset(ff, srcVarFd, null, 0, ff.length(srcVarFd), MemoryTag.MMAP_TABLE_WRITER, CairoConfiguration.O_NONE);
//            dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, MemoryTag.MMAP_TABLE_WRITER);
//            dstVarMem.jumpTo(0);
//            dstFixMem.of(ff, dstFixFd, null, appendPageSize, VarcharTypeDriver.INSTANCE.auxRowsToBytes(rowCount), MemoryTag.MMAP_TABLE_WRITER);
//            dstFixMem.jumpTo(0);
//
//            long offset = 0;
//            for (long i = 0; i < rowCount; i++) {
//                final int length;
//                if (srcVarMem.size() < offset + 4) {
//                    srcVarMem.growToFileSize();
//                    if (srcVarMem.size() < offset + 4) {
//                        // Still not big enough, pad with nulls
//                        LOG.critical().$("cannot read STRING column data vector size, column data is corrupt, padding with NULL values [srcVarFd=").$(srcVarFd)
//                                .$(", rowOffset=").$(i).I$();
//                        corrupt = true;
//                        length = -1;
//                    } else {
//                        length = srcVarMem.getInt(offset);
//                    }
//                } else {
//                    length = srcVarMem.getInt(offset);
//                }
//
//                if (!corrupt && length > 0 && srcVarMem.size() < offset + Vm.getStorageLength(length)) {
//                    srcVarMem.growToFileSize();
//                    if (srcVarMem.size() < offset + Vm.getStorageLength(length)) {
//                        // Still not big enough, pad with nulls
//                        LOG.critical().$("cannot read STRING column data vector size, column data is corrupt, padding with NULL values [srcVarFd=").$(srcVarFd)
//                                .$(", rowOffset=").$(i).I$();
//                        corrupt = true;
//                    }
//                }
//
//                if (!corrupt) {
//                    offset = convertStrToVarchar(srcVarMem, offset, sink, dstVarMem, dstFixMem);
//                } else {
//                    VarcharTypeDriver.appendValue(dstVarMem, dstFixMem, null);
//                }
//            }
//        } finally {
//            sink.clear();
//            sink.resetCapacity();
//            dstVarMem.close(true);
//            dstFixMem.close(true);
//            srcVarMem.close();
//        }
//        return corrupt;
//    }

    private static void convertSymbolToString(long rowCount, long symbolMapAddress, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize, SymbolTable symbolTable, ColumnConversionOffsetSink columnSizesSink) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();

        ColumnTypeDriver typeDriver = StringTypeDriver.INSTANCE;
        long dstFixSize = typeDriver.getAuxVectorSize(rowCount);
        dstFixMem.of(ff, dstFixFd, null, appendPageSize, dstFixSize, memoryTag);
        dstFixMem.jumpTo(0);
        dstFixMem.putLong(0);

        dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, memoryTag);
        dstVarMem.jumpTo(0);

        try {
            for (long lo = symbolMapAddress, hi = symbolMapAddress + rowCount * Integer.BYTES; lo < hi; lo += Integer.BYTES) {
                int symbol = Unsafe.getUnsafe().getInt(lo);
                CharSequence str = symbolTable.valueOf(symbol);
                if (str != null) {
                    StringTypeDriver.appendValue(dstVarMem, dstFixMem, str);
                } else {
                    typeDriver.appendNull(dstVarMem, dstFixMem);
                }
            }
            columnSizesSink.setDestSizes(dstVarMem.getAppendOffset(), dstFixMem.getAppendOffset());
        } finally {
            dstFixMem.detachFdClose();
            dstVarMem.detachFdClose();
        }
    }

    private static void convertSymbolToVarchar(long rowCount, long symbolMapAddress, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize, SymbolTable symbolTable, ColumnConversionOffsetSink columnSizesSink) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();

        ColumnTypeDriver typeDriver = VarcharTypeDriver.INSTANCE;
        dstFixMem.of(ff, dstFixFd, null, appendPageSize, typeDriver.getAuxVectorSize(rowCount), memoryTag);
        dstFixMem.jumpTo(0);

        dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, memoryTag);
        dstVarMem.jumpTo(0);

        Utf8StringSink sink = sinkUtf8TL.get();
        try {
            for (long lo = symbolMapAddress, hi = symbolMapAddress + rowCount * Integer.BYTES; lo < hi; lo += Integer.BYTES) {
                int symbol = Unsafe.getUnsafe().getInt(lo);
                CharSequence str = symbolTable.valueOf(symbol);
                if (str != null) {
                    sink.clear();
                    sink.put(str);
                    VarcharTypeDriver.appendValue(dstVarMem, dstFixMem, sink);
                } else {
                    typeDriver.appendNull(dstVarMem, dstFixMem);
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


//    private static boolean convertVarcharToStringCorrupt(long rowCount, int srcVarFd, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize) {
//        MemoryCMORImpl srcVarMem = srcVarMemTL.get();
//        MemoryCMORImpl srcFixMem = srcFixMemTL.get();
//        MemoryCMARW dstVarMem = dstVarMemTL.get();
//        MemoryCMARW dstFixMem = dstFixMemTL.get();
//        StringSink sink = sinkUtf16TL.get();
//        boolean corrupt = false;
//
//        try {
//
//            srcFixMem.ofOffset(ff, srcVarFd, null, 0, VarcharTypeDriver.INSTANCE.getAuxVectorSize(rowCount), MemoryTag.MMAP_TABLE_WRITER, CairoConfiguration.O_NONE);
//            srcVarMem.ofOffset(ff, srcVarFd, null, 0, ff.length(srcVarFd), MemoryTag.MMAP_TABLE_WRITER, CairoConfiguration.O_NONE);
//
//            dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, MemoryTag.MMAP_TABLE_WRITER);
//            dstVarMem.jumpTo(0);
//            dstFixMem.of(ff, dstFixFd, null, appendPageSize, StringTypeDriver.INSTANCE.getAuxVectorSize(rowCount), MemoryTag.MMAP_TABLE_WRITER);
//            dstFixMem.jumpTo(0);
//
//            for (long i = 0; i < rowCount; i++) {
//                if (!corrupt) {
//                    Utf8Sequence utf8;
//                    try {
//                        utf8 = VarcharTypeDriver.getValueChecked(i, srcFixMem, srcVarMem, 1);
//                    } catch (CairoException ex) {
//                        srcVarMem.growToFileSize();
//                        srcFixMem.growToFileSize();
//
//                        try {
//                            utf8 = VarcharTypeDriver.getValueChecked(i, srcFixMem, srcVarMem, 1);
//                        } catch (CairoException ex2) {
//                            LOG.critical().$("cannot read VARCHAR column data vector size, column data is corrupt, padding with NULL values [srcVarFd=").$(srcVarFd)
//                                    .$(", rowOffset=").$(i).I$();
//                            corrupt = true;
//                            utf8 = null;
//                        }
//                    }
//
//                    if (!corrupt && utf8 != null) {
//                        sink.clear();
//                        sink.put(utf8);
//                        StringTypeDriver.appendValue(dstFixMem, dstVarMem, sink);
//                    } else {
//                        StringTypeDriver.INSTANCE.appendNull(dstFixMem, dstVarMem);
//                    }
//                } else {
//                    StringTypeDriver.INSTANCE.appendNull(dstFixMem, dstVarMem);
//                }
//            }
//        } finally {
//            sink.clear();
//            dstVarMem.close(true);
//            dstFixMem.close(true);
//            srcVarMem.close();
//        }
//        return corrupt;
//    }

}


