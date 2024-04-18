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
            long appendPageSize
    ) {
        if (ColumnType.isSymbol(srcColumnType)) {
            assert symbolTable != null;
            return convertFromSymbol(rowCount, srcFixFd, symbolTable, dstColumnType, dstFixFd, dstVarFd, symbolMapWriter, ff, appendPageSize);
        }
        if (ColumnType.isFixedSize(srcColumnType) && ColumnType.isFixedSize(dstColumnType)) {
            return convertFixedToFixed(rowCount, srcFixFd, dstFixFd, srcColumnType, dstColumnType, ff, appendPageSize);
        } else if (ColumnType.isVarSize(srcColumnType)) {
            switch (srcColumnType) {
                case ColumnType.STRING:
                    return convertFromString(rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, dstColumnType, ff, appendPageSize, symbolMapWriter);
                case ColumnType.VARCHAR:
                    return convertFromVarchar(rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, dstColumnType, ff, appendPageSize, symbolMapWriter);
                default:
                    closeFds(ff, srcFixFd, srcVarFd, dstFixFd, -1);
                    throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
            }
        } else {
            closeFds(ff, srcFixFd, srcVarFd, dstFixFd, dstVarFd);
            throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
        }
    }

    private static void closeFds(FilesFacade ff, int srcFixFd, int dstFixFd, int dstVarFd, int srcVarFd) {
        ff.close(srcFixFd);
        ff.close(srcVarFd);
        ff.close(dstFixFd);
        ff.close(dstVarFd);
    }

    private static boolean convertFixedToFixed(long rowCount, int srcFixFd, int dstFixFd, int srcColumnType, int dstColumnType, FilesFacade ff, long appendPageSize) {
        closeFds(ff, srcFixFd, dstFixFd, -1, -1);
        throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
    }

    private static boolean convertFromString(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, int dstColumnType, FilesFacade ff, long appendPageSize, SymbolMapWriterLite symbolMapWriter) {
        long dataSize;
        try {
            dataSize = StringTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, srcFixFd, rowCount - 1);
            if (dataSize < StringTypeDriver.INSTANCE.getDataVectorMinEntrySize() && rowCount > 0) {
                throw CairoException.nonCritical().put("String column data vector size is less than minimum entry size [size=").put(dataSize)
                        .put(", srcFixFd=").put(srcFixFd).put(']');
            }
            ff.close(srcFixFd);
        } catch (CairoException ex) {
            LOG.error().$("cannot read STRING column data vector size, column data is corrupt will fall back reading file sizes [srcFixFd=").$(srcFixFd)
                    .$(", errno=").$(ex.getErrno())
                    .$(", error=").$(ex.getFlyweightMessage())
                    .I$();
            return false;
        }

        MemoryCMORImpl srcVarMem = srcVarMemTL.get();

        try {
            srcVarMem.ofOffset(ff, srcVarFd, null, 0, dataSize, memoryTag, CairoConfiguration.O_NONE);

            switch (dstColumnType) {
                case ColumnType.VARCHAR:
                    convertStringToVarchar(rowCount, dstFixFd, dstVarFd, ff, appendPageSize, srcVarMem);
                    break;
                case ColumnType.SYMBOL:
                    convertStringToSymbol(rowCount, dstFixFd, ff, symbolMapWriter, srcVarMem);
                    break;
                default:
                    closeFds(ff, -1, srcVarFd, dstFixFd, dstVarFd);
                    throw CairoException.critical(0).put("Unsupported conversion from STRING to ").put(ColumnType.nameOf(dstColumnType));
            }
        } finally {
            srcVarMem.close();
        }
        return true;
    }

    private static boolean convertFromSymbol(long rowCount, int srcFixFd, SymbolTable symbolTable, int dstColumnType, int dstFixFd, int dstVarFd, SymbolMapWriterLite symbolMapWriter, FilesFacade ff, long appendPageSize) {
        long symbolMapAddressRaw;
        try {
            symbolMapAddressRaw = TableUtils.mapAppendColumnBuffer(ff, srcFixFd, 0, rowCount * Integer.BYTES, false, memoryTag);
        } catch (Throwable th) {
            closeFds(ff, srcFixFd, -1, dstFixFd, dstVarFd);
            throw th;
        }

        try {
            long symbolMapAddress = Math.abs(symbolMapAddressRaw);
            switch (ColumnType.tagOf(dstColumnType)) {
                case ColumnType.STRING:
                    convertSymbolToString(rowCount, symbolMapAddress, dstFixFd, dstVarFd, ff, appendPageSize, symbolTable);
                    break;
                case ColumnType.VARCHAR:
                    convertSymbolToVarchar(rowCount, symbolMapAddress, dstFixFd, dstVarFd, ff, appendPageSize, symbolTable);
                    break;
                default:
                    closeFds(ff, -1, -1, dstFixFd, dstVarFd);
                    throw CairoException.critical(0).put("Unsupported conversion from SYMBOL to ").put(ColumnType.nameOf(dstColumnType));
            }
        } finally {
            TableUtils.mapAppendColumnBufferRelease(ff, symbolMapAddressRaw, 0, rowCount * Integer.BYTES, memoryTag);
            ff.close(srcFixFd);
        }
        return true;
    }

    private static boolean convertFromVarchar(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, int dstColumnType, FilesFacade ff, long appendPageSize, SymbolMapWriterLite symbolMapWriter) {
        long dataSize;
        try {
            dataSize = VarcharTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, srcFixFd, rowCount - 1);
        } catch (CairoException ex) {
            LOG.error().$("cannot read VARCHAR column data vector size, column data is corrupt will fall back reading file sizes [srcFixFd=").$(srcFixFd).I$();
            return false;
        }

        MemoryCMORImpl srcVarMem = srcVarMemTL.get();
        MemoryCMORImpl srcFixMem = srcFixMemTL.get();

        try {
            srcVarMem.ofOffset(ff, srcVarFd, null, 0, dataSize, memoryTag, CairoConfiguration.O_NONE);
            srcFixMem.ofOffset(ff, srcFixFd, null, 0, VarcharTypeDriver.INSTANCE.getAuxVectorSize(rowCount), memoryTag, CairoConfiguration.O_NONE);

            switch (ColumnType.tagOf(dstColumnType)) {
                case ColumnType.STRING:
                    convertFromVarcharToString(rowCount, dstFixFd, dstVarFd, ff, appendPageSize, srcVarMem, srcFixMem);
                    break;
                case ColumnType.SYMBOL:
                    convertFromVarcharToSymbol(rowCount, dstFixFd, ff, symbolMapWriter, srcVarMem, srcFixMem);
                    break;
                default:
                    closeFds(ff, srcFixFd, srcVarFd, dstFixFd, dstVarFd);
                    throw CairoException.critical(0).put("Unsupported conversion from VARCHAR to ").put(ColumnType.nameOf(dstColumnType));
            }
        } finally {
            srcVarMem.close();
            srcFixMem.close();
        }
        return true;
    }

    private static void convertFromVarcharToString(long rowCount, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize, MemoryCMORImpl srcVarMem, MemoryCMORImpl srcFixMem) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();

        dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, memoryTag);
        dstVarMem.jumpTo(0);
        dstFixMem.of(ff, dstFixFd, null, appendPageSize, StringTypeDriver.INSTANCE.getAuxVectorSize(rowCount), memoryTag);
        dstFixMem.jumpTo(0);
        dstFixMem.putLong(0L);
        StringSink sink = sinkUtf16TL.get();

        try {
            for (long i = 0; i < rowCount; i++) {
                Utf8Sequence utf8 = VarcharTypeDriver.getValue(i, srcVarMem, srcFixMem, 1);

                if (utf8 != null) {
                    sink.clear();
                    sink.put(utf8);
                    StringTypeDriver.appendValue(dstVarMem, dstFixMem, sink);
                } else {
                    StringTypeDriver.INSTANCE.appendNull(dstVarMem, dstFixMem);
                }
            }
        } finally {
            dstVarMem.close(true);
            dstFixMem.close(true);
        }
    }

    private static void convertFromVarcharToSymbol(long rowCount, int dstFixFd, FilesFacade ff, SymbolMapWriterLite symbolMapWriterLite, MemoryCMORImpl srcVarMem, MemoryCMORImpl srcFixMem) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();

        dstFixMem.of(ff, dstFixFd, null, Files.PAGE_SIZE, rowCount * Integer.BYTES, memoryTag);
        dstFixMem.jumpTo(0);
        StringSink sink = sinkUtf16TL.get();

        try {
            for (long i = 0; i < rowCount; i++) {
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
        } finally {
            dstFixMem.close(true);
        }
    }

    private static void convertStringToSymbol(long rowCount, int dstFixFd, FilesFacade ff, SymbolMapWriterLite symbolMapWriter, MemoryCMORImpl srcVarMem) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        dstFixMem.of(ff, dstFixFd, null, Files.PAGE_SIZE, rowCount * Integer.BYTES, memoryTag);
        dstFixMem.jumpTo(0);
        try {
            long offset = 0;
            for (long i = 0; i < rowCount; i++) {
                CharSequence str = srcVarMem.getStrA(offset);
                offset += Vm.getStorageLength(str);
                int symbolId = symbolMapWriter.resolveSymbol(str);
                dstFixMem.putInt(symbolId);
            }
        } finally {
            dstFixMem.close(true);
        }
    }

    private static void convertStringToVarchar(long rowCount, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize, MemoryCMORImpl srcVarMem) {
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        Utf8StringSink sink = sinkUtf8TL.get();

        try {
            dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, memoryTag);
            dstVarMem.jumpTo(0);

            dstFixMem.of(ff, dstFixFd, null, appendPageSize, rowCount * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, memoryTag);
            dstFixMem.jumpTo(0);

            long offset = 0;
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
        } finally {
            sink.clear();
            sink.resetCapacity();
            dstVarMem.close(true);
            dstFixMem.close(true);
            srcVarMem.close();
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

    private static void convertSymbolToString(long rowCount, long symbolMapAddress, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize, SymbolTable symbolTable) {
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();

        ColumnTypeDriver typeDriver = StringTypeDriver.INSTANCE;
        dstFixMem.of(ff, dstFixFd, null, appendPageSize, typeDriver.getAuxVectorSize(rowCount), memoryTag);
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
        } finally {
            dstFixMem.close(true);
            dstVarMem.close(true);
        }
    }

    private static void convertSymbolToVarchar(long rowCount, long symbolMapAddress, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize, SymbolTable symbolTable) {
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
        } finally {
            sink.clear();
            sink.resetCapacity();
            dstFixMem.close(true);
            dstVarMem.close(true);
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
