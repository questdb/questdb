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
import io.questdb.cairo.vm.MemoryCMORImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8StringSink;

public class ColumnTypeConverter {
    private static final Log LOG = LogFactory.getLog(ColumnTypeConverter.class);
    private static final ThreadLocal<MemoryCMARW> dstFixMemTL = new ThreadLocal<>(io.questdb.cairo.vm.MemoryCMARWImpl::new);
    private static final ThreadLocal<MemoryCMARW> dstVarMemTL = new ThreadLocal<>(io.questdb.cairo.vm.MemoryCMARWImpl::new);
    private static final ThreadLocal<StringSink> sinkUtf16TL = new ThreadLocal<>(StringSink::new);
    private static final ThreadLocal<Utf8StringSink> sinkUtf8TL = new ThreadLocal<>(Utf8StringSink::new);
    private static final ThreadLocal<MemoryCMORImpl> srcFixMemTL = new ThreadLocal<>(MemoryCMORImpl::new);
    private static final ThreadLocal<MemoryCMORImpl> srcVarMemTL = new ThreadLocal<>(MemoryCMORImpl::new);

    public static boolean convertColumn(
            long rowCount,
            int srcFixFd,
            int srcVarFd,
            int dstFixFd,
            int dstVarFd,
            int srcColumnType,
            int dstColumnType,
            FilesFacade ff,
            long appendPageSize
    ) {
        if (ColumnType.isFixedSize(srcColumnType) && ColumnType.isFixedSize(dstColumnType)) {
            return convertFixedToFixed(rowCount, srcFixFd, dstFixFd, srcColumnType, dstColumnType, ff, appendPageSize);
        } else if (ColumnType.isVarSize(srcColumnType) && ColumnType.isVarSize(dstColumnType)) {
            return convertVarToVar(rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, srcColumnType, dstColumnType, ff, appendPageSize);
        } else if (ColumnType.isFixedSize(srcColumnType) && ColumnType.isVarSize(dstColumnType)) {
            return convertFixedToVar(rowCount, srcFixFd, dstFixFd, dstVarFd, srcColumnType, dstColumnType, ff, appendPageSize);
        } else {
            return convertVarToFixed(rowCount, srcFixFd, srcVarFd, dstFixFd, srcColumnType, dstColumnType, ff, appendPageSize);
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

    private static boolean convertFixedToVar(long rowCount, int srcFixFd, int dstFixFd, int dstVarFd, int srcColumnType, int dstColumnType, FilesFacade ff, long appendPageSize) {
        closeFds(ff, srcFixFd, dstFixFd, dstVarFd, dstVarFd);
        throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
    }

    private static long convertStrToVarchar(MemoryCMORImpl srcVarMem, long offset, Utf8StringSink sink, MemoryCMARW dstVarMem, MemoryCMARW dstFixMem) {
        CharSequence str = srcVarMem.getStrA(offset);
        offset += Vm.getStorageLength(str);

        if (str != null) {
            sink.clear();
            sink.put(str);

            VarcharTypeDriver.appendValue(dstVarMem, dstFixMem, sink);
        } else {
            VarcharTypeDriver.appendValue(dstVarMem, dstFixMem, null);
        }
        return offset;
    }

    private static boolean convertStringToVarchar(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize) {
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
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        Utf8StringSink sink = sinkUtf8TL.get();

        try {
            srcVarMem.ofOffset(ff, srcVarFd, null, 0, dataSize, MemoryTag.MMAP_TABLE_WRITER, CairoConfiguration.O_NONE);
            dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, MemoryTag.MMAP_TABLE_WRITER);
            dstVarMem.jumpTo(0);

            dstFixMem.of(ff, dstFixFd, null, appendPageSize, rowCount * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, MemoryTag.MMAP_TABLE_WRITER);
            dstFixMem.jumpTo(0);

            long offset = 0;
            for (long i = 0; i < rowCount; i++) {
                offset = convertStrToVarchar(srcVarMem, offset, sink, dstVarMem, dstFixMem);
            }
        } finally {
            sink.clear();
            sink.resetCapacity();
            dstVarMem.close(true);
            dstFixMem.close(true);
            srcVarMem.close();
        }
        return true;
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

    private static boolean convertVarToFixed(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int srcColumnType, int dstColumnType, FilesFacade ff, long appendPageSize) {
        throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
    }

    private static boolean convertVarToVar(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, int srcColumnType, int dstColumnType, FilesFacade ff, long appendPageSize) {
        // Convert STRING to VARCHAR and back
        // Binary column usage is rare, conversions can be delayed.
        switch (ColumnType.tagOf(srcColumnType)) {
            case ColumnType.STRING:
                assert ColumnType.tagOf(dstColumnType) == ColumnType.VARCHAR;
                return convertStringToVarchar(rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, ff, appendPageSize);
            case ColumnType.VARCHAR:
                assert ColumnType.tagOf(dstColumnType) == ColumnType.STRING;
                return convertVarcharToString(rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, ff, appendPageSize);
            default:
                closeFds(ff, srcFixFd, srcVarFd, dstFixFd, dstVarFd);
                throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
        }
    }

    private static boolean convertVarcharToString(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize) {
        long dataSize;
        try {
            dataSize = VarcharTypeDriver.INSTANCE.getDataVectorSizeAtFromFd(ff, srcFixFd, rowCount - 1);
        } catch (CairoException ex) {
            LOG.error().$("cannot read VARCHAR column data vector size, column data is corrupt will fall back reading file sizes [srcFixFd=").$(srcFixFd).I$();
            return false;
        }

        MemoryCMORImpl srcVarMem = srcVarMemTL.get();
        MemoryCMORImpl srcFixMem = srcFixMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        StringSink sink = sinkUtf16TL.get();

        try {
            srcVarMem.ofOffset(ff, srcVarFd, null, 0, dataSize, MemoryTag.MMAP_TABLE_WRITER, CairoConfiguration.O_NONE);
            srcFixMem.ofOffset(ff, srcFixFd, null, 0, VarcharTypeDriver.INSTANCE.getAuxVectorSize(rowCount), MemoryTag.MMAP_TABLE_WRITER, CairoConfiguration.O_NONE);

            dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, MemoryTag.MMAP_TABLE_WRITER);
            dstVarMem.jumpTo(0);
            dstFixMem.of(ff, dstFixFd, null, appendPageSize, StringTypeDriver.INSTANCE.getAuxVectorSize(rowCount), MemoryTag.MMAP_TABLE_WRITER);
            dstFixMem.jumpTo(0);
            dstFixMem.putLong(0L);

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
            sink.clear();
            dstVarMem.close(true);
            dstFixMem.close(true);
            srcVarMem.close();
            srcFixMem.close();
        }
        return true;
    }
//
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
