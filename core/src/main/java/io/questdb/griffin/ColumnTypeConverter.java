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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.VarcharTypeDriver;
import io.questdb.cairo.vm.MemoryCMORImpl;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryCMARW;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ThreadLocal;
import io.questdb.std.str.Utf8StringSink;

public class ColumnTypeConverter {
    private static final ThreadLocal<MemoryCMARW> dstFixMemTL = new ThreadLocal<>(io.questdb.cairo.vm.MemoryCMARWImpl::new);
    private static final ThreadLocal<MemoryCMARW> dstVarMemTL = new ThreadLocal<>(io.questdb.cairo.vm.MemoryCMARWImpl::new);
    private static final ThreadLocal<Utf8StringSink> sinkTL = new ThreadLocal<>(Utf8StringSink::new);
    private static final ThreadLocal<MemoryCMORImpl> srcFixMemTL = new ThreadLocal<>(MemoryCMORImpl::new);
    private static final ThreadLocal<MemoryCMORImpl> srcVarMemTL = new ThreadLocal<>(MemoryCMORImpl::new);

    public static void convertColumn(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, int srcColumnType, int dstColumnType, FilesFacade ff, long appendPageSize) {
        if (ColumnType.isFixedSize(srcColumnType) && ColumnType.isFixedSize(dstColumnType)) {
            convertFixedToFixed(rowCount, srcFixFd, dstFixFd, srcColumnType, dstColumnType);
        } else if (ColumnType.isVarSize(srcColumnType) && ColumnType.isVarSize(dstColumnType)) {
            convertVarToVar(rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, srcColumnType, dstColumnType, ff, appendPageSize);
        } else if (ColumnType.isFixedSize(srcColumnType) && ColumnType.isVarSize(dstColumnType)) {
            convertFixedToVar(rowCount, srcFixFd, dstFixFd, dstVarFd, srcColumnType, dstColumnType);
        } else {
            convertVarToFixed(rowCount, srcFixFd, srcVarFd, dstFixFd, srcColumnType, dstColumnType);
        }
    }

    private static void convertFixedToFixed(long rowCount, int srcFixFd, int dstFixFd, int srcColumnType, int dstColumnType) {
        throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
    }

    private static void convertFixedToVar(long rowCount, int srcFixFd, int dstFixFd, int dstVarFd, int srcColumnType, int dstColumnType) {
        throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
    }

    private static void convertStringToVarchar(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, FilesFacade ff, long appendPageSize) {
        MemoryCMORImpl srcVarMem = srcVarMemTL.get();
        MemoryCMARW dstVarMem = dstVarMemTL.get();
        MemoryCMARW dstFixMem = dstFixMemTL.get();
        var sink = sinkTL.get();

        try {
            srcVarMem.ofOffset(ff, srcVarFd, null, 0, ff.length(srcVarFd), MemoryTag.MMAP_TABLE_WRITER, CairoConfiguration.O_NONE);
            // Will not use fixed memory, enough data in var mem
            ff.close(srcFixFd);

            dstVarMem.of(ff, dstVarFd, null, appendPageSize, appendPageSize, MemoryTag.MMAP_TABLE_WRITER);
            dstVarMem.jumpTo(0);
            dstFixMem.of(ff, dstFixFd, null, appendPageSize, rowCount * VarcharTypeDriver.VARCHAR_AUX_WIDTH_BYTES, MemoryTag.MMAP_TABLE_WRITER);
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

    private static void convertVarToFixed(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int srcColumnType, int dstColumnType) {
        throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
    }

    private static void convertVarToVar(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, int srcColumnType, int dstColumnType, FilesFacade ff, long appendPageSize) {
        // Convert STRING to VARCHAR and back
        // Binary column usage is rare, conversions can be delayed.
        switch (ColumnType.tagOf(srcColumnType)) {
            case ColumnType.STRING:
                assert ColumnType.tagOf(dstColumnType) == ColumnType.VARCHAR;
                convertStringToVarchar(rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, ff, appendPageSize);
                break;
            case ColumnType.VARCHAR:
                assert ColumnType.tagOf(dstColumnType) == ColumnType.STRING;
                convertVarcharToString(rowCount, srcFixFd, srcVarFd, dstFixFd, dstVarFd, ff);
                break;
            default:
                throw CairoException.critical(0).put("Unsupported conversion from ").put(ColumnType.nameOf(srcColumnType)).put(" to ").put(ColumnType.nameOf(dstColumnType));
        }
    }

    private static void convertVarcharToString(long rowCount, int srcFixFd, int srcVarFd, int dstFixFd, int dstVarFd, FilesFacade ff) {
        ff.close(srcFixFd);
        ff.close(srcVarFd);
        ff.close(dstFixFd);
        ff.close(dstVarFd);
    }

}
