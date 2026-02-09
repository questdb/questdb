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

package io.questdb.recovery;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableUtils;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;

import java.nio.charset.StandardCharsets;

public class ColumnValueReader {
    static final int MAX_DISPLAY_BYTES = 8192;
    private final FilesFacade ff;

    public ColumnValueReader(FilesFacade ff) {
        this.ff = ff;
    }

    public String readValue(
            CharSequence tableDir,
            String partitionDirName,
            String columnName,
            long columnNameTxn,
            int columnType,
            long rowNo,
            long effectiveRows
    ) {
        if (rowNo < 0 || rowNo >= effectiveRows) {
            return "ERROR: row out of range [0, " + effectiveRows + ")";
        }

        short tag = ColumnType.tagOf(columnType);
        if (ColumnType.isVarSize(columnType)) {
            return switch (tag) {
                case ColumnType.VARCHAR -> readVarchar(tableDir, partitionDirName, columnName, columnNameTxn, rowNo);
                case ColumnType.STRING -> readString(tableDir, partitionDirName, columnName, columnNameTxn, rowNo);
                case ColumnType.BINARY -> readBinary(tableDir, partitionDirName, columnName, columnNameTxn, rowNo);
                default -> readArray(tableDir, partitionDirName, columnName, columnNameTxn, rowNo);
            };
        }
        return readFixedSize(tableDir, partitionDirName, columnName, columnNameTxn, columnType, tag, rowNo);
    }

    private String readArray(CharSequence tableDir, String partitionDirName, String columnName, long columnNameTxn, long rowNo) {
        try (Path path = new Path()) {
            path.of(tableDir).slash().concat(partitionDirName).slash();
            int pathLen = path.size();

            // read 16-byte aux entry
            path.trimTo(pathLen);
            TableUtils.iFile(path, columnName, columnNameTxn);
            long auxFd = ff.openRO(path.$());
            if (auxFd < 0) {
                return "ERROR: aux file missing";
            }

            long scratch = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                long bytesRead = ff.read(auxFd, scratch, 16, rowNo * 16);
                if (bytesRead < 16) {
                    return "ERROR: read failed at aux offset " + (rowNo * 16);
                }

                long rawOffset = Unsafe.getUnsafe().getLong(scratch);
                long offset = rawOffset & ((1L << 48) - 1);
                int size = Unsafe.getUnsafe().getInt(scratch + Long.BYTES);

                if (size == 0) {
                    return "null";
                }
                if (size < 0) {
                    return "ERROR: invalid array size " + size;
                }

                // read from .d file
                path.trimTo(pathLen);
                TableUtils.dFile(path, columnName, columnNameTxn);
                long dataFd = ff.openRO(path.$());
                if (dataFd < 0) {
                    return "ERROR: data file missing";
                }

                try {
                    int displaySize = Math.min(size, 128);
                    long dataBuf = Unsafe.malloc(displaySize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long dataRead = ff.read(dataFd, dataBuf, displaySize, offset);
                        if (dataRead < displaySize) {
                            return "ERROR: read failed at data offset " + offset;
                        }
                        StringBuilder sb = new StringBuilder();
                        hexDump(dataBuf, displaySize, sb);
                        if (size > displaySize) {
                            sb.append("... (").append(size).append(" bytes total)");
                        }
                        return sb.toString();
                    } finally {
                        Unsafe.free(dataBuf, displaySize, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(dataFd);
                }
            } finally {
                Unsafe.free(scratch, 16, MemoryTag.NATIVE_DEFAULT);
                ff.close(auxFd);
            }
        }
    }

    private String readBinary(CharSequence tableDir, String partitionDirName, String columnName, long columnNameTxn, long rowNo) {
        try (Path path = new Path()) {
            path.of(tableDir).slash().concat(partitionDirName).slash();
            int pathLen = path.size();

            // read offsets from .i file (N+1 model, 8 bytes per entry)
            path.trimTo(pathLen);
            TableUtils.iFile(path, columnName, columnNameTxn);
            long auxFd = ff.openRO(path.$());
            if (auxFd < 0) {
                return "ERROR: aux file missing";
            }

            long scratch = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                // read offset[rowNo] and offset[rowNo+1]
                long bytesRead = ff.read(auxFd, scratch, 16, rowNo * 8);
                if (bytesRead < 16) {
                    return "ERROR: read failed at aux offset " + (rowNo * 8);
                }

                long dataOffset = Unsafe.getUnsafe().getLong(scratch);
                // read from .d file: 8-byte long length prefix
                path.trimTo(pathLen);
                TableUtils.dFile(path, columnName, columnNameTxn);
                long dataFd = ff.openRO(path.$());
                if (dataFd < 0) {
                    return "ERROR: data file missing";
                }

                try {
                    // read length prefix (8 bytes)
                    long lenBuf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long lenRead = ff.read(dataFd, lenBuf, 8, dataOffset);
                        if (lenRead < 8) {
                            return "ERROR: read failed at data offset " + dataOffset;
                        }
                        long blobLen = Unsafe.getUnsafe().getLong(lenBuf);
                        if (blobLen == -1) {
                            return "null";
                        }
                        if (blobLen < 0) {
                            return "ERROR: invalid binary length " + blobLen;
                        }

                        int displaySize = (int) Math.min(blobLen, 64);
                        long dataBuf = Unsafe.malloc(displaySize, MemoryTag.NATIVE_DEFAULT);
                        try {
                            long dataRead = ff.read(dataFd, dataBuf, displaySize, dataOffset + 8);
                            if (dataRead < displaySize) {
                                return "ERROR: read failed at data offset " + (dataOffset + 8);
                            }
                            StringBuilder sb = new StringBuilder();
                            hexDump(dataBuf, displaySize, sb);
                            if (blobLen > 64) {
                                sb.append("... (").append(blobLen).append(" bytes total)");
                            }
                            return sb.toString();
                        } finally {
                            Unsafe.free(dataBuf, displaySize, MemoryTag.NATIVE_DEFAULT);
                        }
                    } finally {
                        Unsafe.free(lenBuf, 8, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(dataFd);
                }
            } finally {
                Unsafe.free(scratch, 16, MemoryTag.NATIVE_DEFAULT);
                ff.close(auxFd);
            }
        }
    }

    private String readFixedSize(
            CharSequence tableDir,
            String partitionDirName,
            String columnName,
            long columnNameTxn,
            int columnType,
            short tag,
            long rowNo
    ) {
        int size = ColumnType.sizeOf(columnType);
        if (size <= 0) {
            return "ERROR: unsupported fixed-size type (size=" + size + ")";
        }

        try (Path path = new Path()) {
            path.of(tableDir).slash().concat(partitionDirName).slash();
            TableUtils.dFile(path, columnName, columnNameTxn);
            long fd = ff.openRO(path.$());
            if (fd < 0) {
                return "ERROR: data file missing";
            }

            long scratch = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                long offset = rowNo * size;
                long bytesRead = ff.read(fd, scratch, size, offset);
                if (bytesRead < size) {
                    return "ERROR: read failed at offset " + offset;
                }
                return formatFixedValue(scratch, tag, size);
            } finally {
                Unsafe.free(scratch, size, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
        }
    }

    private String readString(CharSequence tableDir, String partitionDirName, String columnName, long columnNameTxn, long rowNo) {
        try (Path path = new Path()) {
            path.of(tableDir).slash().concat(partitionDirName).slash();
            int pathLen = path.size();

            // read offsets from .i file (N+1 model, 8 bytes per entry)
            path.trimTo(pathLen);
            TableUtils.iFile(path, columnName, columnNameTxn);
            long auxFd = ff.openRO(path.$());
            if (auxFd < 0) {
                return "ERROR: aux file missing";
            }

            long scratch = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                // read offset[rowNo] and offset[rowNo+1]
                long bytesRead = ff.read(auxFd, scratch, 16, rowNo * 8);
                if (bytesRead < 16) {
                    return "ERROR: read failed at aux offset " + (rowNo * 8);
                }

                long startOffset = Unsafe.getUnsafe().getLong(scratch);
                // read from .d file: 4-byte int length prefix, then UTF-16 chars
                path.trimTo(pathLen);
                TableUtils.dFile(path, columnName, columnNameTxn);
                long dataFd = ff.openRO(path.$());
                if (dataFd < 0) {
                    return "ERROR: data file missing";
                }

                try {
                    // read length prefix (4 bytes)
                    long lenBuf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long lenRead = ff.read(dataFd, lenBuf, 4, startOffset);
                        if (lenRead < 4) {
                            return "ERROR: read failed at data offset " + startOffset;
                        }
                        int strLen = Unsafe.getUnsafe().getInt(lenBuf);
                        if (strLen == -1) {
                            return "null";
                        }
                        if (strLen < 0) {
                            return "ERROR: invalid string length " + strLen;
                        }

                        // string data is UTF-16, strLen chars = strLen * 2 bytes
                        int displayChars = Math.min(strLen, MAX_DISPLAY_BYTES / 2);
                        long dataBytes = (long) displayChars * 2;
                        long dataBuf = Unsafe.malloc(dataBytes, MemoryTag.NATIVE_DEFAULT);
                        try {
                            long dataRead = ff.read(dataFd, dataBuf, dataBytes, startOffset + 4);
                            if (dataRead < dataBytes) {
                                return "ERROR: read failed at data offset " + (startOffset + 4);
                            }
                            char[] chars = new char[displayChars];
                            for (int i = 0; i < displayChars; i++) {
                                chars[i] = Unsafe.getUnsafe().getChar(dataBuf + (long) i * 2);
                            }
                            String result = new String(chars);
                            if (strLen > displayChars) {
                                return result + "... (" + strLen + " chars total)";
                            }
                            return result;
                        } finally {
                            Unsafe.free(dataBuf, dataBytes, MemoryTag.NATIVE_DEFAULT);
                        }
                    } finally {
                        Unsafe.free(lenBuf, 4, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(dataFd);
                }
            } finally {
                Unsafe.free(scratch, 16, MemoryTag.NATIVE_DEFAULT);
                ff.close(auxFd);
            }
        }
    }

    private String readVarchar(CharSequence tableDir, String partitionDirName, String columnName, long columnNameTxn, long rowNo) {
        try (Path path = new Path()) {
            path.of(tableDir).slash().concat(partitionDirName).slash();
            int pathLen = path.size();

            // read 16-byte aux entry from .i file
            path.trimTo(pathLen);
            TableUtils.iFile(path, columnName, columnNameTxn);
            long auxFd = ff.openRO(path.$());
            if (auxFd < 0) {
                return "ERROR: aux file missing";
            }

            long scratch = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                long bytesRead = ff.read(auxFd, scratch, 16, rowNo * 16);
                if (bytesRead < 16) {
                    return "ERROR: read failed at aux offset " + (rowNo * 16);
                }

                int header = Unsafe.getUnsafe().getInt(scratch);

                // NULL: bit 2 set
                if ((header & 4) != 0) {
                    return "null";
                }

                // INLINED: bit 0 set
                if ((header & 1) != 0) {
                    int length = (header >>> 4) & 0x0F;
                    if (length == 0) {
                        return "";
                    }
                    byte[] bytes = new byte[length];
                    for (int i = 0; i < length; i++) {
                        bytes[i] = Unsafe.getUnsafe().getByte(scratch + 1 + i);
                    }
                    try {
                        return new String(bytes, StandardCharsets.UTF_8);
                    } catch (Exception e) {
                        StringBuilder sb = new StringBuilder();
                        hexDump(scratch + 1, length, sb);
                        return sb.toString();
                    }
                }

                // Non-inlined
                int size = (header >>> 4) & 0x0FFFFFFF;
                long dataOffsetRaw = Unsafe.getUnsafe().getLong(scratch + Long.BYTES);
                long dataOffset = dataOffsetRaw >>> 16;

                // read from .d file
                path.trimTo(pathLen);
                TableUtils.dFile(path, columnName, columnNameTxn);
                long dataFd = ff.openRO(path.$());
                if (dataFd < 0) {
                    return "ERROR: data file missing";
                }

                try {
                    int displaySize = Math.min(size, MAX_DISPLAY_BYTES);
                    long dataBuf = Unsafe.malloc(displaySize, MemoryTag.NATIVE_DEFAULT);
                    try {
                        long dataRead = ff.read(dataFd, dataBuf, displaySize, dataOffset);
                        if (dataRead < displaySize) {
                            return "ERROR: read failed at data offset " + dataOffset;
                        }
                        byte[] bytes = new byte[displaySize];
                        for (int i = 0; i < displaySize; i++) {
                            bytes[i] = Unsafe.getUnsafe().getByte(dataBuf + i);
                        }
                        try {
                            String result = new String(bytes, StandardCharsets.UTF_8);
                            if (size > displaySize) {
                                return result + "... (" + size + " bytes total)";
                            }
                            return result;
                        } catch (Exception e) {
                            StringBuilder sb = new StringBuilder();
                            hexDump(dataBuf, displaySize, sb);
                            if (size > displaySize) {
                                sb.append("... (").append(size).append(" bytes total)");
                            }
                            return sb.toString();
                        }
                    } finally {
                        Unsafe.free(dataBuf, displaySize, MemoryTag.NATIVE_DEFAULT);
                    }
                } finally {
                    ff.close(dataFd);
                }
            } finally {
                Unsafe.free(scratch, 16, MemoryTag.NATIVE_DEFAULT);
                ff.close(auxFd);
            }
        }
    }

    private static String formatFixedValue(long addr, short tag, int size) {
        return switch (tag) {
            case ColumnType.BOOLEAN -> {
                byte v = Unsafe.getUnsafe().getByte(addr);
                yield v == -1 ? "null" : v != 0 ? "true" : "false";
            }
            case ColumnType.BYTE -> Byte.toString(Unsafe.getUnsafe().getByte(addr));
            case ColumnType.SHORT -> Short.toString(Unsafe.getUnsafe().getShort(addr));
            case ColumnType.CHAR -> {
                char c = Unsafe.getUnsafe().getChar(addr);
                yield c == 0 ? "null" : Character.toString(c);
            }
            case ColumnType.INT -> {
                int v = Unsafe.getUnsafe().getInt(addr);
                yield v == Integer.MIN_VALUE ? "null" : Integer.toString(v);
            }
            case ColumnType.LONG -> {
                long v = Unsafe.getUnsafe().getLong(addr);
                yield v == Long.MIN_VALUE ? "null" : Long.toString(v);
            }
            case ColumnType.FLOAT -> {
                float v = Unsafe.getUnsafe().getFloat(addr);
                yield Float.isNaN(v) ? "null" : Float.toString(v);
            }
            case ColumnType.DOUBLE -> {
                double v = Unsafe.getUnsafe().getDouble(addr);
                yield Double.isNaN(v) ? "null" : Double.toString(v);
            }
            case ColumnType.DATE -> {
                long v = Unsafe.getUnsafe().getLong(addr);
                if (v == Long.MIN_VALUE) {
                    yield "null";
                }
                StringSink sink = new StringSink();
                MicrosFormatUtils.appendDateTime(sink, v * 1000);
                yield sink.toString();
            }
            case ColumnType.TIMESTAMP -> {
                long v = Unsafe.getUnsafe().getLong(addr);
                if (v == Long.MIN_VALUE) {
                    yield "null";
                }
                StringSink sink = new StringSink();
                MicrosFormatUtils.appendDateTime(sink, v);
                yield sink.toString();
            }
            case ColumnType.SYMBOL -> {
                int v = Unsafe.getUnsafe().getInt(addr);
                yield v == Integer.MIN_VALUE ? "null" : "symbol#" + v;
            }
            case ColumnType.UUID -> {
                long lo = Unsafe.getUnsafe().getLong(addr);
                long hi = Unsafe.getUnsafe().getLong(addr + 8);
                if (lo == Long.MIN_VALUE && hi == Long.MIN_VALUE) {
                    yield "null";
                }
                yield formatUuid(lo, hi);
            }
            case ColumnType.IPv4 -> {
                int v = Unsafe.getUnsafe().getInt(addr);
                if (v == Numbers.IPv4_NULL) {
                    yield "null";
                }
                StringSink sink = new StringSink();
                Numbers.intToIPv4Sink(sink, v);
                yield sink.toString();
            }
            case ColumnType.LONG256 -> {
                long l0 = Unsafe.getUnsafe().getLong(addr);
                long l1 = Unsafe.getUnsafe().getLong(addr + 8);
                long l2 = Unsafe.getUnsafe().getLong(addr + 16);
                long l3 = Unsafe.getUnsafe().getLong(addr + 24);
                if (l0 == Long.MIN_VALUE && l1 == Long.MIN_VALUE && l2 == Long.MIN_VALUE && l3 == Long.MIN_VALUE) {
                    yield "null";
                }
                yield "0x" + Long.toHexString(l3) + Long.toHexString(l2) + Long.toHexString(l1) + Long.toHexString(l0);
            }
            case ColumnType.LONG128 -> {
                long lo = Unsafe.getUnsafe().getLong(addr);
                long hi = Unsafe.getUnsafe().getLong(addr + 8);
                if (lo == Long.MIN_VALUE && hi == Long.MIN_VALUE) {
                    yield "null";
                }
                yield "0x" + Long.toHexString(hi) + Long.toHexString(lo);
            }
            case ColumnType.GEOBYTE -> Byte.toString(Unsafe.getUnsafe().getByte(addr));
            case ColumnType.GEOSHORT -> Short.toString(Unsafe.getUnsafe().getShort(addr));
            case ColumnType.GEOINT -> Integer.toString(Unsafe.getUnsafe().getInt(addr));
            case ColumnType.GEOLONG -> Long.toString(Unsafe.getUnsafe().getLong(addr));
            default -> {
                // unknown fixed-size: hex dump
                StringBuilder sb = new StringBuilder();
                hexDump(addr, size, sb);
                yield sb.toString();
            }
        };
    }

    private static String formatUuid(long lo, long hi) {
        // UUID is stored as two longs: lo (bytes 0-7) and hi (bytes 8-15)
        // Standard UUID format: 8-4-4-4-12 hex digits
        return String.format(
                "%08x-%04x-%04x-%04x-%012x",
                (hi >>> 32) & 0xFFFFFFFFL,
                (hi >>> 16) & 0xFFFFL,
                hi & 0xFFFFL,
                (lo >>> 48) & 0xFFFFL,
                lo & 0xFFFFFFFFFFFFL
        );
    }

    private static void hexDump(long addr, int length, StringBuilder sb) {
        for (int i = 0; i < length; i++) {
            if (i > 0 && i % 16 == 0) {
                sb.append('\n');
            } else if (i > 0) {
                sb.append(' ');
            }
            int b = Unsafe.getUnsafe().getByte(addr + i) & 0xFF;
            sb.append(Character.forDigit(b >> 4, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
    }
}
