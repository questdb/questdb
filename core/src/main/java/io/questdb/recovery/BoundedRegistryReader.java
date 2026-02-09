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

import io.questdb.cairo.GrowOnlyTableNameRegistryStore;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;

import java.util.HashMap;

public class BoundedRegistryReader extends AbstractBoundedReader {
    public static final int DEFAULT_MAX_ENTRIES = 100_000;
    private static final int MAX_STRING_CHARS = 1024;
    private static final long RESERVED_LONGS_SIZE = GrowOnlyTableNameRegistryStore.TABLE_NAME_ENTRY_RESERVED_LONGS * Long.BYTES;
    private final int maxEntries;

    public BoundedRegistryReader(FilesFacade ff) {
        this(ff, DEFAULT_MAX_ENTRIES);
    }

    public BoundedRegistryReader(FilesFacade ff, int maxEntries) {
        super(ff);
        this.maxEntries = Math.max(1, maxEntries);
    }

    public RegistryState read(CharSequence dbRoot) {
        final RegistryState state = new RegistryState();

        int version = findLastVersion(dbRoot, state);
        if (version < 0) {
            return state;
        }
        state.setVersion(version);

        try (Path path = new Path()) {
            path.of(dbRoot).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).put('.').put(version).$();
            final String registryPath = path.toString();

            final long fd = ff.openRO(path.$());
            if (fd < 0) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.IO_ERROR,
                        "cannot open registry file [path=" + registryPath + ", errno=" + ff.errno() + ']'
                );
                return state;
            }

            try {
                final long fileSize = ff.length(fd);
                state.setFileSize(fileSize);

                if (fileSize < Long.BYTES) {
                    state.addIssue(
                            RecoveryIssueSeverity.ERROR,
                            RecoveryIssueCode.SHORT_FILE,
                            "registry file is shorter than header [path=" + registryPath + ", size=" + fileSize + ']'
                    );
                    return state;
                }

                // scratch buffer: big enough for max string (4 + 1024*2 = 2052 bytes)
                final int scratchSize = Integer.BYTES + MAX_STRING_CHARS * Character.BYTES;
                long scratch = Unsafe.malloc(scratchSize, MemoryTag.NATIVE_DEFAULT);
                try {
                    readEntries(fd, fileSize, scratch, state, registryPath);
                } finally {
                    Unsafe.free(scratch, scratchSize, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                ff.close(fd);
            }
        }

        return state;
    }

    private int findLastVersion(CharSequence dbRoot, RegistryState state) {
        try (Path path = new Path()) {
            long findPtr = ff.findFirst(path.of(dbRoot).$());
            if (findPtr < 1) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.MISSING_FILE,
                        "cannot scan db root directory [path=" + dbRoot + ']'
                );
                return -1;
            }

            try {
                int lastVersion = -1;
                StringSink nameSink = new StringSink();
                do {
                    if (ff.findType(findPtr) == Files.DT_FILE) {
                        nameSink.clear();
                        Utf8s.utf8ToUtf16Z(ff.findName(findPtr), nameSink);
                        if (Chars.startsWith(nameSink, WalUtils.TABLE_REGISTRY_NAME_FILE)
                                && nameSink.length() > WalUtils.TABLE_REGISTRY_NAME_FILE.length() + 1) {
                            try {
                                int v = Numbers.parseInt(
                                        nameSink,
                                        WalUtils.TABLE_REGISTRY_NAME_FILE.length() + 1,
                                        nameSink.length()
                                );
                                if (v > lastVersion) {
                                    lastVersion = v;
                                }
                            } catch (NumericException ignore) {
                            }
                        }
                    }
                } while (ff.findNext(findPtr) > 0);

                if (lastVersion < 0) {
                    state.addIssue(
                            RecoveryIssueSeverity.ERROR,
                            RecoveryIssueCode.MISSING_FILE,
                            "no tables.d file found [dbRoot=" + dbRoot + ']'
                    );
                }
                return lastVersion;
            } finally {
                ff.findClose(findPtr);
            }
        }
    }

    private void readEntries(long fd, long fileSize, long scratch, RegistryState state, String registryPath) {
        int issuesBefore = state.getIssues().size();

        // read append offset (8 bytes at offset 0)
        long bytesRead = ff.read(fd, scratch, Long.BYTES, 0);
        if (bytesRead != Long.BYTES) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.SHORT_FILE,
                    "cannot read append offset [path=" + registryPath + ']'
            );
            return;
        }
        long appendOffset = Unsafe.getUnsafe().getLong(scratch);
        state.setAppendOffset(appendOffset);

        if (appendOffset < Long.BYTES) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_OFFSET,
                    "append offset is less than header size [value=" + appendOffset + ']'
            );
            return;
        }

        if (appendOffset > fileSize) {
            state.addIssue(
                    RecoveryIssueSeverity.WARN,
                    RecoveryIssueCode.INVALID_OFFSET,
                    "append offset exceeds file size [appendOffset=" + appendOffset + ", fileSize=" + fileSize + ']'
            );
            appendOffset = fileSize;
        }

        // replay entries
        HashMap<String, RegistryEntry> entryMap = new HashMap<>();
        long offset = Long.BYTES;
        int entryCount = 0;

        while (offset < appendOffset) {
            issuesBefore = state.getIssues().size();

            // read operation int
            if (!isRangeReadable(offset, Integer.BYTES, fileSize)) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.PARTIAL_READ,
                        "entry operation outside file [entryIndex=" + entryCount + ", offset=" + offset + ']'
                );
                break;
            }

            bytesRead = ff.read(fd, scratch, Integer.BYTES, offset);
            if (bytesRead != Integer.BYTES) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.PARTIAL_READ,
                        "cannot read entry operation [entryIndex=" + entryCount + ", offset=" + offset + ']'
                );
                break;
            }
            int operation = Unsafe.getUnsafe().getInt(scratch);
            offset += Integer.BYTES;

            if (operation != GrowOnlyTableNameRegistryStore.OPERATION_ADD
                    && operation != GrowOnlyTableNameRegistryStore.OPERATION_REMOVE) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.CORRUPT_REGISTRY,
                        "invalid operation code [entryIndex=" + entryCount + ", operation=" + operation + ']'
                );
                break;
            }

            // read tableName
            long[] result = new long[1];
            String tableName = readString(fd, fileSize, scratch, state, offset, result, "tableName", entryCount);
            if (state.getIssues().size() > issuesBefore) {
                break;
            }
            offset = result[0];

            // read dirName
            String dirName = readString(fd, fileSize, scratch, state, offset, result, "dirName", entryCount);
            if (state.getIssues().size() > issuesBefore) {
                break;
            }
            offset = result[0];

            // read tableId
            if (!isRangeReadable(offset, Integer.BYTES, fileSize)) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.PARTIAL_READ,
                        "tableId outside file [entryIndex=" + entryCount + ", offset=" + offset + ']'
                );
                break;
            }
            bytesRead = ff.read(fd, scratch, Integer.BYTES, offset);
            if (bytesRead != Integer.BYTES) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.PARTIAL_READ,
                        "cannot read tableId [entryIndex=" + entryCount + ']'
                );
                break;
            }
            int tableId = Unsafe.getUnsafe().getInt(scratch);
            offset += Integer.BYTES;

            // read tableType
            if (!isRangeReadable(offset, Integer.BYTES, fileSize)) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.PARTIAL_READ,
                        "tableType outside file [entryIndex=" + entryCount + ", offset=" + offset + ']'
                );
                break;
            }
            bytesRead = ff.read(fd, scratch, Integer.BYTES, offset);
            if (bytesRead != Integer.BYTES) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.PARTIAL_READ,
                        "cannot read tableType [entryIndex=" + entryCount + ']'
                );
                break;
            }
            int tableType = Unsafe.getUnsafe().getInt(scratch);
            offset += Integer.BYTES;

            // skip reserved longs for ADD operations
            if (operation == GrowOnlyTableNameRegistryStore.OPERATION_ADD) {
                if (!isRangeReadable(offset, RESERVED_LONGS_SIZE, fileSize)) {
                    state.addIssue(
                            RecoveryIssueSeverity.ERROR,
                            RecoveryIssueCode.PARTIAL_READ,
                            "reserved area outside file [entryIndex=" + entryCount + ", offset=" + offset + ']'
                    );
                    break;
                }
                offset += RESERVED_LONGS_SIZE;
            }

            boolean removed = operation == GrowOnlyTableNameRegistryStore.OPERATION_REMOVE;
            entryMap.put(dirName, new RegistryEntry(tableName, dirName, tableId, tableType, removed));
            entryCount++;

            if (entryCount >= maxEntries) {
                state.addIssue(
                        RecoveryIssueSeverity.WARN,
                        RecoveryIssueCode.TRUNCATED_OUTPUT,
                        "entry list capped [max=" + maxEntries + ']'
                );
                break;
            }
        }

        // copy final non-removed entries into state
        for (RegistryEntry entry : entryMap.values()) {
            if (!entry.isRemoved()) {
                state.getEntries().add(entry);
            }
        }
    }

    private String readString(
            long fd,
            long fileSize,
            long scratch,
            RegistryState state,
            long offset,
            long[] newOffset,
            String fieldName,
            int entryIndex
    ) {
        // read 4-byte char count
        if (!isRangeReadable(offset, Integer.BYTES, fileSize)) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.PARTIAL_READ,
                    fieldName + " length outside file [entryIndex=" + entryIndex + ", offset=" + offset + ']'
            );
            return null;
        }

        long bytesRead = ff.read(fd, scratch, Integer.BYTES, offset);
        if (bytesRead != Integer.BYTES) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.PARTIAL_READ,
                    "cannot read " + fieldName + " length [entryIndex=" + entryIndex + ']'
            );
            return null;
        }

        int charCount = Unsafe.getUnsafe().getInt(scratch);
        if (charCount < 0 || charCount > MAX_STRING_CHARS) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.INVALID_COUNT,
                    fieldName + " char count invalid [entryIndex=" + entryIndex + ", charCount=" + charCount + ']'
            );
            return null;
        }

        long dataOffset = offset + Integer.BYTES;
        long dataSize = (long) charCount * Character.BYTES;

        if (!isRangeReadable(dataOffset, dataSize, fileSize)) {
            state.addIssue(
                    RecoveryIssueSeverity.ERROR,
                    RecoveryIssueCode.PARTIAL_READ,
                    fieldName + " data outside file [entryIndex=" + entryIndex + ", offset=" + dataOffset + ']'
            );
            return null;
        }

        String value;
        if (charCount == 0) {
            value = "";
        } else {
            bytesRead = ff.read(fd, scratch, dataSize, dataOffset);
            if (bytesRead != dataSize) {
                state.addIssue(
                        RecoveryIssueSeverity.ERROR,
                        RecoveryIssueCode.PARTIAL_READ,
                        "cannot read " + fieldName + " data [entryIndex=" + entryIndex + ']'
                );
                return null;
            }

            char[] chars = new char[charCount];
            for (int c = 0; c < charCount; c++) {
                chars[c] = Unsafe.getUnsafe().getChar(scratch + (long) c * Character.BYTES);
            }
            value = new String(chars);
        }

        newOffset[0] = dataOffset + dataSize;
        return value;
    }
}
