/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.*;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;

public class MetadataMigration404 {
    public static final int BOOLEAN = 0;
    public static final int BYTE = 1;
    public static final int SHORT = 2;
    public static final int CHAR = 3;
    public static final int INT = 4;
    public static final int LONG = 5;
    public static final int DATE = 6;
    public static final int TIMESTAMP = 7;
    public static final int FLOAT = 8;
    public static final int DOUBLE = 9;
    public static final int STRING = 10;
    public static final int SYMBOL = 11;
    public static final int LONG256 = 12;
    public static final int BINARY = 13;
    public static final int PARAMETER = 14;

    private static final IntList typeMapping = new IntList();
    private static final Log LOG = LogFactory.getLog(MetadataMigration404.class);

    public static void convert(FilesFacade ff, Path path1, Path path2, MemoryMA appendMem, MemoryMR roMem) {
        final int plen = path1.length();
        path1.concat(TableUtils.META_FILE_NAME).$();

        if (!ff.exists(path1)) {
            return;
        }

        roMem.smallFile(ff, path1, MemoryTag.MMAP_DEFAULT);

        if (roMem.getInt(12) == ColumnType.VERSION) {
            LOG.error().$("already up to date ").$(path1).$();
            return;
        }

        path1.trimTo(plen);
        path1.concat("_meta.1").$();

        appendMem.smallFile(ff, path1, MemoryTag.MMAP_DEFAULT);
        appendMem.jumpTo(0);

        // column count
        final int columnCount = roMem.getInt(0);
        appendMem.putInt(columnCount);
        appendMem.putInt(roMem.getInt(4));
        appendMem.putInt(roMem.getInt(8));
        appendMem.putInt(ColumnType.VERSION);
        appendMem.jumpTo(TableUtils.META_OFFSET_COLUMN_TYPES);

        long offset = TableUtils.META_OFFSET_COLUMN_TYPES;

        for (int i = 0; i < columnCount; i++) {
            appendMem.putByte((byte) typeMapping.getQuick(roMem.getByte(offset)));
            long flags = 0;
            if (roMem.getBool(offset + 1)) {
                flags |= TableUtils.META_FLAG_BIT_INDEXED;
            }
            appendMem.putLong(flags);
            appendMem.putInt(roMem.getInt(offset + 2));
            appendMem.skip(TableUtils.META_COLUMN_DATA_RESERVED); // reserved
            offset += TableUtils.META_COLUMN_DATA_SIZE;
        }

        for (int i = 0; i < columnCount; i++) {
            CharSequence s = roMem.getStr(offset);
            appendMem.putStr(s);
            offset += s.length() * 2L + 4;
        }
        roMem.close();
        appendMem.close();

        // rename

        path1.trimTo(plen).concat(TableUtils.META_FILE_NAME).$();
        path2.trimTo(plen).concat("_meta.old").$();

        if (!ff.rename(path1, path2)) {
            LOG.error().$("could not rename ").$(path1).$(" -> ").$(path2).$();
            return;
        }

        path1.trimTo(plen).concat("_meta.1").$();
        path2.trimTo(plen).concat(TableUtils.META_FILE_NAME).$();

        if (!ff.rename(path1, path2)) {
            LOG.error().$("could not rename ").$(path1).$(" -> ").$(path2).$();
            return;
        }

        LOG.info().$("updated ").$(path2).$();
    }

    public static void main(String[] args) {
        try (
                final MemoryMR roMem = Vm.getMRInstance() ;
                final MemoryMA appendMem = Vm.getMARInstance();
                final Path path1 = new Path();
                final Path path2 = new Path()
        ) {
            if (args.length < 1) {
                LOG.info().$("usage: ").$(MetadataMigration404.class.getName()).$(" <db_path>").$();
                return;
            }
            FilesFacade ff = FilesFacadeImpl.INSTANCE;
            NativeLPSZ nativeLPSZ = new NativeLPSZ();
            path1.of(args[0]).$();

            ff.iterateDir(path1, (name, type) -> {
                if (type == Files.DT_DIR) {
                    nativeLPSZ.of(name);
                    if (!Chars.equals(nativeLPSZ, '.') && !Chars.equals(nativeLPSZ, "..")) {
                        int plen = path1.length();
                        path1.chop$().concat(nativeLPSZ);
                        path2.of(path1);
                        convert(ff, path1, path2, appendMem, roMem);
                        path1.trimTo(plen);
                    }
                }
            });

            convert(
                    FilesFacadeImpl.INSTANCE,
                    path1,
                    path2,
                    appendMem,
                    roMem
            );
        }
    }

    static {
        typeMapping.extendAndSet(BOOLEAN, ColumnType.BOOLEAN);
        typeMapping.extendAndSet(BYTE, ColumnType.BYTE);
        typeMapping.extendAndSet(SHORT, ColumnType.SHORT);
        typeMapping.extendAndSet(INT, ColumnType.INT);
        typeMapping.extendAndSet(LONG, ColumnType.LONG);
        typeMapping.extendAndSet(FLOAT, ColumnType.FLOAT);
        typeMapping.extendAndSet(DOUBLE, ColumnType.DOUBLE);
        typeMapping.extendAndSet(STRING, ColumnType.STRING);
        typeMapping.extendAndSet(SYMBOL, ColumnType.SYMBOL);
        typeMapping.extendAndSet(BINARY, ColumnType.BINARY);
        typeMapping.extendAndSet(DATE, ColumnType.DATE);
        typeMapping.extendAndSet(PARAMETER, ColumnType.PARAMETER);
        typeMapping.extendAndSet(TIMESTAMP, ColumnType.TIMESTAMP);
        typeMapping.extendAndSet(CHAR, ColumnType.CHAR);
        typeMapping.extendAndSet(LONG256, ColumnType.LONG256);
    }
}
