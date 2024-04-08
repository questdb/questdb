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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.wal.SymbolMapDiff;
import io.questdb.cairo.wal.WalColFirstWriter;
import io.questdb.griffin.engine.ops.AlterOperationBuilder;
import io.questdb.std.Files;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;

import static org.junit.Assert.*;

class WalWriterTestUtils {

    private WalWriterTestUtils() {
    }

    public static void addColumn(WalColFirstWriter writer, String columnName, int columnType) {
        writer.addColumn(columnName, columnType);
    }

    public static void assertEmptySymbolDiff(SymbolMapDiff symbolMapDiff, int columnIndex) {
        assertEquals(columnIndex, symbolMapDiff.getColumnIndex());
        assertEquals(0, symbolMapDiff.getSize());
        assertNotNull(symbolMapDiff);
        assertNull(symbolMapDiff.nextEntry());
    }

    public static void assertWalFileExist(Path path, TableToken tableName, String walName, int segment, String fileName) {
        final int pathLen = path.size();
        try {
            path = constructPath(path, tableName, walName, segment, fileName);
            if (!Files.exists(path)) {
                throw new AssertionError("Path " + path + " does not exists!");
            }
        } finally {
            path.trimTo(pathLen);
        }
    }

    public static void assertWalFileExist(Path path, TableToken tableName, String walName, String fileName) {
        assertWalFileExist(path, tableName, walName, -1, fileName);
    }

    public static Path constructPath(Path path, TableToken tableName, CharSequence walName, long segment, CharSequence fileName) {
        return segment < 0
                ? path.concat(tableName).slash().concat(walName).slash().concat(fileName).$()
                : path.concat(tableName).slash().concat(walName).slash().put(segment).slash().concat(fileName).$();
    }

    public static void prepareBinPayload(long pointer, int limit) {
        for (int offset = 0; offset < limit; offset++) {
            Unsafe.getUnsafe().putByte(pointer + offset, (byte) limit);
        }
    }

    public static void removeColumn(TableWriterAPI writer, String columnName) {
        AlterOperationBuilder removeColumnOperation = new AlterOperationBuilder().ofDropColumn(0, writer.getTableToken(), 0);
        removeColumnOperation.ofDropColumn(columnName);
        writer.apply(removeColumnOperation.build(), true);
    }

    public static void renameColumn(TableWriterAPI writer, String oldName, String newName) {
        AlterOperationBuilder renameColumnC = new AlterOperationBuilder().ofRenameColumn(0, writer.getTableToken(), 0);
        renameColumnC.ofRenameColumn(oldName, newName);
        writer.apply(renameColumnC.build(), true);
    }

    public interface RowInserter {
        long getCount();

        void insertRow();
    }
}
