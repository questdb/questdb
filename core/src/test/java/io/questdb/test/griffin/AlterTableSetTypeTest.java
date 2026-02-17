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

package io.questdb.test.griffin;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.griffin.SqlException;
import io.questdb.std.Files;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class AlterTableSetTypeTest extends AbstractCairoTest {
    static final byte NON_WAL = (byte) 0;
    static final byte WAL = (byte) 1;

    @Test
    public void testConvertNonPartitionedToWal() throws Exception {
        final String tableName = "table_non_partitioned";
        assertMemoryLeak(() -> {
            createNonPartitionedTable();
            assertException(
                    "alter table " + tableName + " set type wal",
                    12,
                    "Cannot convert non-partitioned table"
            );
        });
    }

    @Test
    public void testConvertRandom() throws Exception {
        final String tableName = "table_alternating";
        assertMemoryLeak(() -> {
            createTable(tableName, "BYPASS WAL");
            for (int i = 0; i < 30; i++) {
                final boolean walEnabled = sqlExecutionContext.getRandom().nextBoolean();
                execute("alter table " + tableName + " set type " + (walEnabled ? "wal" : "bypass wal"));
                final Path convertFilePath1 = assertConvertFileExists(tableName);
                assertConvertFileContent(convertFilePath1, walEnabled ? WAL : NON_WAL);
            }
        });
    }

    @Test
    public void testConvertToNonWal() throws Exception {
        final String tableName = "table_wal";
        assertMemoryLeak(() -> {
            createTable(tableName, "WAL");
            execute("alter table " + tableName + " set type bypass wal");
            drainWalQueue();
            final Path convertFilePath = assertConvertFileExists(tableName);
            assertConvertFileContent(convertFilePath, NON_WAL);
        });
    }

    @Test
    public void testConvertToWal() throws Exception {
        final String tableName = "table_non_wal";
        assertMemoryLeak(() -> {
            createTable(tableName, "BYPASS WAL");
            execute("alter table " + tableName + " set type wal");
            final Path convertFilePath = assertConvertFileExists(tableName);
            assertConvertFileContent(convertFilePath, WAL);
        });
    }

    private void assertConvertFileContent(Path convertFilePath, byte expected) throws IOException {
        final byte[] fileContent = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(convertFilePath.toString()));
        assertEquals(1, fileContent.length);
        assertEquals(expected, fileContent[0]);
    }

    private Path assertConvertFileExists(String tableName) {
        final TableToken token = engine.verifyTableName(tableName);
        final Path path = Path.PATH.get().of(configuration.getDbRoot()).concat(token).concat(WalUtils.CONVERT_FILE_NAME);
        Assert.assertTrue(Utf8s.toString(path), Files.exists(path.$()));
        return path;
    }

    private void createNonPartitionedTable() throws SqlException {
        execute("create table table_non_partitioned (ts TIMESTAMP, x long)");
    }

    private void createTable(String tableName, String walMode) throws SqlException {
        execute("create table " + tableName + " (ts TIMESTAMP, x long) timestamp(ts) PARTITION BY DAY " + walMode);
    }
}
