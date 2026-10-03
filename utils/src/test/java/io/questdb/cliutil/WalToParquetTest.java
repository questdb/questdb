/*+*****************************************************************************
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

package io.questdb.cliutil;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WalToParquetTest {

    @Test
    public void testArgsMissingDbRoot() {
        WalToParquet.Args args = WalToParquet.Args.parse(new String[]{"--output-dir", "/tmp/out"});
        assertNull("missing --db-root must return null", args);
    }

    @Test
    public void testArgsParseFullForm() {
        WalToParquet.Args args = WalToParquet.Args.parse(new String[]{
                "--db-root", "/data",
                "--output-dir", "/out",
                "--include-system",
                "--include-empty",
                "--no-shoulder"
        });
        assertEquals("/data", args.dbRoot);
        assertEquals("/out", args.outputDir);
        assertTrue(args.includeSystem);
        assertTrue(args.includeEmpty);
        assertTrue(args.noShoulder);
        assertNull(args.tableDir);
    }

    @Test
    public void testArgsParseSingleTable() {
        WalToParquet.Args args = WalToParquet.Args.parse(new String[]{
                "--db-root", "/data",
                "--table-dir", "demo~11",
                "--table-name", "demo",
                "--table-id", "11"
        });
        assertEquals("demo~11", args.tableDir);
        assertEquals("demo", args.tableName);
        assertEquals(Integer.valueOf(11), args.tableId);
        assertFalse(args.includeSystem);
        assertFalse(args.includeEmpty);
        assertFalse(args.noShoulder);
    }

    @Test
    public void testFileNameTier1() {
        String name = WalToParquet.buildParquetFileNameForTest("trades", 4, 0, 14, 16, false);
        assertEquals("trades__wal4__seg0__seqTxn14-16.parquet", name);
    }

    @Test
    public void testFileNameTier1Tier3() {
        String name = WalToParquet.buildParquetFileNameForTest("trades", 4, 0, 14, 16, true);
        assertEquals("trades__wal4__seg0__seqTxn14-16__tier3.parquet", name);
    }

    @Test
    public void testFileNameTier2() {
        // firstSeqTxn < 0 signals tier-2 (txnlog unreadable, no seqTxn known).
        String name = WalToParquet.buildParquetFileNameForTest("trades", 4, 0, -1, -1, false);
        assertEquals("trades__wal4__seg0__tier2.parquet", name);
    }

    @Test
    public void testFileNameTier2Tier3() {
        String name = WalToParquet.buildParquetFileNameForTest("trades", 4, 0, -1, -1, true);
        assertEquals("trades__wal4__seg0__tier2__tier3.parquet", name);
    }

    @Test
    public void testTableInfoNoTilde() {
        // Non-WAL system tables don't follow the <name>~<id> convention.
        WalToParquet.TableInfo info = WalToParquet.tableInfoFromDirNameForTest("sys.column_versions_purge_log");
        assertEquals("sys.column_versions_purge_log", info.dirName);
        assertEquals("sys.column_versions_purge_log", info.tableName);
        assertEquals(0, info.tableId);
    }

    @Test
    public void testTableInfoTildeNonNumeric() {
        // ~ present but suffix is not a number: treat as one whole name.
        WalToParquet.TableInfo info = WalToParquet.tableInfoFromDirNameForTest("weird~name~thing");
        assertEquals("weird~name~thing", info.dirName);
        assertEquals("weird~name~thing", info.tableName);
        assertEquals(0, info.tableId);
    }

    @Test
    public void testTableInfoWithTilde() {
        WalToParquet.TableInfo info = WalToParquet.tableInfoFromDirNameForTest("demo_trades_today~11");
        assertEquals("demo_trades_today~11", info.dirName);
        assertEquals("demo_trades_today", info.tableName);
        assertEquals(11, info.tableId);
    }
}
