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

package io.questdb.test.recovery;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.wal.WalUtils;
import io.questdb.recovery.BoundedRegistryReader;
import io.questdb.recovery.RecoveryIssueCode;
import io.questdb.recovery.RegistryEntry;
import io.questdb.recovery.RegistryState;
import io.questdb.std.Chars;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import org.junit.Assert;
import org.junit.Test;

public class BoundedRegistryReaderTest extends AbstractCairoTest {
    private static final FilesFacade FF = TestFilesFacadeImpl.INSTANCE;

    @Test
    public void testCapsLargeEntryCount() throws Exception {
        assertMemoryLeak(() -> {
            for (int i = 0; i < 20; i++) {
                execute("create table cap_tbl_" + i + " (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            }
            drainWalQueue(engine);

            RegistryState state = new BoundedRegistryReader(FF, 5).read(configuration.getDbRoot());
            Assert.assertEquals(5, state.getEntries().size());
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.TRUNCATED_OUTPUT));
        });
    }

    @Test
    public void testReadAfterDropMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table drop_mv_base (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("create materialized view drop_mv as (select sym, last(price) as price, ts from drop_mv_base sample by 1h) partition by DAY");
            drainWalQueue(engine);
            execute("drop materialized view drop_mv");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            RegistryEntry entry = findEntryByTableName(state, "drop_mv");
            Assert.assertNull("dropped matview should not be in registry", entry);
        });
    }

    @Test
    public void testReadAfterDropView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table drop_v_base (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("create view drop_v as (select sym, last(price) as price, ts from drop_v_base sample by 1h)");

            RegistryState stateBefore = readRegistryState();
            Assert.assertNotNull("view should be in registry before drop", findEntryByTableName(stateBefore, "drop_v"));

            execute("drop view drop_v");
            drainWalQueue(engine);

            RegistryState stateAfter = readRegistryState();
            Assert.assertNull("dropped view should not be in registry", findEntryByTableName(stateAfter, "drop_v"));
        });
    }

    @Test
    public void testReadAfterDropTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table drop_tbl (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("drop table drop_tbl");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            // dropped table should not appear in final entries
            for (int i = 0, n = state.getEntries().size(); i < n; i++) {
                Assert.assertNotEquals("drop_tbl", state.getEntries().getQuick(i).getTableName());
            }
            Assert.assertEquals(0, countIssuesWithCode(state, RecoveryIssueCode.CORRUPT_REGISTRY));
        });
    }

    @Test
    public void testReadAllTableTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table all_nonwal (val long, ts timestamp) timestamp(ts) partition by DAY");
            execute("create table all_wal (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("create view all_view as (select sym, last(price) as price, ts from all_wal sample by 1h)");
            execute("create materialized view all_mv as (select sym, last(price) as price, ts from all_wal sample by 1h) partition by DAY");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            // non-WAL tables are NOT in tables.d
            Assert.assertNull("non-WAL table should not be in registry", findEntryByTableName(state, "all_nonwal"));
            // WAL, view, matview should all be present
            RegistryEntry wal = findEntryByTableName(state, "all_wal");
            Assert.assertNotNull("WAL table should be in registry", wal);
            Assert.assertEquals(TableUtils.TABLE_TYPE_WAL, wal.getTableType());
            RegistryEntry view = findEntryByTableName(state, "all_view");
            Assert.assertNotNull("view should be in registry", view);
            Assert.assertEquals(TableUtils.TABLE_TYPE_VIEW, view.getTableType());
            RegistryEntry mv = findEntryByTableName(state, "all_mv");
            Assert.assertNotNull("matview should be in registry", mv);
            Assert.assertEquals(TableUtils.TABLE_TYPE_MAT, mv.getTableType());
        });
    }

    @Test
    public void testReadAfterRenameTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table rename_src (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("rename table rename_src to rename_dst");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            // after rename: entry should have new tableName, but same dirName
            RegistryEntry entry = findEntryByTableName(state, "rename_dst");
            Assert.assertNotNull("renamed table should be in registry", entry);
            Assert.assertFalse(entry.isRemoved());
        });
    }

    @Test
    public void testReadCorruptAppendOffset() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table corrupt_offset (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            // set append offset to value larger than file size
            String registryPath = findRegistryPath();
            long fd = FF.openRW(Path.getThreadLocal(registryPath).$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                long fileSize = FF.length(fd);
                long scratch = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putLong(scratch, fileSize + 1000);
                    FF.write(fd, scratch, Long.BYTES, 0);
                } finally {
                    Unsafe.free(scratch, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                FF.close(fd);
            }

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_OFFSET));
            // should still read available data
            Assert.assertTrue(state.getEntries().size() > 0);
        });
    }

    @Test
    public void testReadCorruptEntryOperation() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table corrupt_op (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            // write invalid operation code at offset 8 (right after header)
            String registryPath = findRegistryPath();
            long fd = FF.openRW(Path.getThreadLocal(registryPath).$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                long scratch = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(scratch, 42);
                    FF.write(fd, scratch, Integer.BYTES, Long.BYTES);
                } finally {
                    Unsafe.free(scratch, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                FF.close(fd);
            }

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.CORRUPT_REGISTRY));
        });
    }

    @Test
    public void testReadCorruptStringLength() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table corrupt_str (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            // write negative char count after the operation code
            String registryPath = findRegistryPath();
            long fd = FF.openRW(Path.getThreadLocal(registryPath).$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                long scratch = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    // offset 8 = operation (keep valid = 0)
                    // offset 12 = tableName char count -> write -1
                    Unsafe.getUnsafe().putInt(scratch, -1);
                    FF.write(fd, scratch, Integer.BYTES, Long.BYTES + Integer.BYTES);
                } finally {
                    Unsafe.free(scratch, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                FF.close(fd);
            }

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
        });
    }

    @Test
    public void testReadEmptyDatabase() throws Exception {
        assertMemoryLeak(() -> {
            RegistryState state = readRegistryState();
            // empty db still has telemetry & telemetry_config tables
            // but we just check no CORRUPT issues
            Assert.assertEquals(0, countIssuesWithCode(state, RecoveryIssueCode.CORRUPT_REGISTRY));
        });
    }

    @Test
    public void testReadMissingRegistryFile() throws Exception {
        assertMemoryLeak(() -> {
            // delete all tables.d.* files
            deleteAllRegistryFiles();

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.MISSING_FILE));
        });
    }

    @Test
    public void testReadMultipleTables() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table multi_a (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create table multi_b (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            execute("create table multi_c (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            Assert.assertNotNull(findEntryByTableName(state, "multi_a"));
            Assert.assertNotNull(findEntryByTableName(state, "multi_b"));
            Assert.assertNotNull(findEntryByTableName(state, "multi_c"));
        });
    }

    @Test
    public void testReadSingleNonWalTable() throws Exception {
        assertMemoryLeak(() -> {
            // non-WAL tables are never written to tables.d by design
            // (TableNameRegistryRW only calls logAddTable for WAL tables)
            execute("create table non_wal_tbl (val long, ts timestamp) timestamp(ts) partition by DAY");

            RegistryState state = readRegistryState();
            RegistryEntry entry = findEntryByTableName(state, "non_wal_tbl");
            Assert.assertNull("non-wal table should not be in registry", entry);
        });
    }

    @Test
    public void testReadSingleMatView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table mv_base (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("create materialized view mv_single as (select sym, last(price) as price, ts from mv_base sample by 1h) partition by DAY");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            RegistryEntry entry = findEntryByTableName(state, "mv_single");
            Assert.assertNotNull("matview should be in registry", entry);
            Assert.assertEquals(TableUtils.TABLE_TYPE_MAT, entry.getTableType());
            Assert.assertFalse(entry.isRemoved());
        });
    }

    @Test
    public void testReadSingleView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table view_base (sym symbol, price double, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("create view view_single as (select sym, last(price) as price, ts from view_base sample by 1h)");

            RegistryState state = readRegistryState();
            RegistryEntry entry = findEntryByTableName(state, "view_single");
            Assert.assertNotNull("view should be in registry", entry);
            Assert.assertEquals(TableUtils.TABLE_TYPE_VIEW, entry.getTableType());
            Assert.assertFalse(entry.isRemoved());
        });
    }

    @Test
    public void testReadSingleWalTable() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table wal_tbl (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            RegistryEntry entry = findEntryByTableName(state, "wal_tbl");
            Assert.assertNotNull("wal table should be in registry", entry);
            Assert.assertEquals(TableUtils.TABLE_TYPE_WAL, entry.getTableType());
            Assert.assertFalse(entry.isRemoved());
        });
    }

    @Test
    public void testReadTableTypes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table type_nonwal (val long, ts timestamp) timestamp(ts) partition by DAY");
            execute("create table type_wal (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            // non-WAL tables are never written to tables.d
            RegistryEntry nonWal = findEntryByTableName(state, "type_nonwal");
            Assert.assertNull("non-wal table should not be in registry", nonWal);
            // WAL table should be present
            RegistryEntry wal = findEntryByTableName(state, "type_wal");
            Assert.assertNotNull("wal table should be in registry", wal);
            Assert.assertEquals(TableUtils.TABLE_TYPE_WAL, wal.getTableType());
        });
    }

    @Test
    public void testReadTruncatedEntry() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trunc_entry (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            // truncate file mid-entry (after header but before first entry completes)
            String registryPath = findRegistryPath();
            long fd = FF.openRW(Path.getThreadLocal(registryPath).$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                // keep header (8 bytes) + operation int (4 bytes) + partial tableName length
                Assert.assertTrue(FF.truncate(fd, Long.BYTES + Integer.BYTES + 2));
            } finally {
                FF.close(fd);
            }

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.PARTIAL_READ));
        });
    }

    @Test
    public void testReadTruncatedHeader() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trunc_hdr (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            // truncate file to 4 bytes (header needs 8)
            String registryPath = findRegistryPath();
            long fd = FF.openRW(Path.getThreadLocal(registryPath).$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                Assert.assertTrue(FF.truncate(fd, 4));
            } finally {
                FF.close(fd);
            }

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
        });
    }

    @Test
    public void testReadTruncatedString() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table trunc_str (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            // set a large char count for the tableName but truncate file so data isn't there
            String registryPath = findRegistryPath();
            long fd = FF.openRW(Path.getThreadLocal(registryPath).$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                long scratch = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    // write charCount=100 at offset 12 (tableName length)
                    Unsafe.getUnsafe().putInt(scratch, 100);
                    FF.write(fd, scratch, Integer.BYTES, Long.BYTES + Integer.BYTES);
                } finally {
                    Unsafe.free(scratch, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
                // truncate to just past the char count field
                Assert.assertTrue(FF.truncate(fd, Long.BYTES + Integer.BYTES + Integer.BYTES + 10));
            } finally {
                FF.close(fd);
            }

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.PARTIAL_READ));
        });
    }

    @Test
    public void testReadAddRemoveReAdd() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table readd_tbl (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);
            execute("drop table readd_tbl");
            drainWalQueue(engine);
            execute("create table readd_tbl (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            RegistryState state = readRegistryState();
            RegistryEntry entry = findEntryByTableName(state, "readd_tbl");
            Assert.assertNotNull("re-added table should be in registry", entry);
            Assert.assertFalse("re-added table should not be marked as removed", entry.isRemoved());
        });
    }

    @Test
    public void testReadCorruptStringLengthExceedsMax() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table corrupt_maxstr (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            // set table name char count to 2000 (exceeds reader max of 1024)
            String registryPath = findRegistryPath();
            long fd = FF.openRW(Path.getThreadLocal(registryPath).$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                long scratch = Unsafe.malloc(Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    // offset 12 = table name char count (after 8-byte header + 4-byte operation)
                    Unsafe.getUnsafe().putInt(scratch, 2000);
                    FF.write(fd, scratch, Integer.BYTES, Long.BYTES + Integer.BYTES);
                } finally {
                    Unsafe.free(scratch, Integer.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            } finally {
                FF.close(fd);
            }

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.INVALID_COUNT));
        });
    }

    @Test
    public void testReadZeroLengthFile() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table zero_len (val long, ts timestamp) timestamp(ts) partition by DAY WAL");
            drainWalQueue(engine);

            String registryPath = findRegistryPath();
            long fd = FF.openRW(Path.getThreadLocal(registryPath).$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            try {
                Assert.assertTrue(FF.truncate(fd, 0));
            } finally {
                FF.close(fd);
            }

            RegistryState state = readRegistryState();
            Assert.assertTrue(hasIssue(state, RecoveryIssueCode.SHORT_FILE));
        });
    }

    private static int countIssuesWithCode(RegistryState state, RecoveryIssueCode code) {
        int count = 0;
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).getCode() == code) {
                count++;
            }
        }
        return count;
    }

    private static void deleteAllRegistryFiles() {
        try (Path path = new Path()) {
            long findPtr = FF.findFirst(path.of(configuration.getDbRoot()).$());
            if (findPtr < 1) {
                return;
            }
            try {
                StringSink nameSink = new StringSink();
                do {
                    nameSink.clear();
                    Utf8s.utf8ToUtf16Z(FF.findName(findPtr), nameSink);
                    if (Chars.startsWith(nameSink, WalUtils.TABLE_REGISTRY_NAME_FILE + ".")) {
                        try (Path filePath = new Path()) {
                            FF.removeQuiet(filePath.of(configuration.getDbRoot()).concat(nameSink).$());
                        }
                    }
                } while (FF.findNext(findPtr) > 0);
            } finally {
                FF.findClose(findPtr);
            }
        }
    }

    private static RegistryEntry findEntryByTableName(RegistryState state, String tableName) {
        ObjList<RegistryEntry> entries = state.getEntries();
        for (int i = 0, n = entries.size(); i < n; i++) {
            RegistryEntry entry = entries.getQuick(i);
            if (tableName.equals(entry.getTableName())) {
                return entry;
            }
        }
        return null;
    }

    private static String findRegistryPath() {
        try (Path path = new Path()) {
            long findPtr = FF.findFirst(path.of(configuration.getDbRoot()).$());
            Assert.assertTrue(findPtr > 0);
            try {
                int lastVersion = -1;
                StringSink nameSink = new StringSink();
                do {
                    nameSink.clear();
                    Utf8s.utf8ToUtf16Z(FF.findName(findPtr), nameSink);
                    if (Chars.startsWith(nameSink, WalUtils.TABLE_REGISTRY_NAME_FILE + ".")) {
                        try {
                            int v = io.questdb.std.Numbers.parseInt(
                                    nameSink,
                                    WalUtils.TABLE_REGISTRY_NAME_FILE.length() + 1,
                                    nameSink.length()
                            );
                            if (v > lastVersion) {
                                lastVersion = v;
                            }
                        } catch (io.questdb.std.NumericException ignore) {
                        }
                    }
                } while (FF.findNext(findPtr) > 0);
                Assert.assertTrue("no registry file found", lastVersion >= 0);
                return configuration.getDbRoot() + java.io.File.separator
                        + WalUtils.TABLE_REGISTRY_NAME_FILE + "." + lastVersion;
            } finally {
                FF.findClose(findPtr);
            }
        }
    }

    private static boolean hasIssue(RegistryState state, RecoveryIssueCode code) {
        for (int i = 0, n = state.getIssues().size(); i < n; i++) {
            if (state.getIssues().getQuick(i).getCode() == code) {
                return true;
            }
        }
        return false;
    }

    private static RegistryState readRegistryState() {
        return new BoundedRegistryReader(FF).read(configuration.getDbRoot());
    }
}
