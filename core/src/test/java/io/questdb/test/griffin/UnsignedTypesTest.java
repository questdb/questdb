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

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryMR;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

public class UnsignedTypesTest extends AbstractCairoTest {

    @Test
    public void testUnsignedComparisonAndOutput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ut_cmp(u16 uint16, u32 uint32, u64 uint64)");
            execute("insert into ut_cmp values (1, 2, 3), (-1, -1, -1)");

            assertSql(
                    "u16\tu32\tu64\n" +
                            "1\t2\t3\n" +
                            "65535\t4294967295\t18446744073709551615\n",
                    "select u16, u32, u64 from ut_cmp"
            );

            assertSql(
                    "u16\tu32\tu64\n" +
                            "1\t2\t3\n" +
                            "65535\t4294967295\t18446744073709551615\n",
                    "select u16, u32, u64 from ut_cmp order by u16, u32, u64"
            );

            assertSql("count\n1\n", "select count() from ut_cmp where u32 < 3");
            assertSql("count\n1\n", "select count() from ut_cmp where u64 < 4");
            assertSql("count\n1\n", "select count() from ut_cmp where u16 < 2");
            assertSql("count\n1\n", "select count() from ut_cmp where u32 > 3");
            assertSql("count\n1\n", "select count() from ut_cmp where u64 > 4");
            assertSql("count\n1\n", "select count() from ut_cmp where u16 > 2");
            assertSql("count\n1\n", "select count() from ut_cmp where u32 <= 2");
            assertSql("count\n1\n", "select count() from ut_cmp where u64 <= 3");
            assertSql("count\n1\n", "select count() from ut_cmp where u16 <= 1");

            assertSql("u16\n1\n", "select u16 from ut_cmp order by u16 limit 1");
            assertSql("u32\n2\n", "select u32 from ut_cmp order by u32 limit 1");
            assertSql("u64\n3\n", "select u64 from ut_cmp order by u64 limit 1");
            assertSql("u16\n65535\n", "select u16 from ut_cmp order by u16 desc limit 1");
            assertSql("u32\n4294967295\n", "select u32 from ut_cmp order by u32 desc limit 1");
            assertSql("u64\n18446744073709551615\n", "select u64 from ut_cmp order by u64 desc limit 1");
        });
    }

    @Test
    public void testUnsignedNullOrdering() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ut_null_order(u16 uint16, u32 uint32, u64 uint64)");
            execute("insert into ut_null_order values (null, null, null), (1, 1, 1), (-1, -1, -1)");

            assertSql(
                    "u16\n" +
                            "\n" +
                            "1\n" +
                            "65535\n",
                    "select u16 from ut_null_order order by u16"
            );
            assertSql(
                    "u32\n" +
                            "\n" +
                            "1\n" +
                            "4294967295\n",
                    "select u32 from ut_null_order order by u32"
            );
            assertSql(
                    "u64\n" +
                            "\n" +
                            "1\n" +
                            "18446744073709551615\n",
                    "select u64 from ut_null_order order by u64"
            );
        });
    }

    @Test
    public void testUnsignedSentinelValuesMapToNull() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ut_sentinel(u16 uint16, u32 uint32, u64 uint64)");
            execute(
                    "insert into ut_sentinel values " +
                            "((-32767 - 1), (-2147483647 - 1), (-9223372036854775807 - 1)), " +
                            "(0, 0, 0), " +
                            "(-1, -1, -1)"
            );

            assertSql(
                    "u16\tu32\tu64\n" +
                            "\t\t\n" +
                            "0\t0\t0\n" +
                            "65535\t4294967295\t18446744073709551615\n",
                    "select u16, u32, u64 from ut_sentinel order by u16, u32, u64"
            );
        });
    }

    @Test
    public void testUnsignedUnionOutput() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ut_union_a(u32 uint32, u64 uint64)");
            execute("create table ut_union_b(u32 uint32, u64 uint64)");
            execute("insert into ut_union_a values (-1, -1)");
            execute("insert into ut_union_b values (1, 1)");

            assertSql(
                    "u32\tu64\n" +
                            "1\t1\n" +
                            "4294967295\t18446744073709551615\n",
                    "select u32, u64 from (select * from ut_union_a union all select * from ut_union_b) order by u32, u64"
            );
        });
    }

    @Test
    public void testUnsignedStorageWidthsOnDisk() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table ut_disk(u16 uint16, u32 uint32, u64 uint64)");
            execute("insert into ut_disk values (1, 11, 111), (2, 22, 222), (3, 33, 333)");
            drainWalQueue();

            TableToken token = engine.verifyTableName("ut_disk");
            try (Path path = new Path()) {
                final int rootLen = path.of(configuration.getDbRoot()).concat(token).size();
                TableUtils.setPathForNativePartition(path.trimTo(rootLen), ColumnType.TIMESTAMP_MICRO, PartitionBy.NONE, 0, -1L);
                final int partitionLen = path.size();

                TableUtils.dFile(path, "u16");
                assertUInt16Packed(path);

                TableUtils.dFile(path.trimTo(partitionLen), "u32");
                assertUInt32Packed(path);

                TableUtils.dFile(path.trimTo(partitionLen), "u64");
                assertUInt64Packed(path);
            }
        });
    }

    private void assertUInt16Packed(Path path) {
        try (MemoryMR mem = Vm.getCMRInstance()) {
            final long len = configuration.getFilesFacade().length(path.$());
            Assert.assertTrue(len >= 3L * Short.BYTES);
            mem.of(configuration.getFilesFacade(), path.$(), len, len, MemoryTag.MMAP_DEFAULT);
            Assert.assertEquals(1, mem.getShort(0));
            Assert.assertEquals(2, mem.getShort(2));
            Assert.assertEquals(3, mem.getShort(4));
        }
    }

    private void assertUInt32Packed(Path path) {
        try (MemoryMR mem = Vm.getCMRInstance()) {
            final long len = configuration.getFilesFacade().length(path.$());
            Assert.assertTrue(len >= 3L * Integer.BYTES);
            mem.of(configuration.getFilesFacade(), path.$(), len, len, MemoryTag.MMAP_DEFAULT);
            Assert.assertEquals(11, mem.getInt(0));
            Assert.assertEquals(22, mem.getInt(4));
            Assert.assertEquals(33, mem.getInt(8));
        }
    }

    private void assertUInt64Packed(Path path) {
        try (MemoryMR mem = Vm.getCMRInstance()) {
            final long len = configuration.getFilesFacade().length(path.$());
            Assert.assertTrue(len >= 3L * Long.BYTES);
            mem.of(configuration.getFilesFacade(), path.$(), len, len, MemoryTag.MMAP_DEFAULT);
            Assert.assertEquals(111, mem.getLong(0));
            Assert.assertEquals(222, mem.getLong(8));
            Assert.assertEquals(333, mem.getLong(16));
        }
    }
}
