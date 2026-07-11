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

package io.questdb.test.cairo.parquet;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PartitionFormat;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.table.ParquetRowGroupFilter;
import io.questdb.griffin.engine.table.parquet.ParquetFileDecoder;
import io.questdb.griffin.engine.table.parquet.ParquetPartitionDecoder;
import io.questdb.griffin.engine.table.parquet.RowGroupBuffers;
import io.questdb.std.DirectIntList;
import io.questdb.std.FilesFacade;
import io.questdb.std.MemoryTag;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration tests for O3 writes into Parquet partitions, covering the three
 * rewrite triggers:
 * <ul>
 *   <li>Single row group &mdash; always rewritten to avoid dead space</li>
 *   <li>Unused-bytes ratio exceeds threshold</li>
 *   <li>Absolute unused bytes exceeds threshold</li>
 * </ul>
 */
public class ParquetWriteTest extends AbstractCairoTest {

    @Test
    public void testAlterColumnTypeAllFixedToStringWithParquetPartition() throws Exception {
        testConvertFixedToVar("STRING");
    }

    @Test
    public void testAlterColumnTypeAllFixedToVarcharWithParquetPartition() throws Exception {
        testConvertFixedToVar("VARCHAR");
    }

    @Test
    public void testAlterColumnTypeAllToByteWithParquetPartition() throws Exception {
        testConvertAllToType("BYTE",
                """
                        rnd_short() v_short,
                        rnd_int(0, 1_000_000, 4) v_int,
                        rnd_long(0, 1_000_000_000L, 4) v_long,
                        rnd_float(4) v_float,
                        rnd_double(4) v_double""",
                new String[]{"v_short", "v_int", "v_long", "v_float", "v_double"},
                "42");
    }

    @Test
    public void testAlterColumnTypeAllToDateWithParquetPartition() throws Exception {
        testConvertAllToType("DATE",
                """
                        rnd_long(0, 1_000_000_000_000L, 4) v_long,
                        rnd_timestamp(
                            to_timestamp('2000', 'yyyy'),
                            to_timestamp('2025', 'yyyy'), 4
                        ) v_ts""",
                new String[]{"v_long", "v_ts"},
                "'2020-07-01T00:00:00.000Z'");
    }

    @Test
    public void testAlterColumnTypeAllToDoubleWithParquetPartition() throws Exception {
        testConvertAllToType("DOUBLE",
                """
                        rnd_byte() v_byte,
                        rnd_short() v_short,
                        rnd_int(0, 1_000_000, 4) v_int,
                        rnd_long(0, 1_000_000_000L, 4) v_long,
                        rnd_float(4) v_float""",
                new String[]{"v_byte", "v_short", "v_int", "v_long", "v_float"},
                "3.14");
    }

    @Test
    public void testAlterColumnTypeAllToFloatWithParquetPartition() throws Exception {
        testConvertAllToType("FLOAT",
                """
                        rnd_byte() v_byte,
                        rnd_short() v_short,
                        rnd_int(0, 1_000_000, 4) v_int,
                        rnd_long(0, 1_000_000_000L, 4) v_long,
                        rnd_double(4) v_double""",
                new String[]{"v_byte", "v_short", "v_int", "v_long", "v_double"},
                "1.5");
    }

    @Test
    public void testAlterColumnTypeAllToIntWithParquetPartition() throws Exception {
        testConvertAllToType("INT",
                """
                        rnd_boolean() v_bool,
                        rnd_byte() v_byte,
                        rnd_short() v_short,
                        rnd_long(0, 1_000_000_000L, 4) v_long,
                        rnd_float(4) v_float,
                        rnd_double(4) v_double""",
                new String[]{"v_bool", "v_byte", "v_short", "v_long", "v_float", "v_double"},
                "12345");
    }

    @Test
    public void testAlterColumnTypeAllToLongWithParquetPartition() throws Exception {
        testConvertAllToType("LONG",
                """
                        rnd_byte() v_byte,
                        rnd_short() v_short,
                        rnd_int(0, 1_000_000, 4) v_int,
                        rnd_float(4) v_float,
                        rnd_double(4) v_double,
                        rnd_date(
                            to_date('2000', 'yyyy'),
                            to_date('2025', 'yyyy'), 4
                        ) v_date,
                        rnd_timestamp(
                            to_timestamp('2000', 'yyyy'),
                            to_timestamp('2025', 'yyyy'), 4
                        ) v_ts""",
                new String[]{"v_byte", "v_short", "v_int", "v_float", "v_double", "v_date", "v_ts"},
                "123456789");
    }

    @Test
    public void testAlterColumnTypeAllToShortWithParquetPartition() throws Exception {
        testConvertAllToType("SHORT",
                """
                        rnd_byte() v_byte,
                        rnd_int(0, 1_000_000, 4) v_int,
                        rnd_long(0, 1_000_000_000L, 4) v_long,
                        rnd_float(4) v_float,
                        rnd_double(4) v_double""",
                new String[]{"v_byte", "v_int", "v_long", "v_float", "v_double"},
                "999");
    }

    @Test
    public void testAlterColumnTypeAllToTimestampWithParquetPartition() throws Exception {
        testConvertAllToType("TIMESTAMP",
                """
                        rnd_long(0, 1_000_000_000_000L, 4) v_long,
                        rnd_date(
                            to_date('2000', 'yyyy'),
                            to_date('2025', 'yyyy'), 4
                        ) v_date""",
                new String[]{"v_long", "v_date"},
                "'2020-07-01T00:00:00.000000Z'");
    }

    @Test
    public void testAlterColumnTypeBooleanToIntWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v BOOLEAN, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (true, '2020-01-01T00:00:00.000Z'),
                            (false, '2020-01-01T06:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (true, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE INT");
            drainWalQueue();

            // Boolean has no distinct null (sentinel = 0 = false),
            // so NULL boolean expands to INT 0, not INT null.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1\t2020-01-01T00:00:00.000000Z
                            0\t2020-01-01T06:00:00.000000Z
                            0\t2020-01-01T12:00:00.000000Z
                            1\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeByteToDoubleWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v BYTE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (127, '2020-01-01T06:00:00.000Z'),
                            (-1, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (42, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1.0\t2020-01-01T00:00:00.000000Z
                            127.0\t2020-01-01T06:00:00.000000Z
                            -1.0\t2020-01-01T12:00:00.000000Z
                            42.0\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeChainedFixedToVarWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (10, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (30, '2020-01-01T16:00:00.000Z'),
                            (40, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (50, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Three chained conversions: INT → LONG → DOUBLE → VARCHAR.
            // The parquet file stores data under the original INT writer index.
            // The read path and O3 merge must walk the full replacingIndex chain.
            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN v TYPE VARCHAR");
            drainWalQueue();

            // The second ALTER TYPE eagerly converts the parquet partition to native so
            // it goes through the same chain as the native one:
            // INT → LONG → DOUBLE → VARCHAR, producing "10.0", "30.0", "40.0".
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T08:00:00.000000Z\t10.0
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t30.0
                            2020-01-01T20:00:00.000000Z\t40.0
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t50.0
                            """);

            // O3 merge into the (now native) partition.
            execute("INSERT INTO x(v, ts) VALUES ('99', '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T06:00:00.000000Z\t99
                            2020-01-01T08:00:00.000000Z\t10.0
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t30.0
                            2020-01-01T20:00:00.000000Z\t40.0
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t50.0
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeChainedWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (10, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (30, '2020-01-01T16:00:00.000Z'),
                            (40, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (50, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Two consecutive type changes: INT → LONG → DOUBLE.
            // The parquet file stores data under the original INT writer index.
            // The O3 merge must walk the full replacingIndex chain to find it.
            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();
            execute("ALTER TABLE x ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T08:00:00.000000Z\t10.0
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t30.0
                            2020-01-01T20:00:00.000000Z\t40.0
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t50.0
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES (99, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T06:00:00.000000Z\t99.0
                            2020-01-01T08:00:00.000000Z\t10.0
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t30.0
                            2020-01-01T20:00:00.000000Z\t40.0
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t50.0
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeDateToLongWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (CAST('1970-01-01T00:00:01.000Z' AS DATE), '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (CAST('1970-01-01T00:16:40.000Z' AS DATE), '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (CAST('1970-01-01T00:00:00.100Z' AS DATE), '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();

            // DATE and LONG share the same i64 representation (milliseconds)
            // and null sentinel (Long.MIN_VALUE). Conversion is a no-op.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1000\t2020-01-01T00:00:00.000000Z
                            null\t2020-01-01T06:00:00.000000Z
                            1000000\t2020-01-01T12:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeDateToTimestampWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DATE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (CAST('1970-01-01T00:00:01.000Z' AS DATE), '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (CAST('1970-01-01T00:16:40.000Z' AS DATE), '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (CAST('1970-01-01T00:00:00.500Z' AS DATE), '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE TIMESTAMP");
            drainWalQueue();

            // DATE (ms) → TIMESTAMP (µs): values are scaled ×1000.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:01.000000Z\t2020-01-01T00:00:00.000000Z
                            \t2020-01-01T06:00:00.000000Z
                            1970-01-01T00:16:40.000000Z\t2020-01-01T12:00:00.000000Z
                            1970-01-01T00:00:00.500000Z\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeDoubleToByteWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (10.5, '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (-3.9, '2020-01-01T12:00:00.000Z'),
                            (1e10, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (7.1, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE BYTE");
            drainWalQueue();

            // Out-of-range 1e10 should become BYTE null (0).
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            10\t2020-01-01T00:00:00.000000Z
                            0\t2020-01-01T06:00:00.000000Z
                            -3\t2020-01-01T12:00:00.000000Z
                            0\t2020-01-01T18:00:00.000000Z
                            7\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeDoubleToDateWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1.0, '2020-01-01T00:00:00.000Z'),
                            (-1.0, '2020-01-01T04:00:00.000Z'),
                            (NULL, '2020-01-01T08:00:00.000Z'),
                            (0.0, '2020-01-01T12:00:00.000Z'),
                            (86400000.0, '2020-01-01T16:00:00.000Z')
                            """
            );
            execute(
                    """
                            INSERT INTO x VALUES
                            (1.0, '2020-01-02T00:00:00.000Z'),
                            (-1.0, '2020-01-02T04:00:00.000Z'),
                            (NULL, '2020-01-02T08:00:00.000Z'),
                            (0.0, '2020-01-02T12:00:00.000Z'),
                            (86400000.0, '2020-01-02T16:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DATE");
            drainWalQueue();

            // DOUBLE→DATE is a range-checked f64→i64 cast. NaN→null.
            // Both partitions must produce identical results.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:00.001Z\t2020-01-01T00:00:00.000000Z
                            1969-12-31T23:59:59.999Z\t2020-01-01T04:00:00.000000Z
                            \t2020-01-01T08:00:00.000000Z
                            1970-01-01T00:00:00.000Z\t2020-01-01T12:00:00.000000Z
                            1970-01-02T00:00:00.000Z\t2020-01-01T16:00:00.000000Z
                            1970-01-01T00:00:00.001Z\t2020-01-02T00:00:00.000000Z
                            1969-12-31T23:59:59.999Z\t2020-01-02T04:00:00.000000Z
                            \t2020-01-02T08:00:00.000000Z
                            1970-01-01T00:00:00.000Z\t2020-01-02T12:00:00.000000Z
                            1970-01-02T00:00:00.000Z\t2020-01-02T16:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeDoubleToIntOutOfRangeWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (42.5, '2020-01-01T00:00:00.000Z'),
                            (1e15, '2020-01-01T06:00:00.000Z'),
                            (-1e15, '2020-01-01T12:00:00.000Z'),
                            (NULL, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (3.7, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE INT");
            drainWalQueue();

            // C++ converts out-of-range doubles to INT null. The Rust parquet
            // decoder must do the same: values outside [INT_MIN+1, INT_MAX]
            // should become null, and fractional parts are truncated toward zero.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            42\t2020-01-01T00:00:00.000000Z
                            null\t2020-01-01T06:00:00.000000Z
                            null\t2020-01-01T12:00:00.000000Z
                            null\t2020-01-01T18:00:00.000000Z
                            3\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeDoubleToShortWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (10.5, '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (-3.9, '2020-01-01T12:00:00.000Z'),
                            (1e10, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (7.1, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE SHORT");
            drainWalQueue();

            // Out-of-range 1e10 should become SHORT null (0).
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            10\t2020-01-01T00:00:00.000000Z
                            0\t2020-01-01T06:00:00.000000Z
                            -3\t2020-01-01T12:00:00.000000Z
                            0\t2020-01-01T18:00:00.000000Z
                            7\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeDoubleToTimestampWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1.0, '2020-01-01T00:00:00.000Z'),
                            (-1.0, '2020-01-01T04:00:00.000Z'),
                            (NULL, '2020-01-01T08:00:00.000Z'),
                            (0.0, '2020-01-01T12:00:00.000Z'),
                            (1000000.0, '2020-01-01T16:00:00.000Z')
                            """
            );
            execute(
                    """
                            INSERT INTO x VALUES
                            (1.0, '2020-01-02T00:00:00.000Z'),
                            (-1.0, '2020-01-02T04:00:00.000Z'),
                            (NULL, '2020-01-02T08:00:00.000Z'),
                            (0.0, '2020-01-02T12:00:00.000Z'),
                            (1000000.0, '2020-01-02T16:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE TIMESTAMP");
            drainWalQueue();

            // DOUBLE→TIMESTAMP is a range-checked f64→i64 cast. NaN��null.
            // Both partitions must produce identical results.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:00.000001Z\t2020-01-01T00:00:00.000000Z
                            1969-12-31T23:59:59.999999Z\t2020-01-01T04:00:00.000000Z
                            \t2020-01-01T08:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2020-01-01T12:00:00.000000Z
                            1970-01-01T00:00:01.000000Z\t2020-01-01T16:00:00.000000Z
                            1970-01-01T00:00:00.000001Z\t2020-01-02T00:00:00.000000Z
                            1969-12-31T23:59:59.999999Z\t2020-01-02T04:00:00.000000Z
                            \t2020-01-02T08:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2020-01-02T12:00:00.000000Z
                            1970-01-01T00:00:01.000000Z\t2020-01-02T16:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeDoubleToVarcharWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v DOUBLE");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (3.14, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (-0.5, '2020-01-01T16:00:00.000Z'),
                            (1000000.123, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (42.0, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE VARCHAR");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T08:00:00.000000Z\t3.14
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t-0.5
                            2020-01-01T20:00:00.000000Z\t1000000.123
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t42.0
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES ('99.9', '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T06:00:00.000000Z\t99.9
                            2020-01-01T08:00:00.000000Z\t3.14
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t-0.5
                            2020-01-01T20:00:00.000000Z\t1000000.123
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t42.0
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeFloatToByteWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (10.5, '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (-3.9, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (7.1, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE BYTE");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            10\t2020-01-01T00:00:00.000000Z
                            0\t2020-01-01T06:00:00.000000Z
                            -3\t2020-01-01T12:00:00.000000Z
                            7\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeFloatToDateWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1.0::FLOAT, '2020-01-01T00:00:00.000Z'),
                            (-1.0::FLOAT, '2020-01-01T04:00:00.000Z'),
                            (NULL, '2020-01-01T08:00:00.000Z'),
                            (0.0::FLOAT, '2020-01-01T12:00:00.000Z'),
                            (86400000.0::FLOAT, '2020-01-01T16:00:00.000Z')
                            """
            );
            execute(
                    """
                            INSERT INTO x VALUES
                            (1.0::FLOAT, '2020-01-02T00:00:00.000Z'),
                            (-1.0::FLOAT, '2020-01-02T04:00:00.000Z'),
                            (NULL, '2020-01-02T08:00:00.000Z'),
                            (0.0::FLOAT, '2020-01-02T12:00:00.000Z'),
                            (86400000.0::FLOAT, '2020-01-02T16:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DATE");
            drainWalQueue();

            // FLOAT→DATE is a range-checked f32→i64 cast. NaN→null.
            // Both partitions must produce identical results.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:00.001Z\t2020-01-01T00:00:00.000000Z
                            1969-12-31T23:59:59.999Z\t2020-01-01T04:00:00.000000Z
                            \t2020-01-01T08:00:00.000000Z
                            1970-01-01T00:00:00.000Z\t2020-01-01T12:00:00.000000Z
                            1970-01-02T00:00:00.000Z\t2020-01-01T16:00:00.000000Z
                            1970-01-01T00:00:00.001Z\t2020-01-02T00:00:00.000000Z
                            1969-12-31T23:59:59.999Z\t2020-01-02T04:00:00.000000Z
                            \t2020-01-02T08:00:00.000000Z
                            1970-01-01T00:00:00.000Z\t2020-01-02T12:00:00.000000Z
                            1970-01-02T00:00:00.000Z\t2020-01-02T16:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeFloatToDoubleWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v FLOAT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (1.5, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (3.5, '2020-01-01T16:00:00.000Z'),
                            (4.5, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (5.5, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T08:00:00.000000Z\t1.5
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t3.5
                            2020-01-01T20:00:00.000000Z\t4.5
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t5.5
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES (9.5, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T06:00:00.000000Z\t9.5
                            2020-01-01T08:00:00.000000Z\t1.5
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t3.5
                            2020-01-01T20:00:00.000000Z\t4.5
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t5.5
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeFloatToIntOutOfRangeWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (42.5, '2020-01-01T00:00:00.000Z'),
                            (1e15, '2020-01-01T06:00:00.000Z'),
                            (-1e15, '2020-01-01T12:00:00.000Z'),
                            (NULL, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (7.9, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE INT");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            42\t2020-01-01T00:00:00.000000Z
                            null\t2020-01-01T06:00:00.000000Z
                            null\t2020-01-01T12:00:00.000000Z
                            null\t2020-01-01T18:00:00.000000Z
                            7\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeFloatToLongWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (10.5, '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (-3.9, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (7.1, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            10\t2020-01-01T00:00:00.000000Z
                            null\t2020-01-01T06:00:00.000000Z
                            -3\t2020-01-01T12:00:00.000000Z
                            7\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeFloatToShortWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (10.5, '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (-3.9, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (7.1, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE SHORT");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            10\t2020-01-01T00:00:00.000000Z
                            0\t2020-01-01T06:00:00.000000Z
                            -3\t2020-01-01T12:00:00.000000Z
                            7\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeFloatToTimestampWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v FLOAT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1.0::FLOAT, '2020-01-01T00:00:00.000Z'),
                            (-1.0::FLOAT, '2020-01-01T04:00:00.000Z'),
                            (NULL, '2020-01-01T08:00:00.000Z'),
                            (0.0::FLOAT, '2020-01-01T12:00:00.000Z'),
                            (1000000.0::FLOAT, '2020-01-01T16:00:00.000Z')
                            """
            );
            execute(
                    """
                            INSERT INTO x VALUES
                            (1.0::FLOAT, '2020-01-02T00:00:00.000Z'),
                            (-1.0::FLOAT, '2020-01-02T04:00:00.000Z'),
                            (NULL, '2020-01-02T08:00:00.000Z'),
                            (0.0::FLOAT, '2020-01-02T12:00:00.000Z'),
                            (1000000.0::FLOAT, '2020-01-02T16:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE TIMESTAMP");
            drainWalQueue();

            // FLOAT→TIMESTAMP is a range-checked f32→i64 cast. NaN→null.
            // Both partitions must produce identical results.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:00.000001Z\t2020-01-01T00:00:00.000000Z
                            1969-12-31T23:59:59.999999Z\t2020-01-01T04:00:00.000000Z
                            \t2020-01-01T08:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2020-01-01T12:00:00.000000Z
                            1970-01-01T00:00:01.000000Z\t2020-01-01T16:00:00.000000Z
                            1970-01-01T00:00:00.000001Z\t2020-01-02T00:00:00.000000Z
                            1969-12-31T23:59:59.999999Z\t2020-01-02T04:00:00.000000Z
                            \t2020-01-02T08:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2020-01-02T12:00:00.000000Z
                            1970-01-01T00:00:01.000000Z\t2020-01-02T16:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeIntToBooleanWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (0, '2020-01-01T06:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (42, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (0, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE BOOLEAN");
            drainWalQueue();

            // INT→BOOLEAN: non-zero → true, zero → false, NULL → false.
            // The Rust decoder normalizes via contract_to_bool (not truncation).
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            true\t2020-01-01T00:00:00.000000Z
                            false\t2020-01-01T06:00:00.000000Z
                            false\t2020-01-01T12:00:00.000000Z
                            true\t2020-01-01T18:00:00.000000Z
                            false\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeIntToDateWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (-1, '2020-01-01T04:00:00.000Z'),
                            (2_147_483_647, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (0, '2020-01-01T16:00:00.000Z')
                            """
            );
            // Native partition for comparison.
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, '2020-01-02T00:00:00.000Z'),
                            (-1, '2020-01-02T04:00:00.000Z'),
                            (2_147_483_647, '2020-01-02T08:00:00.000Z'),
                            (NULL, '2020-01-02T12:00:00.000Z'),
                            (0, '2020-01-02T16:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DATE");
            drainWalQueue();

            // INT→DATE is a plain i32→i64 widening. The int value becomes
            // the DATE millisecond value. INT NULL (Integer.MIN_VALUE) maps
            // to DATE NULL. Both partitions must produce identical results.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:00.001Z\t2020-01-01T00:00:00.000000Z
                            1969-12-31T23:59:59.999Z\t2020-01-01T04:00:00.000000Z
                            1970-01-25T20:31:23.647Z\t2020-01-01T08:00:00.000000Z
                            \t2020-01-01T12:00:00.000000Z
                            1970-01-01T00:00:00.000Z\t2020-01-01T16:00:00.000000Z
                            1970-01-01T00:00:00.001Z\t2020-01-02T00:00:00.000000Z
                            1969-12-31T23:59:59.999Z\t2020-01-02T04:00:00.000000Z
                            1970-01-25T20:31:23.647Z\t2020-01-02T08:00:00.000000Z
                            \t2020-01-02T12:00:00.000000Z
                            1970-01-01T00:00:00.000Z\t2020-01-02T16:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeIntToDoubleWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (10, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (30, '2020-01-01T16:00:00.000Z'),
                            (40, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (50, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T08:00:00.000000Z\t10.0
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t30.0
                            2020-01-01T20:00:00.000000Z\t40.0
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t50.0
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES (99, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T06:00:00.000000Z\t99.0
                            2020-01-01T08:00:00.000000Z\t10.0
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t30.0
                            2020-01-01T20:00:00.000000Z\t40.0
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t50.0
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeIntToStringWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (10, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (30, '2020-01-01T16:00:00.000Z'),
                            (40, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (50, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE STRING");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T08:00:00.000000Z\t10
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t30
                            2020-01-01T20:00:00.000000Z\t40
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t50
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES ('99', '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T06:00:00.000000Z\t99
                            2020-01-01T08:00:00.000000Z\t10
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t30
                            2020-01-01T20:00:00.000000Z\t40
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t50
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeIntToTimestampWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (-1, '2020-01-01T04:00:00.000Z'),
                            (2_147_483_647, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (0, '2020-01-01T16:00:00.000Z')
                            """
            );
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, '2020-01-02T00:00:00.000Z'),
                            (-1, '2020-01-02T04:00:00.000Z'),
                            (2_147_483_647, '2020-01-02T08:00:00.000Z'),
                            (NULL, '2020-01-02T12:00:00.000Z'),
                            (0, '2020-01-02T16:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE TIMESTAMP");
            drainWalQueue();

            // INT→TIMESTAMP is a plain i32→i64 widening. The int value becomes
            // the TIMESTAMP microsecond value. Both partitions must match.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:00.000001Z\t2020-01-01T00:00:00.000000Z
                            1969-12-31T23:59:59.999999Z\t2020-01-01T04:00:00.000000Z
                            1970-01-01T00:35:47.483647Z\t2020-01-01T08:00:00.000000Z
                            \t2020-01-01T12:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2020-01-01T16:00:00.000000Z
                            1970-01-01T00:00:00.000001Z\t2020-01-02T00:00:00.000000Z
                            1969-12-31T23:59:59.999999Z\t2020-01-02T04:00:00.000000Z
                            1970-01-01T00:35:47.483647Z\t2020-01-02T08:00:00.000000Z
                            \t2020-01-02T12:00:00.000000Z
                            1970-01-01T00:00:00.000000Z\t2020-01-02T16:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeIntToVarcharWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (10, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (30, '2020-01-01T16:00:00.000Z'),
                            (40, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (50, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormatFromMetadata(0));
                Assert.assertEquals(PartitionFormat.NATIVE, reader.getPartitionFormatFromMetadata(1));
            }

            // INT → VARCHAR. Parquet partition has column_top=2 for v.
            execute("ALTER TABLE x ALTER COLUMN v TYPE VARCHAR");
            drainWalQueue();

            // Read path: lazy conversion in PageFrameMemoryRecord.
            // column_top rows → null (empty), explicit NULL → null, values → string representation.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T08:00:00.000000Z\t10
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t30
                            2020-01-01T20:00:00.000000Z\t40
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t50
                            """);

            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormatFromMetadata(0));
                Assert.assertEquals(PartitionFormat.NATIVE, reader.getPartitionFormatFromMetadata(1));
            }

            // O3 merge: Rust post_convert eagerly converts INT → VARCHAR.
            execute("INSERT INTO x(v, ts) VALUES ('99', '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T06:00:00.000000Z\t99
                            2020-01-01T08:00:00.000000Z\t10
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t30
                            2020-01-01T20:00:00.000000Z\t40
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t50
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeLongToDateWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1000, '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (1000000, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DATE");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:01.000Z\t2020-01-01T00:00:00.000000Z
                            \t2020-01-01T06:00:00.000000Z
                            1970-01-01T00:16:40.000Z\t2020-01-01T12:00:00.000000Z
                            1970-01-01T00:00:00.100Z\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeLongToFloatWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (100, '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (-42, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (7, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE FLOAT");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            100.0\t2020-01-01T00:00:00.000000Z
                            null\t2020-01-01T06:00:00.000000Z
                            -42.0\t2020-01-01T12:00:00.000000Z
                            7.0\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeLongToIntWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (10, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (30, '2020-01-01T16:00:00.000Z'),
                            (40, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (50, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // LONG narrowed to INT — small values are preserved.
            execute("ALTER TABLE x ALTER COLUMN v TYPE INT");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T08:00:00.000000Z\t10
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t30
                            2020-01-01T20:00:00.000000Z\t40
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t50
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES (99, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T06:00:00.000000Z\t99
                            2020-01-01T08:00:00.000000Z\t10
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t30
                            2020-01-01T20:00:00.000000Z\t40
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t50
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeLongToTimestampWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (1000000, '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            (1000000000, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (100000, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE TIMESTAMP");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:01.000000Z\t2020-01-01T00:00:00.000000Z
                            \t2020-01-01T06:00:00.000000Z
                            1970-01-01T00:16:40.000000Z\t2020-01-01T12:00:00.000000Z
                            1970-01-01T00:00:00.100000Z\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeLongToVarcharWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v LONG");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (100_000, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (-42, '2020-01-01T16:00:00.000Z'),
                            (9_999_999_999, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (7, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE VARCHAR");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T08:00:00.000000Z\t100000
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t-42
                            2020-01-01T20:00:00.000000Z\t9999999999
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t7
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES ('123', '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\t
                            2020-01-01T04:00:00.000000Z\t
                            2020-01-01T06:00:00.000000Z\t123
                            2020-01-01T08:00:00.000000Z\t100000
                            2020-01-01T12:00:00.000000Z\t
                            2020-01-01T16:00:00.000000Z\t-42
                            2020-01-01T20:00:00.000000Z\t9999999999
                            2020-01-02T00:00:00.000000Z\t
                            2020-01-02T12:00:00.000000Z\t7
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeShortToDoubleWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (100, '2020-01-01T00:00:00.000Z'),
                            (-32768, '2020-01-01T06:00:00.000Z'),
                            (0, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (1, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DOUBLE");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            100.0\t2020-01-01T00:00:00.000000Z
                            -32768.0\t2020-01-01T06:00:00.000000Z
                            0.0\t2020-01-01T12:00:00.000000Z
                            1.0\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeShortToFloatWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v SHORT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            (100, '2020-01-01T00:00:00.000Z'),
                            (32767, '2020-01-01T06:00:00.000Z'),
                            (-100, '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES (7, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE FLOAT");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            100.0\t2020-01-01T00:00:00.000000Z
                            32767.0\t2020-01-01T06:00:00.000000Z
                            -100.0\t2020-01-01T12:00:00.000000Z
                            7.0\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeShortToIntWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v SHORT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (5, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (7, '2020-01-01T16:00:00.000Z'),
                            (8, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (9, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE INT");
            drainWalQueue();

            // The 00:00 and 04:00 rows were inserted before ADD COLUMN v -- they are
            // column_top rows and read back as INT null on both native and parquet
            // (parquet stores them as def-level=0 since SHORT/BYTE/CHAR are now
            // declared OPTIONAL in the parquet schema). Explicit NULL inserted into
            // SHORT becomes 0 because SHORT has no null sentinel; that 0 stays 0
            // after SHORT->INT on both paths.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T08:00:00.000000Z\t5
                            2020-01-01T12:00:00.000000Z\t0
                            2020-01-01T16:00:00.000000Z\t7
                            2020-01-01T20:00:00.000000Z\t8
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t9
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES (99, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T06:00:00.000000Z\t99
                            2020-01-01T08:00:00.000000Z\t5
                            2020-01-01T12:00:00.000000Z\t0
                            2020-01-01T16:00:00.000000Z\t7
                            2020-01-01T20:00:00.000000Z\t8
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t9
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeStringToAllFixedWithParquetPartition() throws Exception {
        testConvertVarToAllFixed("STRING");
    }

    @Test
    public void testAlterColumnTypeTimestampToDateWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            ('1970-01-01T00:00:01.000000Z', '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            ('1970-01-01T00:16:40.123456Z', '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES ('1970-01-01T00:00:00.500999Z', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE DATE");
            drainWalQueue();

            // TIMESTAMP (µs) → DATE (ms): values are divided by 1000, truncating
            // sub-millisecond precision.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1970-01-01T00:00:01.000Z\t2020-01-01T00:00:00.000000Z
                            \t2020-01-01T06:00:00.000000Z
                            1970-01-01T00:16:40.123Z\t2020-01-01T12:00:00.000000Z
                            1970-01-01T00:00:00.500Z\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeTimestampToLongWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x (v TIMESTAMP, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    """
                            INSERT INTO x VALUES
                            ('1970-01-01T00:00:01.000000Z', '2020-01-01T00:00:00.000Z'),
                            (NULL, '2020-01-01T06:00:00.000Z'),
                            ('1970-01-01T00:16:40.000000Z', '2020-01-01T12:00:00.000Z')
                            """
            );
            execute("INSERT INTO x VALUES ('1970-01-01T00:00:00.100000Z', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();

            // TIMESTAMP and LONG share the same i64 representation (microseconds)
            // and null sentinel (Long.MIN_VALUE). Conversion is a no-op.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts
                            1000000\t2020-01-01T00:00:00.000000Z
                            null\t2020-01-01T06:00:00.000000Z
                            1000000000\t2020-01-01T12:00:00.000000Z
                            100000\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeToSymbolWithUndefinedParquetColumn() throws Exception {
        // When a column is added after a partition is converted to parquet,
        // the parquet file has no data for that column (UNDEFINED). Converting
        // that column to SYMBOL must set the null flag on the symbol map,
        // because all rows in the parquet partition are NULL for that column.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (v INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T04:00:00.000Z'),
                            (3, '2020-01-01T08:00:00.000Z')
                            """
            );
            drainWalQueue();

            // Convert to parquet before adding the new column.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormatFromMetadata(0));
            }

            // Add a STRING column — it won't exist in the parquet file.
            execute("ALTER TABLE x ADD COLUMN s STRING");
            drainWalQueue();

            // Insert rows with non-null values for the new column in a new partition.
            execute(
                    """
                            INSERT INTO x VALUES
                            (4, '2020-01-02T00:00:00.000Z', 'abc'),
                            (5, '2020-01-02T04:00:00.000Z', 'def')
                            """
            );
            drainWalQueue();

            // Convert s from STRING to SYMBOL. The parquet partition has
            // UNDEFINED for s, so the pre-pass should set the null flag.
            execute("ALTER TABLE x ALTER COLUMN s TYPE SYMBOL");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            // Verify: parquet partition rows have NULL for 's',
            // native partition rows have the symbol values.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts\ts
                            1\t2020-01-01T00:00:00.000000Z\t
                            2\t2020-01-01T04:00:00.000000Z\t
                            3\t2020-01-01T08:00:00.000000Z\t
                            4\t2020-01-02T00:00:00.000000Z\tabc
                            5\t2020-01-02T04:00:00.000000Z\tdef
                            """);

            // Re-convert to parquet. If the null flag was not set on the symbol
            // map, the encoder would use Required encoding for 's', which fails
            // to encode the NULL rows from the old parquet partition.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            v\tts\ts
                            1\t2020-01-01T00:00:00.000000Z\t
                            2\t2020-01-01T04:00:00.000000Z\t
                            3\t2020-01-01T08:00:00.000000Z\t
                            4\t2020-01-02T00:00:00.000000Z\tabc
                            5\t2020-01-02T04:00:00.000000Z\tdef
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeVarcharToAllFixedWithParquetPartition() throws Exception {
        testConvertVarToAllFixed("VARCHAR");
    }

    @Test
    public void testAlterColumnTypeWithDecimalInParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (
                                d DECIMAL(8,2),
                                ts TIMESTAMP
                            ) TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Initial rows — column_top = 2 for 'v' after ADD COLUMN.
            execute(
                    """
                            INSERT INTO x(d, ts) VALUES
                            ('100.50', '2020-01-01T00:00:00.000Z'),
                            ('200.75', '2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(d, ts) VALUES ('600.00', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(d, v, ts) VALUES
                            ('300.25', 10, '2020-01-01T08:00:00.000Z'),
                            ('350.00', NULL, '2020-01-01T12:00:00.000Z'),
                            ('400.99', 30, '2020-01-01T16:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(d, v, ts) VALUES ('700.50', 50, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            d\tts\tv
                            100.50\t2020-01-01T00:00:00.000000Z\tnull
                            200.75\t2020-01-01T04:00:00.000000Z\tnull
                            300.25\t2020-01-01T08:00:00.000000Z\t10
                            350.00\t2020-01-01T12:00:00.000000Z\tnull
                            400.99\t2020-01-01T16:00:00.000000Z\t30
                            600.00\t2020-01-02T00:00:00.000000Z\tnull
                            700.50\t2020-01-02T12:00:00.000000Z\t50
                            """);

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(d, v, ts) VALUES ('999.99', 99, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            d\tts\tv
                            100.50\t2020-01-01T00:00:00.000000Z\tnull
                            200.75\t2020-01-01T04:00:00.000000Z\tnull
                            999.99\t2020-01-01T06:00:00.000000Z\t99
                            300.25\t2020-01-01T08:00:00.000000Z\t10
                            350.00\t2020-01-01T12:00:00.000000Z\tnull
                            400.99\t2020-01-01T16:00:00.000000Z\t30
                            600.00\t2020-01-02T00:00:00.000000Z\tnull
                            700.50\t2020-01-02T12:00:00.000000Z\t50
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeWithExoticColumnsInParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (
                                ip IPv4,
                                uu UUID,
                                gh GEOHASH(4c),
                                l2 LONG256,
                                ts TIMESTAMP
                            ) TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Initial rows — column_top = 2 for 'v' after ADD COLUMN.
            execute(
                    """
                            INSERT INTO x(ip, uu, gh, l2, ts) VALUES
                            ('10.0.0.1', '11111111-1111-1111-1111-111111111111', #u33d, CAST('0x01' AS LONG256), '2020-01-01T00:00:00.000Z'),
                            ('10.0.0.2', '22222222-2222-2222-2222-222222222222', #u33e, CAST('0x02' AS LONG256), '2020-01-01T04:00:00.000Z')
                            """
            );
            execute(
                    """
                            INSERT INTO x(ip, uu, gh, l2, ts) VALUES
                            ('10.0.0.6', '66666666-6666-6666-6666-666666666666', #u33k, CAST('0x06' AS LONG256), '2020-01-02T00:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            execute(
                    """
                            INSERT INTO x(ip, uu, gh, l2, v, ts) VALUES
                            ('10.0.0.3', '33333333-3333-3333-3333-333333333333', #u33f, CAST('0x03' AS LONG256), 10, '2020-01-01T08:00:00.000Z'),
                            ('10.0.0.4', '44444444-4444-4444-4444-444444444444', #u33g, CAST('0x04' AS LONG256), NULL, '2020-01-01T12:00:00.000Z'),
                            ('10.0.0.5', '55555555-5555-5555-5555-555555555555', #u33h, CAST('0x05' AS LONG256), 30, '2020-01-01T16:00:00.000Z')
                            """
            );
            execute(
                    """
                            INSERT INTO x(ip, uu, gh, l2, v, ts) VALUES
                            ('10.0.0.7', '77777777-7777-7777-7777-777777777777', #u33m, CAST('0x07' AS LONG256), 50, '2020-01-02T12:00:00.000Z')
                            """
            );
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // INT → LONG with column_top, NULLs, and exotic columns.
            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ip\tuu\tgh\tl2\tts\tv
                            10.0.0.1\t11111111-1111-1111-1111-111111111111\tu33d\t0x01\t2020-01-01T00:00:00.000000Z\tnull
                            10.0.0.2\t22222222-2222-2222-2222-222222222222\tu33e\t0x02\t2020-01-01T04:00:00.000000Z\tnull
                            10.0.0.3\t33333333-3333-3333-3333-333333333333\tu33f\t0x03\t2020-01-01T08:00:00.000000Z\t10
                            10.0.0.4\t44444444-4444-4444-4444-444444444444\tu33g\t0x04\t2020-01-01T12:00:00.000000Z\tnull
                            10.0.0.5\t55555555-5555-5555-5555-555555555555\tu33h\t0x05\t2020-01-01T16:00:00.000000Z\t30
                            10.0.0.6\t66666666-6666-6666-6666-666666666666\tu33k\t0x06\t2020-01-02T00:00:00.000000Z\tnull
                            10.0.0.7\t77777777-7777-7777-7777-777777777777\tu33m\t0x07\t2020-01-02T12:00:00.000000Z\t50
                            """);

            // O3 merge into the parquet partition.
            execute(
                    """
                            INSERT INTO x(ip, uu, gh, l2, v, ts) VALUES
                            ('10.0.0.9', '99999999-9999-9999-9999-999999999999', #u33z, CAST('0x09' AS LONG256), 99, '2020-01-01T06:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ip\tuu\tgh\tl2\tts\tv
                            10.0.0.1\t11111111-1111-1111-1111-111111111111\tu33d\t0x01\t2020-01-01T00:00:00.000000Z\tnull
                            10.0.0.2\t22222222-2222-2222-2222-222222222222\tu33e\t0x02\t2020-01-01T04:00:00.000000Z\tnull
                            10.0.0.9\t99999999-9999-9999-9999-999999999999\tu33z\t0x09\t2020-01-01T06:00:00.000000Z\t99
                            10.0.0.3\t33333333-3333-3333-3333-333333333333\tu33f\t0x03\t2020-01-01T08:00:00.000000Z\t10
                            10.0.0.4\t44444444-4444-4444-4444-444444444444\tu33g\t0x04\t2020-01-01T12:00:00.000000Z\tnull
                            10.0.0.5\t55555555-5555-5555-5555-555555555555\tu33h\t0x05\t2020-01-01T16:00:00.000000Z\t30
                            10.0.0.6\t66666666-6666-6666-6666-666666666666\tu33k\t0x06\t2020-01-02T00:00:00.000000Z\tnull
                            10.0.0.7\t77777777-7777-7777-7777-777777777777\tu33m\t0x07\t2020-01-02T12:00:00.000000Z\t50
                            """);
        });
    }

    @Test
    public void testAlterColumnTypeWithParquetPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Initial rows — column_top = 2 for 'v' after ADD COLUMN.
            execute(
                    """
                            INSERT INTO x(ts) VALUES
                            ('2020-01-01T00:00:00.000Z'),
                            ('2020-01-01T04:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(ts) VALUES ('2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN v INT");
            drainWalQueue();

            // More rows with v, including an explicit NULL.
            execute(
                    """
                            INSERT INTO x(v, ts) VALUES
                            (10, '2020-01-01T08:00:00.000Z'),
                            (NULL, '2020-01-01T12:00:00.000Z'),
                            (30, '2020-01-01T16:00:00.000Z'),
                            (40, '2020-01-01T20:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(v, ts) VALUES (50, '2020-01-02T12:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormatFromMetadata(0));
                Assert.assertEquals(PartitionFormat.NATIVE, reader.getPartitionFormatFromMetadata(1));
            }

            // INT → LONG. Parquet partition has column_top=2 for v.
            execute("ALTER TABLE x ALTER COLUMN v TYPE LONG");
            drainWalQueue();

            // column_top rows → null, explicit NULL preserved, values converted.
            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T08:00:00.000000Z\t10
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t30
                            2020-01-01T20:00:00.000000Z\t40
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t50
                            """);

            try (TableReader reader = getReader("x")) {
                Assert.assertEquals(PartitionFormat.PARQUET, reader.getPartitionFormatFromMetadata(0));
                Assert.assertEquals(PartitionFormat.NATIVE, reader.getPartitionFormatFromMetadata(1));
            }

            // O3 merge into the parquet partition.
            execute("INSERT INTO x(v, ts) VALUES (99, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x").noLeakCheck().inferTimestamp().inferRandomAccess().sizeMayVary().returns(
                    """
                            ts\tv
                            2020-01-01T00:00:00.000000Z\tnull
                            2020-01-01T04:00:00.000000Z\tnull
                            2020-01-01T06:00:00.000000Z\t99
                            2020-01-01T08:00:00.000000Z\t10
                            2020-01-01T12:00:00.000000Z\tnull
                            2020-01-01T16:00:00.000000Z\t30
                            2020-01-01T20:00:00.000000Z\t40
                            2020-01-02T00:00:00.000000Z\tnull
                            2020-01-02T12:00:00.000000Z\t50
                            """);
        });
    }

    @Test
    public void testArrayCorruptionAfterWriterReopenBeforeParquetConversion() throws Exception {
        // Reproduces DOUBLE[][] data corruption from WalWriterFuzzTest
        // (seeds 286709679787041L, 1772793547818L).
        //
        // Matches the fuzz test's exact scenario for partition 2022-02-27:
        //  - 896 initial rows (no array column)
        //  - ADD COLUMN DOUBLE[][] → column_top = 896
        //  - INSERT 202 rows with array data
        //  - Writer closes (engine.releaseInactive)
        //  - Convert partition to parquet (column_top=896)
        //  - Then: native→append→parquet→native→append→parquet→O3 merge
        //
        // Tests both A (writer stays open) and B (writer closes) scenarios
        // to detect if close/reopen introduces corruption.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows (matching fuzz test column_top=896)
            // Timestamps: 2020-01-01T00:00:00 through ~2020-01-01T00:14:56 (every second)
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            // Row in next partition so 2020-01-01T00 is non-active
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-01T01:00:00.000Z', -1)");
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            // INSERT 202 rows with 3x3 null-double arrays (matching fuzz test)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            // Apply ADD COLUMN + INSERT together (same WAL batch)
            drainWalQueue();

            // Verify baseline before any conversions
            assertQuery("SELECT * FROM x WHERE x = 896")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 897")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);

            // *** Critical point: writer closes before convert to parquet ***
            engine.releaseInactive();

            // Convert to parquet (column_top=896, 1098 total rows)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();

            // Check immediately after parquet conversion
            assertQuery("SELECT * FROM x WHERE x = 896")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 897")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);

            // Convert back to native (materializes all 1098 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            drainWalQueue();

            // Check after native conversion
            assertQuery("SELECT * FROM x WHERE x = 896")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 897")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);

            engine.releaseInactive();

            // Append more rows, then convert to parquet again (column_top=0)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1098; i < 1722; i++) {
                if (i > 1098) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();
            engine.releaseInactive();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            drainWalQueue();
            engine.releaseInactive();

            // Append more
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1722; i < 2256; i++) {
                if (i > 1722) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Convert to parquet (this is the file the O3 merge will read)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            drainWalQueue();

            // Check the parquet file
            assertQuery("SELECT * FROM x WHERE x = 896")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 897")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);

            engine.releaseInactive();

            // O3 merge into parquet — matches fuzz test's 949-row merge
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 0; i < 949; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Use half-second timestamps to interleave with existing full-second data
                sb.append(String.format("('2020-01-01T00:%02d:%02d.500Z', ", i / 60, i % 60))
                        .append(20_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify after O3 merge
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x WHERE x = 896")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 897")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 0")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """);
            assertQuery("SELECT * FROM x WHERE x = 20000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:00:00.500000Z\t20000\t[[0.0]]
                            """);
        });
    }

    @Test
    public void testArrayCorruptionMinimalRoundTripInSingleWriterSession() throws Exception {
        // Minimal reproducer: just CONVERT TO PARQUET → CONVERT TO NATIVE →
        // CONVERT TO PARQUET in one writer session. No inserts between conversions.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Queue just: CONVERT TO PARQUET → CONVERT TO NATIVE → CONVERT TO PARQUET
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            drainWalQueue();

            assertQuery("SELECT * FROM x WHERE x = 896")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
        });
    }

    @Test
    public void testArrayCorruptionMultipleRoundTripsInSingleWriterSession() throws Exception {
        // Reproduces DOUBLE[][] data corruption when multiple parquet↔native
        // round-trips happen in a single writer session without ejection.
        //
        // Bug: WalWriterFuzzTest#testConvertPartitionToParquet
        //      seeds 286709679787041L, 1772793547818L
        // Error: Row 1144 column new_col_8[DOUBLE[][]]
        //        expected:<...[[[null,null,null],[null,null,null],[null,null,null]]]>
        //        but was:<...[null]>
        //
        // The critical difference from testArrayCorruptionAfterWriterReopenBeforeParquetConversion:
        // no engine.releaseInactive() between operations — all conversions
        // and inserts execute in one writer session via a single drainWalQueue().
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY HOUR WAL
                            """
            );

            // Insert 896 rows (matching fuzz test column_top=896)
            for (int batch = 0; batch < 9; batch++) {
                int lo = batch * 100;
                int hi = Math.min(lo + 100, 896);
                StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
                for (int i = lo; i < hi; i++) {
                    if (i > lo) {
                        sb.append(",\n");
                    }
                    sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                            .append(i).append(')');
                }
                execute(sb.toString());
            }
            drainWalQueue();
            engine.releaseInactive();

            // ADD COLUMN DOUBLE[][] → column_top = 896
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            // INSERT 202 rows with 3x3 null-double arrays (matching fuzz test)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 896; i < 1098; i++) {
                if (i > 896) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();
            engine.releaseInactive();

            // Verify baseline
            assertQuery("SELECT * FROM x WHERE x = 897")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);

            // Queue ALL critical operations for a single writer session

            // 1. native→parquet (column_top=896→zeroed to 0)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // 2. parquet→native (materializes all 1098 rows, column_top=0)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");

            // 3. Insert 624 more rows (timestamps 00:18:18–00:28:41)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1098; i < 1722; i++) {
                if (i > 1098) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());

            // 4. native→parquet (1722 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // 5. parquet→native (1722 rows)
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01T00'");

            // 6. Insert 534 more rows (timestamps 00:28:42–00:37:35)
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 1722; i < 2256; i++) {
                if (i > 1722) {
                    sb.append(",\n");
                }
                sb.append(String.format("('2020-01-01T00:%02d:%02d.000Z', ", i / 60, i % 60))
                        .append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());

            // 7. native→parquet (2256 rows — corruption manifests here)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01T00'");

            // Apply ALL operations in one writer session
            drainWalQueue();

            // Verify row 897 has 3x3 matrix, not NULL
            assertQuery("SELECT * FROM x WHERE x = 896")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:56.000000Z\t896\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 897")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:14:57.000000Z\t897\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 0")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """);
            assertQuery("SELECT * FROM x WHERE x = 1098")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:18:18.000000Z\t1098\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
        });
    }

    @Test
    public void testCopyRowGroupPreservesBloomFilterOffset() throws Exception {
        // Small row group size to produce 3 row groups.
        // Set absolute threshold low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 12 rows → 3 row groups (RG0, RG1, RG2) of 4 rows each.
            // Values chosen so the bloom filter is the only way to skip
            // the copied row group for specific query values.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z'),
                            (9, '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            // Extra row in a different partition so 2020-01-01 is not the last.
            execute("INSERT INTO x(x, ts) VALUES (1000, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Convert with bloom filters on the 'x' column.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01' WITH (bloom_filter_columns = 'x')");
            drainWalQueue();

            // First O3: UPDATE mode. Inserts into RG0's time range.
            // This replaces RG0, appending the new RG0' at the end of the file,
            // leaving dead RG0 data at the original position near the file start.
            // RG0' values: {1, 100, 2, 101, 3, 4} → min=1, max=101
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (100, '2020-01-01T00:30:00.000Z'),
                            (101, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Second O3: targets RG1's time range (different from the first O3).
            // accumulated unused_bytes > 100 → REWRITE mode.
            // In the REWRITE, RG0' (appended at the end of the old file) is
            // raw-copied to the beginning of the new file via copy_row_group,
            // creating a large offset_delta. copy_row_group must adjust
            // bloom_filter_offset by the same delta as data_page_offset.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (200, '2020-01-01T04:30:00.000Z'),
                            (201, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // After REWRITE, the parquet partition has 3 row groups:
            //   RG0' (copied): x in {1, 100, 2, 101, 3, 4} → min=1, max=101
            //   RG1' (merged): x in {5, 200, 6, 201, 7, 8} → min=5, max=201
            //   RG2  (copied): x in {9, 10, 11, 12}         → min=9, max=12
            //
            // Query: WHERE x = 50
            //   RG0': 50 in [1,101] → can't skip by min/max → bloom filter needed
            //         50 NOT in {1,100,2,101,3,4} → bloom SHOULD skip
            //   RG1': 50 in [5,201] → can't skip by min/max → bloom filter needed
            //         50 NOT in {5,200,6,201,7,8} → bloom SHOULD skip
            //   RG2:  50 > 12 → skipped by min/max alone
            //
            // If bloom_filter_offset is stale on copied RG0', its bloom filter
            // read fails silently → RG0' is NOT skipped → fewer skips.
            // Verify data correctness.
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            100\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            101\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            200\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            201\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            9\t2020-01-01T08:00:00.000000Z
                            10\t2020-01-01T09:00:00.000000Z
                            11\t2020-01-01T10:00:00.000000Z
                            12\t2020-01-01T11:00:00.000000Z
                            1000\t2020-01-02T00:00:00.000000Z
                            """);

            // Bloom filter skip check. assertSql runs a single query execution,
            // so the counter reflects exactly one scan of the 3 row groups.
            // Query: WHERE x = 50
            //   RG0' (copied): 50 in [1,101] → needs bloom filter → should skip
            //   RG1' (merged): 50 in [5,201] → needs bloom filter → should skip
            //   RG2  (copied): 50 > 12       → skipped by min/max
            // All 3 row groups should be skipped → counter = 3.
            // With stale bloom_filter_offset on copied RG0', bloom filter read
            // fails silently → RG0' NOT skipped → counter = 2.
            ParquetRowGroupFilter.resetRowGroupsSkipped();
            // Use returnsOnce(): it runs a single query execution (the builder's
            // single-factory path, equivalent to assertSql), so the row-group skip
            // counter reflects exactly one scan. The returns() path would re-read
            // the cursor, which would inflate the counter.
            assertQuery("SELECT x FROM x WHERE x = 50")
                    .noLeakCheck()
                    .returnsOnce("x\n");
            Assert.assertEquals(
                    "bloom filter should skip all 3 row groups (2 by bloom, 1 by min/max)",
                    3,
                    ParquetRowGroupFilter.getRowGroupsSkipped()
            );
        });
    }

    @Test
    public void testDedupRowGroupBoundary() throws Exception {
        // A single timestamp value (11:58:00) straddles the boundary between two
        // parquet row groups: with row group size 5750 and 8 symbols per minute, the
        // 8000-row first commit writes the 2020-02-27 partition as two row groups and
        // splits minute 718 (11:58) across them - 6 symbols in rg0, 2 in rg1. The
        // second commit re-inserts all 8 symbols at 11:58 as out-of-order data; they
        // are known dedup keys, so the row count must stay 8000 with no (ts, s)
        // duplicates. The merge must deduplicate the boundary symbols that live in rg1.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 5750);
        assertMemoryLeak(() -> {
            execute(
                    "create table tab (ts timestamp, s symbol index) " +
                            "timestamp(ts) partition by day format parquet wal " +
                            "dedup upsert keys(ts, s)"
            );

            // Commit 1: 8000 in-order rows (1000 minutes x 8 symbols) -> Feb 27
            // partition written as parquet with two row groups, minute 718
            // (11:58:00) split across the boundary.
            execute(
                    "insert into tab " +
                            "select dateadd('m', a.m, '2020-02-27T00:00:00.000000Z'::TIMESTAMP) ts, ('sym' || b.sy) s " +
                            "from (select x::INT - 1 m from long_sequence(1000)) a " +
                            "cross join (select x::INT - 1 sy from long_sequence(8)) b " +
                            "order by ts, s"
            );
            drainWalQueue();
            assertQuery("select count() from tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n8000\n");

            // Commit 2: re-insert all 8 symbols at the boundary timestamp as
            // out-of-order data. They are all known dedup keys, so the row count
            // must stay 8000 and no (ts, s) duplicates may appear.
            execute(
                    "insert into tab " +
                            "select '2020-02-27T11:58:00.000000Z'::TIMESTAMP, ('sym' || (x - 1)) " +
                            "from long_sequence(8)"
            );
            drainWalQueue();

            assertQuery("select count() from tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n8000\n");
            // No (ts, s) duplicates: the largest group size must be 1. Use max() over the
            // materialized group-by rather than count() over a filtered subquery, which
            // trips an unrelated count fast-path bug (questdb/questdb#7201).
            assertQuery("select max(c) from (select ts, s, count() c from tab)")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("max\n1\n");
        });
    }

    @Test
    public void testDedupSameTimestampSpansThreeRowGroups() throws Exception {
        // Degraded variant of testDedupRowGroupBoundary: a single timestamp value
        // is large enough to span THREE parquet row groups. With row group size 4,
        // 12 rows that all share timestamp 12:00 are written as 3 row groups, each
        // with min == max == 12:00. A second commit re-inserts the same 12 keys as
        // out-of-order data.
        //
        // computeMergeActions assigns ALL 12 O3 rows to the first row group (the
        // overlap test o3Ts <= rgMax matches rg0, then o3Cursor advances past the
        // whole run). mergeRowGroup must deduplicate them against every tied row
        // group, not just rg0, so the keys living in rg1/rg2 are not duplicated.
        // Correct behaviour: the row count stays 12 with no (ts, s) duplicates.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    "create table tab (ts timestamp, s symbol index) " +
                            "timestamp(ts) partition by day format parquet wal " +
                            "dedup upsert keys(ts, s)"
            );

            // Commit 1: 12 rows at a single timestamp -> 3 row groups, all [12:00, 12:00].
            execute(
                    "insert into tab " +
                            "select '2020-02-27T12:00:00.000000Z'::TIMESTAMP, ('sym' || (x - 1)) " +
                            "from long_sequence(12)"
            );
            drainWalQueue();
            assertQuery("select count() from tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n12\n");

            // Commit 2: re-insert the same 12 keys at the same timestamp as
            // out-of-order data. All are known dedup keys, so the count must stay 12
            // and no (ts, s) duplicates may appear.
            execute(
                    "insert into tab " +
                            "select '2020-02-27T12:00:00.000000Z'::TIMESTAMP, ('sym' || (x - 1)) " +
                            "from long_sequence(12)"
            );
            drainWalQueue();

            assertQuery("select count() from tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n12\n");
            // No (ts, s) duplicates: the largest group size must be 1. Use max() over the
            // materialized group-by rather than count() over a filtered subquery, which
            // trips an unrelated count fast-path bug (questdb/questdb#7201).
            assertQuery("select max(c) from (select ts, s, count() c from tab)")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("max\n1\n");
        });
    }

    @Test
    public void testDirtyAheadHeaderQueryReturnsCommittedSnapshot() throws Exception {
        // Drive the crash window end-to-end through SQL. commitParquetMeta
        // publishes the _pm header (M) before the _txn commit (N); a crash in that
        // window leaves _pm physically grown with its header dirty-ahead of the
        // committed footer, while _txn still records N. A query maps _pm at the raw
        // header M but must resolve the committed footer at N and serve only
        // committed rows -- no SIGBUS, no truncate, no never-committed data. The
        // reader-level testDirtyAheadHeaderResolvesToCommittedHead pins this; here
        // it runs against a real table through SELECT and SHOW PARTITIONS.
        //
        // Row-group-size 4 over 8 rows -> the committed footer carries two row
        // groups, more than the spliced dead footer's one, so the row-group span
        // reported by SHOW PARTITIONS proves the dead footer was skipped.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);

        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows / row-group-size 4 -> two row groups in the committed footer.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            TableToken tableToken = engine.verifyTableName("x");
            File tableDir = new File(root, tableToken.getDirName());
            final long committedPmSize = parquetMetaLength(tableDir);
            Assert.assertTrue(committedPmSize > 0);

            // Baseline: the committed snapshot reads back as eight rows. The
            // interval-filtered parquet scan reports an unknown size, hence
            // sizeMayVary/noRandomAccess rather than expectSize here.
            assertQuery("SELECT * FROM x WHERE ts IN '2020-01-01'")
                    .noLeakCheck()
                    .sizeMayVary()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            """);

            // Splice _pm into the dirty-ahead crash state. Release every handle
            // first so the rewrite never races a live mmap (required on Windows).
            engine.releaseAllWriters();
            engine.releaseAllReaders();
            engine.releaseInactive();
            final long dirtyPmSize = makeParquetMetaDirtyAhead(tableDir);
            Assert.assertTrue(
                    "dead footer must grow _pm past the committed head [committed=" + committedPmSize + ", dirty=" + dirtyPmSize + "]",
                    dirtyPmSize > committedPmSize
            );
            Assert.assertEquals("header must read the dirty-ahead M", dirtyPmSize, parquetMetaHeaderSize(tableDir));

            // Fresh readers map _pm at the dirty header M, walk back to the
            // committed footer at N, and serve the committed snapshot unchanged --
            // the dead footer past the committed head is never read.
            assertQuery("SELECT * FROM x WHERE ts IN '2020-01-01'")
                    .noLeakCheck()
                    .sizeMayVary()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            """);

            // SHOW PARTITIONS maps _pm too, reading min/max from the resolved
            // footer's row groups: the committed two-row-group span (00:00..07:00,
            // 8 rows) confirms the dead one-row-group footer was skipped.
            assertQuery(
                    """
                            SELECT name, minTimestamp, maxTimestamp, numRows, isParquet
                            FROM table_partitions('x') WHERE isParquet
                            """
            )
                    .noLeakCheck()
                    .sizeMayVary()
                    .noRandomAccess()
                    .returns("""
                            name\tminTimestamp\tmaxTimestamp\tnumRows\tisParquet
                            2020-01-01\t2020-01-01T00:00:00.000000Z\t2020-01-01T07:00:00.000000Z\t8\ttrue
                            """);

            // The read path never truncates _pm: it stays dirty-ahead at M.
            Assert.assertEquals("read path must not truncate _pm", dirtyPmSize, parquetMetaLength(tableDir));
            Assert.assertEquals(dirtyPmSize, parquetMetaHeaderSize(tableDir));
        });
    }

    @Test
    public void testFooterCacheUsedInUpdateMode() throws Exception {
        // Small row group size to produce multiple row groups.
        // Disable rewrite: ratio=1.0 (impossible to exceed), max_bytes=Long.MAX_VALUE.
        // This forces every O3 merge to use the UPDATE path, exercising footer_cache.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            int rowGroupCountBefore = -1;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    ParquetPartitionDecoder decoder = reader.getAndInitParquetPartitionDecoder(i);
                    rowGroupCountBefore = decoder.metadata().getRowGroupCount();
                    Assert.assertEquals("initial row group count", 2, rowGroupCountBefore);
                }
            }
            Assert.assertTrue("should find parquet partition", rowGroupCountBefore > 0);

            // First O3: UPDATE mode. FooterCache parses the original footer,
            // scans row group offsets, and appends a replacement row group.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            int rowGroupCountAfterFirst = 0;
            long unusedAfterFirst = 0;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    rowGroupCountAfterFirst = reader.getAndInitParquetPartitionDecoder(i).metadata().getRowGroupCount();
                    try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                        footerDecoder.of(reader.getParquetAddr(i), reader.getParquetFileSize(i), MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        unusedAfterFirst = footerDecoder.metadata().getUnusedBytes();
                    }
                    Assert.assertTrue(
                            "row group count should be >= 2 after first O3 update, got " + rowGroupCountAfterFirst,
                            rowGroupCountAfterFirst >= 2
                    );
                    Assert.assertTrue(
                            "unused_bytes should be > 0 after first update, got " + unusedAfterFirst,
                            unusedAfterFirst > 0
                    );
                }
            }

            // Second O3: UPDATE mode again. FooterCache re-parses the updated footer
            // (which already contains dead space from the first update) and appends
            // another replacement row group.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (11, '2020-01-01T04:30:00.000Z'),
                            (12, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            int rowGroupCountAfterSecond;
            long unusedAfterSecond;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    rowGroupCountAfterSecond = reader.getAndInitParquetPartitionDecoder(i).metadata().getRowGroupCount();
                    try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                        footerDecoder.of(reader.getParquetAddr(i), reader.getParquetFileSize(i), MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        unusedAfterSecond = footerDecoder.metadata().getUnusedBytes();
                    }
                    Assert.assertTrue(
                            "row group count should be >= previous after second O3 update, was " + rowGroupCountAfterFirst + ", got " + rowGroupCountAfterSecond,
                            rowGroupCountAfterSecond >= rowGroupCountAfterFirst
                    );
                    Assert.assertTrue(
                            "unused_bytes should grow after second update, got " + unusedAfterSecond,
                            unusedAfterSecond > unusedAfterFirst
                    );
                }
            }

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            11\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            12\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testNativeToParquetRoundTripColumnTopEqualsRowCount() throws Exception {
        // When a column is added after all rows already exist, the native
        // partition starts with column_top == partitionRowCount. Converting to
        // parquet must materialize those nulls into the parquet chunks and
        // normalize the parquet-side top to 0. The parquet→native round-trip
        // must then rebuild full-size native null columns rather than treating
        // the all-null parquet chunks as empty data.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add columns after all data exists — column_top == 4 == partitionRowCount.
            // Cover various types: fixed (LONG), var-size (VARCHAR), and SYMBOL.
            execute("ALTER TABLE x ADD COLUMN n LONG");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            drainWalQueue();

            final String expected = """
                    ts\tx\tn\tv\ts
                    2020-01-01T00:00:00.000000Z\t1\tnull\t\t
                    2020-01-01T01:00:00.000000Z\t2\tnull\t\t
                    2020-01-01T02:00:00.000000Z\t3\tnull\t\t
                    2020-01-01T03:00:00.000000Z\t4\tnull\t\t
                    """;

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // native → parquet → native round-trip
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertPmAllNullChunkUsesZeroPointers();
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // Second round-trip to verify the normalized representation remains stable.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testNativeToParquetRoundTripMultiDimArrayNulls() throws Exception {
        // A DOUBLE[][] column whose values are arrays of null elements
        // (e.g., [[null,null],[null,null]]) must survive a native→parquet→native
        // round-trip without collapsing into plain NULL.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column with column top = 4
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows where 'a' is an array of null elements
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // native → parquet → native round-trip
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testNativeToParquetRoundTripNonExistentColumn() throws Exception {
        // When a column is added on a later partition, earlier partitions have
        // no native column files for it (getColumnTop returns -1). Converting
        // such a partition to parquet must synthesize all-null chunks, and the
        // parquet→native round-trip must materialize full native null columns
        // from those chunks even though the parquet-side top is normalized to 0.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            // Insert into a second partition so that it becomes the active one
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', 10)");
            drainWalQueue();

            // Add columns while 2020-01-02 is the active partition.
            // For 2020-01-01, getColumnTop returns -1 (column does not exist).
            execute("ALTER TABLE x ADD COLUMN n LONG");
            execute("ALTER TABLE x ADD COLUMN v VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            drainWalQueue();

            final String expected01 = """
                    ts\tx\tn\tv\ts
                    2020-01-01T00:00:00.000000Z\t1\tnull\t\t
                    2020-01-01T01:00:00.000000Z\t2\tnull\t\t
                    2020-01-01T02:00:00.000000Z\t3\tnull\t\t
                    2020-01-01T03:00:00.000000Z\t4\tnull\t\t
                    """;

            assertQuery("SELECT * FROM x WHERE ts < '2020-01-02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected01);

            // native → parquet → native round-trip for 2020-01-01
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertQuery("SELECT * FROM x WHERE ts < '2020-01-02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected01);

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertQuery("SELECT * FROM x WHERE ts < '2020-01-02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected01);

            // Second round-trip to verify the normalized representation remains stable.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertQuery("SELECT * FROM x WHERE ts < '2020-01-02'")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expected01);
        });
    }

    @Test
    public void testNonDedupSameTimestampSpansThreeRowGroups() throws Exception {
        // Non-deduplicating counterpart of testDedupSameTimestampSpansThreeRowGroups with
        // identical data: 12 rows all sharing timestamp 12:00 are written as 3 row groups
        // (row group size 4), then the same 12 rows are re-inserted as out-of-order data.
        //
        // Without dedup, computeMergeActions must NOT coalesce the tied row groups (the
        // coalesceBoundaryTies gate is false), so the O3 batch merges through the ordinary
        // per-row-group path and no full-partition rewrite is forced. All 24 rows must
        // survive: every (ts, s) appears exactly twice.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    "create table tab (ts timestamp, s symbol index) " +
                            "timestamp(ts) partition by day format parquet wal"
            );

            // Commit 1: 12 rows at a single timestamp -> 3 row groups, all [12:00, 12:00].
            execute(
                    "insert into tab " +
                            "select '2020-02-27T12:00:00.000000Z'::TIMESTAMP, ('sym' || (x - 1)) " +
                            "from long_sequence(12)"
            );
            drainWalQueue();
            assertQuery("select count() from tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n12\n");

            // Commit 2: re-insert the same 12 rows at the same timestamp as out-of-order
            // data. No dedup, so every row is kept: the count doubles to 24.
            execute(
                    "insert into tab " +
                            "select '2020-02-27T12:00:00.000000Z'::TIMESTAMP, ('sym' || (x - 1)) " +
                            "from long_sequence(12)"
            );
            drainWalQueue();

            assertQuery("select count() from tab")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n24\n");
            // 12 distinct (ts, s) groups, each appearing exactly twice: max == min == 2.
            // (max()/min() over the materialized group-by avoids the count() fast-path bug
            // that a filtered count subquery trips -- questdb/questdb#7201.)
            assertQuery("select max(c) mx, min(c) mn from (select ts, s, count() c from tab)")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("mx\tmn\n2\t2\n");
        });
    }

    @Test
    public void testO3AfterAddArrayColumnMultipleRowGroups() throws Exception {
        // Exercises the collect_leaf_path() code path in Rust: when a DOUBLE[]
        // column is added after a partition is already in Parquet format, the
        // O3 merge calls copyRowGroupWithNullColumns() for untouched row groups.
        // The null column chunk for an array column uses a nested LIST schema
        // (col_name / list / element), and path_in_schema must contain the full
        // root-to-leaf path, not just [col_name].
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add DOUBLE[] column. Parquet file has 2 columns (x, ts), table now has 3.
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[]");
            drainWalQueue();

            // O3 insert overlapping first row group (00:00-03:00).
            // RG0 is merged (null fill for a), RG1 is copied via
            // copyRowGroupWithNullColumns (null column chunk for a — nested LIST).
            execute(
                    """
                            INSERT INTO x(x, a, ts) VALUES
                            (9, ARRAY[1.5, 2.5], '2020-01-01T00:30:00.000Z'),
                            (10, ARRAY[3.5], '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ta
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            9\t2020-01-01T00:30:00.000000Z\t[1.5,2.5]
                            2\t2020-01-01T01:00:00.000000Z\tnull
                            10\t2020-01-01T01:30:00.000000Z\t[3.5]
                            3\t2020-01-01T02:00:00.000000Z\tnull
                            4\t2020-01-01T03:00:00.000000Z\tnull
                            5\t2020-01-01T04:00:00.000000Z\tnull
                            6\t2020-01-01T05:00:00.000000Z\tnull
                            7\t2020-01-01T06:00:00.000000Z\tnull
                            8\t2020-01-01T07:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testO3AfterAddArrayColumnNestedListEncoding() throws Exception {
        // Reproduces C2: missing repetition levels in null column chunks for
        // nested LIST schema. When rawArrayEncoding is disabled, DOUBLE[] is
        // encoded as a nested LIST group (col / list / element) with
        // max_rep_level > 0. The null column chunk generated by
        // copyRowGroupWithNullColumns must include both repetition and
        // definition level sections; otherwise split_buffer_v1 misparses
        // the page and produces garbage or crashes.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_RAW_ARRAY_ENCODING_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add DOUBLE[] column. With rawArrayEncoding=false the parquet
            // schema uses nested LIST: a / list / element.
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[]");
            drainWalQueue();

            // O3 insert overlapping first row group (00:00-03:00).
            // RG0 is merged (null fill for a), RG1 is copied via
            // copyRowGroupWithNullColumns which must emit correct rep+def
            // levels for the nested LIST null column chunk.
            execute(
                    """
                            INSERT INTO x(x, a, ts) VALUES
                            (9, ARRAY[1.5, 2.5], '2020-01-01T00:30:00.000Z'),
                            (10, ARRAY[3.5], '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ta
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            9\t2020-01-01T00:30:00.000000Z\t[1.5,2.5]
                            2\t2020-01-01T01:00:00.000000Z\tnull
                            10\t2020-01-01T01:30:00.000000Z\t[3.5]
                            3\t2020-01-01T02:00:00.000000Z\tnull
                            4\t2020-01-01T03:00:00.000000Z\tnull
                            5\t2020-01-01T04:00:00.000000Z\tnull
                            6\t2020-01-01T05:00:00.000000Z\tnull
                            7\t2020-01-01T06:00:00.000000Z\tnull
                            8\t2020-01-01T07:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testO3AfterAddColumnMultipleRowGroups() throws Exception {
        // Small row group size -> multiple row groups. Schema mismatch forces
        // rewrite, exercising copyRowGroupWithNullColumns() for untouched RGs.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (1, 'a', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, ts) VALUES (100, 'd', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // O3 insert overlapping first row group (00:00-03:00).
            // RG0 is merged (null fill for y), RG1 is copied via
            // copyRowGroupWithNullColumns (null column chunk for y).
            execute(
                    """
                            INSERT INTO x(x, s, y, ts) VALUES
                            (9, 'b', 1.5, '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 2.5, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\ts\tts\ty
                            1\ta\t2020-01-01T00:00:00.000000Z\tnull
                            9\tb\t2020-01-01T00:30:00.000000Z\t1.5
                            2\tb\t2020-01-01T01:00:00.000000Z\tnull
                            10\tc\t2020-01-01T01:30:00.000000Z\t2.5
                            3\ta\t2020-01-01T02:00:00.000000Z\tnull
                            4\tc\t2020-01-01T03:00:00.000000Z\tnull
                            5\tb\t2020-01-01T04:00:00.000000Z\tnull
                            6\ta\t2020-01-01T05:00:00.000000Z\tnull
                            7\tc\t2020-01-01T06:00:00.000000Z\tnull
                            8\tb\t2020-01-01T07:00:00.000000Z\tnull
                            100\td\t2020-01-02T00:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testO3AfterAddColumnRewriteMode() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (1, 'a', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', '2020-01-01T06:00:00.000Z'),
                            (3, 'a', '2020-01-01T12:00:00.000Z'),
                            (4, 'c', '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, ts) VALUES (100, 'd', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add a new column after converting to Parquet.
            // The Parquet file has 3 columns (x, s, ts), the table now has 4 (x, s, ts, y).
            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // O3 insert into the parquet partition.
            // Single row group → always REWRITE. Original rows get NULL for y.
            execute(
                    """
                            INSERT INTO x(x, s, y, ts) VALUES
                            (5, 'b', 1.5, '2020-01-01T03:00:00.000Z'),
                            (6, 'a', 2.5, '2020-01-01T09:00:00.000Z'),
                            (7, 'c', 3.5, '2020-01-01T15:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\ts\tts\ty
                            1\ta\t2020-01-01T00:00:00.000000Z\tnull
                            5\tb\t2020-01-01T03:00:00.000000Z\t1.5
                            2\tb\t2020-01-01T06:00:00.000000Z\tnull
                            6\ta\t2020-01-01T09:00:00.000000Z\t2.5
                            3\ta\t2020-01-01T12:00:00.000000Z\tnull
                            7\tc\t2020-01-01T15:00:00.000000Z\t3.5
                            4\tc\t2020-01-01T18:00:00.000000Z\tnull
                            100\td\t2020-01-02T00:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testO3AfterAddColumnSingleRowPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (1, '2020-01-01T12:00:00.000Z')");
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // O3 insert into the single-row parquet partition.
            execute("INSERT INTO x(x, y, ts) VALUES (2, 1.5, '2020-01-01T06:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ty
                            2\t2020-01-01T06:00:00.000000Z\t1.5
                            1\t2020-01-01T12:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testO3AfterAddDedupKeyColumn() throws Exception {
        // Reproduces: dedup key column added after parquet partition exists.
        // O3 merge into that partition enters the dedup code path, where
        // the new column is a dedup key but is missing from the parquet
        // file (decodeIdx == -1), causing an assertion failure or crash.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            // Second partition keeps 2020-01-01 as a non-active partition.
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add a new column and make it a dedup key.
            // Parquet file has (x, ts); table now has (x, ts, y).
            execute("ALTER TABLE x ADD COLUMN y INT");
            execute("ALTER TABLE x DEDUP ENABLE UPSERT KEYS(ts, y)");
            drainWalQueue();

            // O3 insert with timestamps that duplicate existing rows.
            // Triggers dedup merge, which iterates dedup key columns.
            // Column y is a dedup key but does not exist in the parquet file.
            execute(
                    """
                            INSERT INTO x(x, y, ts) VALUES
                            (5, 10, '2020-01-01T00:00:00.000Z'),
                            (6, 20, '2020-01-01T06:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Old rows have y=NULL, new rows have y=10/20.
            // Dedup keys (ts, y) differ, so all rows are kept.
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ty
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            5\t2020-01-01T00:00:00.000000Z\t10
                            2\t2020-01-01T06:00:00.000000Z\tnull
                            6\t2020-01-01T06:00:00.000000Z\t20
                            3\t2020-01-01T12:00:00.000000Z\tnull
                            4\t2020-01-01T18:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testO3AfterAddRequiredSymbolColumnMultipleRowGroups() throws Exception {
        // Reproduces: Required Symbol null chunk encoding mismatch.
        //
        // When a SYMBOL column is added after a parquet partition exists and
        // only non-null values are inserted, the symbol map's null flag stays
        // false → the target schema marks it as Required (no def levels).
        // copy_row_group_with_null_columns must generate a Required null chunk
        // (zero-filled Int32), but generate_required_zero_page did not handle
        // ColumnTypeTag::Symbol, falling through to generate_optional_null_page
        // — a page with RLE definition levels in a Required column, producing
        // a malformed parquet file.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups (RG0: rows 1-4, RG1: rows 5-8).
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add a SYMBOL column after the parquet partition exists.
            execute("ALTER TABLE x ADD COLUMN s SYMBOL");
            drainWalQueue();

            // O3 insert with non-null symbol values into RG0's time range.
            // null flag stays false -> Required in target schema.
            // RG0: merged with new rows (s filled for all rows).
            // RG1: copied via copy_row_group_with_null_columns — needs a
            //      Required null chunk for 's'. Bug: Symbol not handled in
            //      generate_required_zero_page -> Optional page for Required column.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (9, 'a', '2020-01-01T00:30:00.000Z'),
                            (10, 'b', '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Verify new O3 rows have correct symbol values.
            assertQuery("SELECT * FROM x WHERE x IN (9, 10) ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ts
                            9\t2020-01-01T00:30:00.000000Z\ta
                            10\t2020-01-01T01:30:00.000000Z\tb
                            """);

            // Read from the copied row group (RG1) to exercise the symbol
            // null chunk. Before the fix, the null chunk used Plain encoding
            // which the symbol decoder does not support, causing a decode error.
            // Old rows have no symbol data -> NULL (empty).
            assertQuery("SELECT * FROM x WHERE x >= 5 AND x <= 8 ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ts
                            5\t2020-01-01T04:00:00.000000Z\t
                            6\t2020-01-01T05:00:00.000000Z\t
                            7\t2020-01-01T06:00:00.000000Z\t
                            8\t2020-01-01T07:00:00.000000Z\t
                            """);
        });
    }

    @Test
    public void testO3AfterMultipleAddColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Add two columns after conversion. The parquet file has 2 columns,
            // the table now has 4.
            execute("ALTER TABLE x ADD COLUMN a DOUBLE");
            execute("ALTER TABLE x ADD COLUMN b VARCHAR");
            drainWalQueue();

            // O3 insert with both new columns.
            execute(
                    """
                            INSERT INTO x(x, a, b, ts) VALUES
                            (5, 1.5, 'hello', '2020-01-01T03:00:00.000Z'),
                            (6, 2.5, 'world', '2020-01-01T09:00:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ta\tb
                            1\t2020-01-01T00:00:00.000000Z\tnull\t
                            5\t2020-01-01T03:00:00.000000Z\t1.5\thello
                            2\t2020-01-01T06:00:00.000000Z\tnull\t
                            6\t2020-01-01T09:00:00.000000Z\t2.5\tworld
                            3\t2020-01-01T12:00:00.000000Z\tnull\t
                            4\t2020-01-01T18:00:00.000000Z\tnull\t
                            100\t2020-01-02T00:00:00.000000Z\tnull\t
                            """);
        });
    }

    @Test
    public void testO3IntoParquetAfterColumnTopAndConversionCycles() throws Exception {
        // Reproduces the DOUBLE[][] data corruption seen in WalWriterFuzzTest
        // with seeds 286709679787041L, 1772793547818L.
        //
        // Key sequence:
        // 1. Create partition with initial rows (no array column).
        // 2. ADD COLUMN arr DOUBLE[][] → column_top = initial rows.
        // 3. Insert rows with array data.
        // 4. Convert to parquet (column_top > 0 in QDB metadata).
        // 5. Convert back to native (decoder materializes all rows).
        // 6. Insert more rows.
        // 7. Convert to parquet (column_top = 0 now).
        // 8. O3 insert → merge into parquet.
        // 9. Verify array values are preserved.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert initial rows into partition 2020-01-01 (timestamps every minute)
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 0; i < 100; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i).append(')');
            }
            execute(sb.toString());

            // Insert a row in the next partition so 2020-01-01 is non-active
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', 999)");
            drainWalQueue();

            // Add DOUBLE[][] column → column_top = 100 for partition 2020-01-01
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with actual DOUBLE[][] data (timestamps after existing max)
            sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 100; i < 150; i++) {
                if (i > 100) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify baseline
            String expected100 = "ts\tx\ta\n";
            StringBuilder expectedSb = new StringBuilder(expected100);
            for (int i = 0; i < 100; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i).append("\tnull\n");
            }
            for (int i = 100; i < 150; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i)
                        .append("\t[[null,null,null],[null,null,null],[null,null,null]]\n");
            }
            assertQuery("SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedSb.toString());

            // Step 4: Convert to parquet (column_top=100 for arr)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertQuery("SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedSb.toString());

            // Step 5: Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();
            assertQuery("SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedSb.toString());

            // Step 6: Insert more rows with array data
            sb = new StringBuilder();
            sb.append("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 150; i < 200; i++) {
                if (i > 150) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Update expected
            for (int i = 150; i < 200; i++) {
                expectedSb.append("2020-01-01T").append(String.format("%02d:%02d", i / 60, i % 60))
                        .append(":00.000000Z\t").append(i)
                        .append("\t[[null,null,null],[null,null,null],[null,null,null]]\n");
            }
            assertQuery("SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedSb.toString());

            // Step 7: Convert to parquet (column_top = 0 now)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertQuery("SELECT * FROM x WHERE ts >= '2020-01-01' AND ts < '2020-01-02' ORDER BY ts")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns(expectedSb.toString());

            // Step 8: O3 insert into the parquet partition
            // Insert rows with timestamps that fall BEFORE the existing max,
            // triggering O3 merge with the parquet partition.
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T00:00:30.000Z', 1000, ARRAY[[42.0, 43.0]]),
                            ('2020-01-01T01:30:30.000Z', 1001, ARRAY[[44.0]]),
                            ('2020-01-01T02:15:30.000Z', 1002, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])
                            """
            );
            drainWalQueue();

            // Step 9: Verify all data
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Spot-check specific array values that should NOT be null
            assertQuery("SELECT * FROM x WHERE x = 100")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T01:40:00.000000Z\t100\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 149")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T02:29:00.000000Z\t149\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 150")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T02:30:00.000000Z\t150\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 1002")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T02:15:30.000000Z\t1002\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 1000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:00:30.000000Z\t1000\t[[42.0,43.0]]
                            """);
        });
    }

    @Test
    public void testO3IntoParquetAfterColumnTopMultipleO3Merges() throws Exception {
        // Closer to the actual fuzz failure: exercises multiple O3 merges into
        // parquet after ADD COLUMN with column_top and parquet↔native cycles.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert rows with timestamps every second (large enough to have
            // substantial column_top when column is added)
            StringBuilder sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 0; i < 500; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Timestamps every 2 seconds to leave gaps for O3
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i).append(')');
            }
            execute(sb.toString());

            // Row in next partition
            execute("INSERT INTO x(ts, x) VALUES ('2020-01-02T00:00:00.000Z', -1)");
            drainWalQueue();

            // Convert to parquet, then back to native (cycle 1)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            // Append more rows
            sb = new StringBuilder("INSERT INTO x(ts, x) VALUES\n");
            for (int i = 500; i < 700; i++) {
                if (i > 500) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i).append(')');
            }
            execute(sb.toString());
            drainWalQueue();

            // ADD COLUMN with column_top = 700
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with actual DOUBLE[][] data
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 700; i < 900; i++) {
                if (i > 700) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Convert to parquet (column_top = 700)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            // Append more rows
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 900; i < 1100; i++) {
                if (i > 900) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2) / 60, (i * 2) % 60))
                        .append(".000Z', ").append(i)
                        .append(", ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Convert to parquet again (column_top = 0)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert #1: overlaps with existing data
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 0; i < 200; i++) {
                if (i > 0) {
                    sb.append(",\n");
                }
                // Odd seconds to interleave with even seconds
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2 + 1) / 60, (i * 2 + 1) % 60))
                        .append(".000Z', ").append(10_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // O3 insert #2: more interleaving
            sb = new StringBuilder("INSERT INTO x(ts, x, a) VALUES\n");
            for (int i = 200; i < 500; i++) {
                if (i > 200) {
                    sb.append(",\n");
                }
                sb.append("('2020-01-01T00:").append(String.format("%02d:%02d", (i * 2 + 1) / 60, (i * 2 + 1) % 60))
                        .append(".000Z', ").append(10_000 + i)
                        .append(", ARRAY[[").append(i).append("]])");
            }
            execute(sb.toString());
            drainWalQueue();

            // Verify
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Check that array values are correct for rows after column_top
            assertQuery("SELECT * FROM x WHERE x = 700")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:23:20.000000Z\t700\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 899")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:29:58.000000Z\t899\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            assertQuery("SELECT * FROM x WHERE x = 900")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:30:00.000000Z\t900\t[[null,null,null],[null,null,null],[null,null,null]]
                            """);
            // Rows before column_top should be null
            assertQuery("SELECT * FROM x WHERE x = 0")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:00:00.000000Z\t0\tnull
                            """);
            // O3 rows should have their values
            assertQuery("SELECT * FROM x WHERE x = 10000")
                    .noLeakCheck()
                    .timestamp("ts")
                    .returns("""
                            ts\tx\ta
                            2020-01-01T00:00:01.000000Z\t10000\t[[0.0]]
                            """);
        });
    }

    @Test
    public void testO3IntoParquetWithMultiDimArrayNulls() throws Exception {
        // Reproduces a bug where DOUBLE[][] values consisting of arrays of null
        // elements (e.g., [[null,null,null],...]) collapse to plain NULL after
        // an O3 merge into a parquet partition via the Rust updater.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column — existing rows get column_top = 4
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert rows with DOUBLE[][] values that contain null elements
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // Convert to parquet
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // O3 insert into the parquet partition → triggers Rust O3 updater
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T02:30:00.000Z', 10, ARRAY[[null::double, null::double, null::double]])
                            """
            );
            drainWalQueue();

            final String expectedAfterO3 = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T02:30:00.000000Z\t10\t[[null,null,null]]
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    """;

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expectedAfterO3);
        });
    }

    @Test
    public void testO3MergeAfterDropAllNonTimestampColumns() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, b DOUBLE, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, b, ts) VALUES
                            (1, 1.1, '2020-01-01T00:00:00.000Z'),
                            (2, 2.2, '2020-01-01T06:00:00.000Z'),
                            (3, 3.3, '2020-01-01T12:00:00.000Z'),
                            (4, 4.4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, b, ts) VALUES (100, 99.9, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Drop all non-timestamp columns.
            execute("ALTER TABLE x DROP COLUMN a");
            execute("ALTER TABLE x DROP COLUMN b");
            drainWalQueue();

            // O3 insert into the parquet partition — only the timestamp column remains.
            execute("INSERT INTO x(ts) VALUES ('2020-01-01T03:00:00.000Z')");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            ts
                            2020-01-01T00:00:00.000000Z
                            2020-01-01T03:00:00.000000Z
                            2020-01-01T06:00:00.000000Z
                            2020-01-01T12:00:00.000000Z
                            2020-01-01T18:00:00.000000Z
                            2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testO3MergeAfterDropColumnWrongTimestampIndex() throws Exception {
        // Two rounds of O3 merge exercise the wrong-timestamp-index bug.
        // Round 1: DROP COLUMN 'a' (before ts) + ADD COLUMN 'b' causes
        //   schema change → rewrite. The rewritten parquet file has compacted
        //   columns: x(0), ts(1), b(2). Writer metadata keeps timestampIndex=2
        //   (stable), but ts is now at parquet position 1.
        // Round 2: second O3 merge reads the rewritten file.
        //   timestampIndex=2, timestampParquetIdx=1 — they differ.
        //   Reading row group stats with timestampIndex fetches stats for
        //   parquet column 2 (column 'b', not the timestamp), producing
        //   incorrect row group bounds and a type mismatch error.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (a INT, x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(a, x, ts) VALUES
                            (1, 10, '2020-01-01T00:00:00.000Z'),
                            (2, 20, '2020-01-01T01:00:00.000Z'),
                            (3, 30, '2020-01-01T02:00:00.000Z'),
                            (4, 40, '2020-01-01T03:00:00.000Z'),
                            (5, 50, '2020-01-01T04:00:00.000Z'),
                            (6, 60, '2020-01-01T05:00:00.000Z'),
                            (7, 70, '2020-01-01T06:00:00.000Z'),
                            (8, 80, '2020-01-01T07:00:00.000Z'),
                            (9, 90, '2020-01-01T08:00:00.000Z'),
                            (10, 100, '2020-01-01T09:00:00.000Z'),
                            (11, 110, '2020-01-01T10:00:00.000Z'),
                            (12, 120, '2020-01-01T11:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(a, x, ts) VALUES (1000, 9999, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Parquet file: a(0), x(1), ts(2). timestampIndex=2, timestampParquetIdx=2.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Drop column 'a' (timestampIndex stays 2) and add column 'b' so
            // the rewritten parquet file has ≥3 columns and timestampIndex=2
            // points at 'b' instead of going out of bounds.
            execute("ALTER TABLE x DROP COLUMN a");
            execute("ALTER TABLE x ADD COLUMN b LONG");
            drainWalQueue();

            // Round 1: O3 insert triggers merge with schema change → rewrite.
            // Rewritten parquet: x(0), ts(1), b(2). timestampParquetIdx=1.
            execute(
                    """
                            INSERT INTO x(x, b, ts) VALUES
                            (100, 1, '2020-01-01T04:30:00.000Z'),
                            (101, 2, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after round 1",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            // Round 2: O3 insert against the rewritten parquet file.
            // timestampIndex=2, timestampParquetIdx=1 -- they now differ.
            // The row group stat lookup must use timestampParquetIdx, not
            // timestampIndex; reading parquet column 2 ('b', LONG) as TIMESTAMP
            // would produce a type mismatch and suspend the table.
            execute(
                    """
                            INSERT INTO x(x, b, ts) VALUES
                            (200, 3, '2020-01-01T06:30:00.000Z'),
                            (201, 4, '2020-01-01T07:30:00.000Z')
                            """
            );
            drainWalQueue();

            Assert.assertFalse(
                    "table suspended after round 2",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\tb
                            10\t2020-01-01T00:00:00.000000Z\tnull
                            20\t2020-01-01T01:00:00.000000Z\tnull
                            30\t2020-01-01T02:00:00.000000Z\tnull
                            40\t2020-01-01T03:00:00.000000Z\tnull
                            50\t2020-01-01T04:00:00.000000Z\tnull
                            100\t2020-01-01T04:30:00.000000Z\t1
                            60\t2020-01-01T05:00:00.000000Z\tnull
                            101\t2020-01-01T05:30:00.000000Z\t2
                            70\t2020-01-01T06:00:00.000000Z\tnull
                            200\t2020-01-01T06:30:00.000000Z\t3
                            80\t2020-01-01T07:00:00.000000Z\tnull
                            201\t2020-01-01T07:30:00.000000Z\t4
                            90\t2020-01-01T08:00:00.000000Z\tnull
                            100\t2020-01-01T09:00:00.000000Z\tnull
                            110\t2020-01-01T10:00:00.000000Z\tnull
                            120\t2020-01-01T11:00:00.000000Z\tnull
                            9999\t2020-01-02T00:00:00.000000Z\tnull
                            """);
        });
    }

    @Test
    public void testO3MergeWithStatisticsDisabled() throws Exception {
        // Disable parquet statistics and use small row groups to produce multiple row groups.
        // O3 merge reads row group min/max timestamps from _pm; when parquet
        // statistics are absent the stats are missing from _pm and the merge
        // must still produce correct results without crashing or corrupting data.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_STATISTICS_ENABLED, false);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z'),
                            (9, '2020-01-01T08:00:00.000Z'),
                            (10, '2020-01-01T09:00:00.000Z'),
                            (11, '2020-01-01T10:00:00.000Z'),
                            (12, '2020-01-01T11:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (1000, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 12 rows with row group size 4 → 3 row groups, no statistics.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert into the middle row group's time range.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (100, '2020-01-01T04:30:00.000Z'),
                            (101, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Table must not be suspended.
            Assert.assertFalse(
                    "table should not be suspended after O3 merge with statistics disabled",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            100\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            101\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            9\t2020-01-01T08:00:00.000000Z
                            10\t2020-01-01T09:00:00.000000Z
                            11\t2020-01-01T10:00:00.000000Z
                            12\t2020-01-01T11:00:00.000000Z
                            1000\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testO3MergeWithVarSizeColumnTop() throws Exception {
        // Small row group size so the column-top covers entire row groups.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 8 rows without the STRING column.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Add STRING column. column_top = 8 for this partition.
            execute("ALTER TABLE x ADD COLUMN s STRING");
            drainWalQueue();

            // Insert 4 more rows with s values.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (9, 'abc', '2020-01-01T08:00:00.000Z'),
                            (10, 'def', '2020-01-01T09:00:00.000Z'),
                            (11, 'ghi', '2020-01-01T10:00:00.000Z'),
                            (12, 'jkl', '2020-01-01T11:00:00.000Z')
                            """
            );
            drainWalQueue();

            // 12 rows with row group size 4 → 3 row groups.
            // RG0 (rows 0-3): s is entirely topped (column_top=8 ≥ 4)
            // RG1 (rows 4-7): s is entirely topped (column_top=8 ≥ 8)
            // RG2 (rows 8-11): s has actual data
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // O3 insert targeting RG0 (timestamp range 00:00-03:00).
            // The merge decodes RG0 where s has column_top → null aux_ptr.
            // Exercises the null source buffer allocation for var-size columns
            // (STRING null markers need a non-zero data buffer).
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (13, 'new', '2020-01-01T00:30:00.000Z')
                            """
            );
            drainWalQueue();

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ts
                            1\t2020-01-01T00:00:00.000000Z\t
                            13\t2020-01-01T00:30:00.000000Z\tnew
                            2\t2020-01-01T01:00:00.000000Z\t
                            3\t2020-01-01T02:00:00.000000Z\t
                            4\t2020-01-01T03:00:00.000000Z\t
                            5\t2020-01-01T04:00:00.000000Z\t
                            6\t2020-01-01T05:00:00.000000Z\t
                            7\t2020-01-01T06:00:00.000000Z\t
                            8\t2020-01-01T07:00:00.000000Z\t
                            9\t2020-01-01T08:00:00.000000Z\tabc
                            10\t2020-01-01T09:00:00.000000Z\tdef
                            11\t2020-01-01T10:00:00.000000Z\tghi
                            12\t2020-01-01T11:00:00.000000Z\tjkl
                            100\t2020-01-02T00:00:00.000000Z\t
                            """);
        });
    }

    @Test
    public void testParquetToNativePreservesColumnTopZeroing() throws Exception {
        // Reproduces a bug where convertPartitionParquetToNative materializes
        // ALL rows (including column_top nulls) but does not zero the
        // column_tops in ColumnVersionWriter. A subsequent native→parquet
        // conversion reads the stale column_top, causing data to shift by
        // column_top positions and corrupting array values.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, x INT)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );

            // Insert 4 rows into partition 2020-01-01
            execute(
                    """
                            INSERT INTO x(ts, x) VALUES
                            ('2020-01-01T00:00:00.000Z', 1),
                            ('2020-01-01T01:00:00.000Z', 2),
                            ('2020-01-01T02:00:00.000Z', 3),
                            ('2020-01-01T03:00:00.000Z', 4)
                            """
            );
            drainWalQueue();

            // Add a DOUBLE[][] column → column_top = 4 for the existing rows
            execute("ALTER TABLE x ADD COLUMN a DOUBLE[][]");
            drainWalQueue();

            // Insert 4 more rows with actual DOUBLE[][] data
            execute(
                    """
                            INSERT INTO x(ts, x, a) VALUES
                            ('2020-01-01T04:00:00.000Z', 5, ARRAY[[null::double, null::double, null::double], [null::double, null::double, null::double], [null::double, null::double, null::double]]),
                            ('2020-01-01T05:00:00.000Z', 6, ARRAY[[1.5, null::double], [null::double, 2.5]]),
                            ('2020-01-01T06:00:00.000Z', 7, ARRAY[[null::double]]),
                            ('2020-01-01T07:00:00.000Z', 8, ARRAY[[3.0, 4.0]])
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tx\ta
                    2020-01-01T00:00:00.000000Z\t1\tnull
                    2020-01-01T01:00:00.000000Z\t2\tnull
                    2020-01-01T02:00:00.000000Z\t3\tnull
                    2020-01-01T03:00:00.000000Z\t4\tnull
                    2020-01-01T04:00:00.000000Z\t5\t[[null,null,null],[null,null,null],[null,null,null]]
                    2020-01-01T05:00:00.000000Z\t6\t[[1.5,null],[null,2.5]]
                    2020-01-01T06:00:00.000000Z\t7\t[[null]]
                    2020-01-01T07:00:00.000000Z\t8\t[[3.0,4.0]]
                    """;

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // Convert to parquet (encodes with column_top=4)
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // Convert back to native -- decoder materializes ALL rows, so
            // the native files contain every row. CONVERT PARTITION TO NATIVE
            // must zero column_top in ColumnVersionWriter; a stale column_top
            // of 4 would cause the subsequent re-encode to skip the first 4
            // rows of the native file.
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // Re-encode to parquet. A stale column_top of 4 would make the
            // encoder read from offset 0 in the native file but skip the
            // first 4 rows, shifting DOUBLE[][] data by 4 positions.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testParquetToNativeWithColumnTops() throws Exception {
        // Tests that converting parquet→native correctly zeroes column tops.
        // The parquet decoder materializes NULLs for column-top rows, so the
        // native files contain ALL rows. Without zeroing, the reader offsets
        // into the file incorrectly, producing wrong data.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (ts TIMESTAMP, v VARCHAR, s SYMBOL)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(ts, v, s) VALUES
                            ('2020-01-01T00:00:00.000Z', 'foo', 'a'),
                            ('2020-01-01T01:00:00.000Z', 'bar', 'b'),
                            ('2020-01-01T02:00:00.000Z', 'baz', 'a'),
                            ('2020-01-01T03:00:00.000Z', 'qux', 'c')
                            """
            );
            drainWalQueue();

            // Add columns AFTER data exists, creating non-zero column tops
            execute("ALTER TABLE x ADD COLUMN n INT");
            execute("ALTER TABLE x ADD COLUMN v2 VARCHAR");
            execute("ALTER TABLE x ADD COLUMN s2 SYMBOL");
            drainWalQueue();

            // Insert more rows so the new columns have some non-null data
            execute(
                    """
                            INSERT INTO x(ts, v, s, n, v2, s2) VALUES
                            ('2020-01-01T04:00:00.000Z', 'aaa', 'd', 42, 'hello', 'x'),
                            ('2020-01-01T05:00:00.000Z', 'bbb', 'e', 99, 'world', 'y')
                            """
            );
            drainWalQueue();

            final String expected = """
                    ts\tv\ts\tn\tv2\ts2
                    2020-01-01T00:00:00.000000Z\tfoo\ta\tnull\t\t
                    2020-01-01T01:00:00.000000Z\tbar\tb\tnull\t\t
                    2020-01-01T02:00:00.000000Z\tbaz\ta\tnull\t\t
                    2020-01-01T03:00:00.000000Z\tqux\tc\tnull\t\t
                    2020-01-01T04:00:00.000000Z\taaa\td\t42\thello\tx
                    2020-01-01T05:00:00.000000Z\tbbb\te\t99\tworld\ty
                    """;

            // Verify data before conversion
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // Convert to parquet
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Verify data in parquet format
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);

            // Convert back to native
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            // Verify data after converting back to native
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns(expected);
        });
    }

    @Test
    public void testParquetToNativeWithDeletedColumns() throws Exception {
        // Tests that converting parquet→native correctly maps columns by writer ID
        // when columns have been deleted, causing gaps in the table column indices.
        assertMemoryLeak(() -> {
            // Create table with several column types including VARCHAR
            execute(
                    """
                            CREATE TABLE x (x INT, v VARCHAR, s SYMBOL, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, v, s, ts) VALUES
                            (1, 'foo', 'a', '2020-01-01T00:00:00.000Z'),
                            (2, 'bar', 'b', '2020-01-01T01:00:00.000Z'),
                            (3, 'baz', 'a', '2020-01-01T02:00:00.000Z'),
                            (4, 'qux', 'c', '2020-01-01T03:00:00.000Z')
                            """
            );
            drainWalQueue();

            // Case 1: remove a column BEFORE converting to parquet.
            // The parquet file won't contain 'x' and its sequential column indices
            // will differ from the table column indices.
            execute("ALTER TABLE x DROP COLUMN x");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Convert back to native. The conversion must use table column IDs
            // (not parquet sequential indices) for native file naming; otherwise
            // the rewritten partition cannot be opened against the table metadata.
            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            v\ts\tts
                            foo\ta\t2020-01-01T00:00:00.000000Z
                            bar\tb\t2020-01-01T01:00:00.000000Z
                            baz\ta\t2020-01-01T02:00:00.000000Z
                            qux\tc\t2020-01-01T03:00:00.000000Z
                            """);

            // Case 2: convert to parquet again, then remove a column AFTER
            // conversion, then convert back. The parquet contains the column
            // but the table metadata marks it deleted, shifting sequential indices.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x DROP COLUMN s");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            v\tts
                            foo\t2020-01-01T00:00:00.000000Z
                            bar\t2020-01-01T01:00:00.000000Z
                            baz\t2020-01-01T02:00:00.000000Z
                            qux\t2020-01-01T03:00:00.000000Z
                            """);

            // Case 3: convert to parquet, then rename a VARCHAR column, then convert back.
            // The parquet stores the old column name but openPartition expects the new name.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            execute("ALTER TABLE x RENAME COLUMN v TO v_renamed");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO NATIVE LIST '2020-01-01'");
            drainWalQueue();

            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x")));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            v_renamed\tts
                            foo\t2020-01-01T00:00:00.000000Z
                            bar\t2020-01-01T01:00:00.000000Z
                            baz\t2020-01-01T02:00:00.000000Z
                            qux\t2020-01-01T03:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testReadParquetAfterAddColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01', '2020-01-02'");
            drainWalQueue();

            // Add column after conversion. No O3 -- just query.
            execute("ALTER TABLE x ADD COLUMN y DOUBLE");
            drainWalQueue();

            // Insert with the new column into the non-parquet partition.
            execute("INSERT INTO x(x, y, ts) VALUES (5, 1.5, '2020-01-02T06:00:00.000Z')");
            drainWalQueue();

            // Parquet partition returns NULLs for the missing column.
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ty
                            1\t2020-01-01T00:00:00.000000Z\tnull
                            2\t2020-01-01T06:00:00.000000Z\tnull
                            3\t2020-01-01T12:00:00.000000Z\tnull
                            4\t2020-01-01T18:00:00.000000Z\tnull
                            100\t2020-01-02T00:00:00.000000Z\tnull
                            5\t2020-01-02T06:00:00.000000Z\t1.5
                            """);
        });
    }

    @Test
    public void testRewriteAbsoluteUnusedBytesThreshold() throws Exception {
        // Use small row group size to get multiple row groups.
        // Disable ratio check (set to 1.0 = impossible to exceed).
        // Set absolute threshold very low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', 'abc', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', 'jkl', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: unused_bytes = 0, multiple row groups → UPDATE mode.
            // Replaces one row group, accumulating dead space.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (9, 'b', 'mno', '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 'pqr', '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionAfterUpdate = getPartitionNameTxn(partitionTs);

            // Second O3: accumulated unused_bytes > 100 → REWRITE.
            // Rewrite copies unchanged row groups (exercises copy_row_group
            // with dictionary pages from SYMBOL columns).
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (11, 'a', 'stu', '2020-01-01T04:30:00.000Z'),
                            (12, 'c', 'vwx', '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn(partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionAfterUpdate, versionAfterRewrite);

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            9\tb\tmno\t2020-01-01T00:30:00.000000Z
                            2\tb\tbar\t2020-01-01T01:00:00.000000Z
                            10\tc\tpqr\t2020-01-01T01:30:00.000000Z
                            3\ta\tbaz\t2020-01-01T02:00:00.000000Z
                            4\tc\tqux\t2020-01-01T03:00:00.000000Z
                            5\tb\tabc\t2020-01-01T04:00:00.000000Z
                            11\ta\tstu\t2020-01-01T04:30:00.000000Z
                            6\ta\tdef\t2020-01-01T05:00:00.000000Z
                            12\tc\tvwx\t2020-01-01T05:30:00.000000Z
                            7\tc\tghi\t2020-01-01T06:00:00.000000Z
                            8\tb\tjkl\t2020-01-01T07:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRewriteCleanupOnCopyRowGroupFailure() throws Exception {
        // Test that the inner catch cleans up the new directory when copyRowGroup()
        // fails mid-rewrite, leaving the original partition intact.
        //
        // Strategy: override openRW to return a read-only fd for the new data.parquet
        // in rewrite mode. ParquetUpdater.of() succeeds (no writes in the constructor
        // for rewrite mode), but the first write attempt in copyRowGroup() fails with
        // EBADF, exercising the error recovery path.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);

        AtomicBoolean armed = new AtomicBoolean(false);

        FilesFacade dodgyFacade = new TestFilesFacadeImpl() {
            @Override
            public long openRW(LPSZ name, int opts) {
                if (armed.get() && Utf8s.endsWithAscii(name, "data.parquet")) {
                    // Create the file normally, then close it and re-open as
                    // read-only. The Rust writer gets an O_RDONLY fd: init
                    // succeeds (no writes), but copyRowGroup's first write
                    // fails with EBADF.
                    long rwFd = super.openRW(name, opts);
                    super.close(rwFd);
                    return super.openRO(name);
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(dodgyFacade, () -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: UPDATE mode. Merges into rg0, accumulating dead space.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            TableToken tableToken = engine.verifyTableName("x");
            File tableDir = new File(root, tableToken.getDirName());

            // Arm the failure injection before the rewrite-triggering O3.
            armed.set(true);

            // Second O3: accumulated unused_bytes > 100 → REWRITE mode.
            // O3 data hits rg1, so the first merge action is
            // COPY_ROW_GROUP_SLICE for unmodified rg0 → copyRowGroup() fails.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (11, '2020-01-01T04:30:00.000Z'),
                            (12, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Table should be suspended due to O3 error.
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            // The inner catch should have cleaned up the new directory.
            int partDirsAfterFailure = countPartitionDirs(tableDir);
            Assert.assertEquals(
                    "orphaned rewrite directory left on disk",
                    1,
                    partDirsAfterFailure
            );

            // Disarm, resume, and retry.
            armed.set(false);
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();

            // After successful retry, data should be correct.
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            11\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            12\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRewriteCleanupOnUpdateParquetIndexesFailure() throws Exception {
        // Regression test for orphaned directory when updateParquetIndexes() fails
        // in rewrite mode.
        AtomicBoolean armed = new AtomicBoolean(false);
        FilesFacade dodgyFacade = getFilesFacade(armed);

        assertMemoryLeak(dodgyFacade, () -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Single row group → rewrite guaranteed on O3.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // Count 2020-01-01.* partition directories before O3.
            TableToken tableToken = engine.verifyTableName("x");
            File tableDir = new File(root, tableToken.getDirName());

            // Arm the failure injection.
            armed.set(true);

            // O3 insert triggers rewrite. The rewrite itself succeeds but
            // updateParquetIndexes fails because the 2nd openRO returns -1.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (5, '2020-01-01T03:00:00.000Z')
                            """
            );
            drainWalQueue();

            // Table should be suspended due to O3 error.
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            int partDirsAfterFailure = countPartitionDirs(tableDir);
            Assert.assertEquals(
                    "orphaned rewrite directory left on disk",
                    1,
                    partDirsAfterFailure
            );

            // Disarm, resume, and retry.
            armed.set(false);
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();

            // After successful retry, data should be correct.
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            5\t2020-01-01T03:00:00.000000Z
                            2\t2020-01-01T06:00:00.000000Z
                            3\t2020-01-01T12:00:00.000000Z
                            4\t2020-01-01T18:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRewriteResetsUnusedBytesToZero() throws Exception {
        // Use small row group size to get multiple row groups.
        // Set absolute threshold low so the second O3 triggers a rewrite.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, 100);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 -> 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: UPDATE mode. Replaces one row group, accumulating dead space.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Verify unused_bytes > 0 after in-place update.
            long unusedAfterUpdate;
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                        footerDecoder.of(reader.getParquetAddr(i), reader.getParquetFileSize(i), MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        unusedAfterUpdate = footerDecoder.metadata().getUnusedBytes();
                    }
                    Assert.assertTrue("unused_bytes should be > 0 after update, got " + unusedAfterUpdate, unusedAfterUpdate > 0);
                }
            }

            // Second O3: accumulated unused_bytes > 100 -> REWRITE.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (11, '2020-01-01T04:30:00.000Z'),
                            (12, '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            // Verify unused_bytes == 0 after rewrite.
            try (TableReader reader = getReader("x")) {
                for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                    if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                        continue;
                    }
                    reader.openPartition(i);
                    try (ParquetFileDecoder footerDecoder = new ParquetFileDecoder()) {
                        footerDecoder.of(reader.getParquetAddr(i), reader.getParquetFileSize(i), MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        long unusedAfterRewrite = footerDecoder.metadata().getUnusedBytes();
                        Assert.assertEquals("unused_bytes should be 0 after rewrite", 0, unusedAfterRewrite);
                    }
                }
            }

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            11\t2020-01-01T04:30:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            12\t2020-01-01T05:30:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRewriteSingleRowGroup() throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T06:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T12:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T18:00:00.000Z')
                            """
            );
            // Insert into the next day so 2020-01-01 is not the last partition.
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 4 rows with default test row group size (1000) → single row group.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionBeforeO3 = getPartitionNameTxn(partitionTs);

            // O3 insert into the parquet partition.
            // Single row group always triggers a full REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (5, 'b', 'abc', '2020-01-01T03:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T09:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T15:00:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn(partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionBeforeO3, versionAfterRewrite);

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            5\tb\tabc\t2020-01-01T03:00:00.000000Z
                            2\tb\tbar\t2020-01-01T06:00:00.000000Z
                            6\ta\tdef\t2020-01-01T09:00:00.000000Z
                            3\ta\tbaz\t2020-01-01T12:00:00.000000Z
                            7\tc\tghi\t2020-01-01T15:00:00.000000Z
                            4\tc\tqux\t2020-01-01T18:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRewriteUnusedBytesRatioThreshold() throws Exception {
        // Use small row group size to get multiple row groups.
        // Set ratio to 10% to trigger rewrite after one update round.
        // Disable absolute bytes threshold.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "0.1");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, s SYMBOL, v VARCHAR, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (1, 'a', 'foo', '2020-01-01T00:00:00.000Z'),
                            (2, 'b', 'bar', '2020-01-01T01:00:00.000Z'),
                            (3, 'a', 'baz', '2020-01-01T02:00:00.000Z'),
                            (4, 'c', 'qux', '2020-01-01T03:00:00.000Z'),
                            (5, 'b', 'abc', '2020-01-01T04:00:00.000Z'),
                            (6, 'a', 'def', '2020-01-01T05:00:00.000Z'),
                            (7, 'c', 'ghi', '2020-01-01T06:00:00.000Z'),
                            (8, 'b', 'jkl', '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, s, v, ts) VALUES (100, 'd', 'end', '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows with row group size 4 → 2 row groups.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            // First O3: unused_bytes = 0, multiple row groups → UPDATE mode.
            // Replaces one row group, accumulating dead space > 10% of file.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (9, 'b', 'mno', '2020-01-01T00:30:00.000Z'),
                            (10, 'c', 'pqr', '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionAfterUpdate = getPartitionNameTxn(partitionTs);

            // Second O3: unused_bytes / file_size > 0.1 → REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, v, ts) VALUES
                            (11, 'a', 'stu', '2020-01-01T04:30:00.000Z'),
                            (12, 'c', 'vwx', '2020-01-01T05:30:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn(partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionAfterUpdate, versionAfterRewrite);

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\ts\tv\tts
                            1\ta\tfoo\t2020-01-01T00:00:00.000000Z
                            9\tb\tmno\t2020-01-01T00:30:00.000000Z
                            2\tb\tbar\t2020-01-01T01:00:00.000000Z
                            10\tc\tpqr\t2020-01-01T01:30:00.000000Z
                            3\ta\tbaz\t2020-01-01T02:00:00.000000Z
                            4\tc\tqux\t2020-01-01T03:00:00.000000Z
                            5\tb\tabc\t2020-01-01T04:00:00.000000Z
                            11\ta\tstu\t2020-01-01T04:30:00.000000Z
                            6\ta\tdef\t2020-01-01T05:00:00.000000Z
                            12\tc\tvwx\t2020-01-01T05:30:00.000000Z
                            7\tc\tghi\t2020-01-01T06:00:00.000000Z
                            8\tb\tjkl\t2020-01-01T07:00:00.000000Z
                            100\td\tend\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    @Test
    public void testRewriteWithColumnTop() throws Exception {
        // After a rewrite-mode O3 merge on a partition with column_top > 0,
        // the rewritten Parquet QDB metadata must zero column_top so the
        // decoder reads the (null) pages for the new column instead of
        // skipping the row groups that now hold actual data.
        //
        // Steps:
        // 1. Create table, insert rows, add a new column (column_top > 0).
        // 2. Insert more rows with the new column populated.
        // 3. Convert to Parquet (single row group => rewrite is guaranteed).
        // 4. O3 insert into the Parquet partition, triggering REWRITE.
        // 5. Read back all data -- the new column must materialize correctly
        //    in the copied row group regions.
        assertMemoryLeak(() -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            // Insert 4 rows without the STRING column.
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T06:00:00.000Z'),
                            (3, '2020-01-01T12:00:00.000Z'),
                            (4, '2020-01-01T18:00:00.000Z')
                            """
            );
            // Second partition so 2020-01-01 is not the last.
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // Add STRING column. column_top = 4 for the 2020-01-01 partition.
            execute("ALTER TABLE x ADD COLUMN s STRING");
            drainWalQueue();

            // Insert 2 rows with s values, still in the same partition.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (5, 'hello', '2020-01-01T20:00:00.000Z'),
                            (6, 'world', '2020-01-01T22:00:00.000Z')
                            """
            );
            drainWalQueue();

            // 6 rows, default row group size (1000) → single row group.
            // column_top = 4: first 4 rows have s = NULL, last 2 have data.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            long partitionTs = parseFloorPartialTimestamp("2020-01-01");
            long versionBeforeO3 = getPartitionNameTxn(partitionTs);

            // O3 insert into the Parquet partition.
            // Single row group always triggers a full REWRITE.
            execute(
                    """
                            INSERT INTO x(x, s, ts) VALUES
                            (7, 'mid', '2020-01-01T09:00:00.000Z')
                            """
            );
            drainWalQueue();

            long versionAfterRewrite = getPartitionNameTxn(partitionTs);
            Assert.assertNotEquals("partition version should change on REWRITE", versionBeforeO3, versionAfterRewrite);

            // Verify all data is correct. Without the column_top fix, the
            // decoder would see stale column_top and skip pages for 's',
            // returning incorrect NULL values for rows that have actual data.
            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts\ts
                            1\t2020-01-01T00:00:00.000000Z\t
                            2\t2020-01-01T06:00:00.000000Z\t
                            7\t2020-01-01T09:00:00.000000Z\tmid
                            3\t2020-01-01T12:00:00.000000Z\t
                            4\t2020-01-01T18:00:00.000000Z\t
                            5\t2020-01-01T20:00:00.000000Z\thello
                            6\t2020-01-01T22:00:00.000000Z\tworld
                            100\t2020-01-02T00:00:00.000000Z\t
                            """);
        });
    }

    @Test
    public void testRollbackDataTruncateFailureStillSuspends() throws Exception {
        // M1 regression guard, symmetric with testRollbackFsyncFailureStillSuspends.
        // The rollback opens data.parquet via TableUtils.openRW to truncate it back
        // to the pre-merge size; openRW throws on an FS fault. The fix swallows it so
        // o3BumpErrorCount still runs and the table suspends. Without the swallow the
        // throw escapes the catch before o3BumpErrorCount and the failed update would
        // commit as a success. This injects the openRW fault on the rollback path.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        AtomicBoolean armed = new AtomicBoolean(false);
        AtomicInteger openROCount = new AtomicInteger(0);
        // Set when the 2nd data.parquet openRO fails (the update is about to roll
        // back). The rollback's data.parquet truncate openRW is the next such call.
        AtomicBoolean rollingBack = new AtomicBoolean(false);
        AtomicInteger dataTruncateFailures = new AtomicInteger(0);
        FilesFacade dodgyFacade = new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (armed.get() && Utf8s.endsWithAscii(name, "data.parquet")) {
                    // 2nd openRO (updateParquetIndexes) fails: the in-place update
                    // rolls back, after updateFileMetadata grew _pm.
                    if (openROCount.incrementAndGet() == 2) {
                        rollingBack.set(true);
                        return -1;
                    }
                }
                return super.openRO(name);
            }

            @Override
            public long openRW(LPSZ name, int opts) {
                // Fail only the rollback's data.parquet truncate openRW, exactly once.
                if (Utf8s.endsWithAscii(name, "data.parquet") && rollingBack.compareAndSet(true, false)) {
                    dataTruncateFailures.incrementAndGet();
                    return -1;
                }
                return super.openRW(name, opts);
            }
        };

        assertMemoryLeak(dodgyFacade, () -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            TableToken tableToken = engine.verifyTableName("x");

            armed.set(true);
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();
            armed.set(false);

            // The rollback truncate openRW fault fired, yet the table still
            // suspended: the swallowed exception did not skip o3BumpErrorCount.
            Assert.assertEquals("rollback data.parquet truncate fault must have fired once", 1, dataTruncateFailures.get());
            Assert.assertTrue(
                    "table must suspend despite the rollback data.parquet truncate failure",
                    engine.getTableSequencerAPI().isSuspended(tableToken)
            );

            // Recovery still works once the fault is disarmed: the retried update
            // overwrites the dead tail from the committed head.
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n11\n");
        });
    }

    @Test
    public void testRollbackFsyncFailureStillSuspends() throws Exception {
        // C2 regression guard. The rollback fsync of the leftover _pm tail
        // (ff.fsyncAndClose) can throw; the fix swallows it so o3BumpErrorCount
        // still runs and the table suspends. Without the swallow, the failed
        // update would commit as a success. This injects the fault and checks it.
        node1.setProperty(PropertyKey.CAIRO_COMMIT_MODE, "sync");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        AtomicBoolean armed = new AtomicBoolean(false);
        AtomicInteger openROCount = new AtomicInteger(0);
        // Set when the 2nd data.parquet openRO fails (the update is about to
        // roll back). The rollback's _pm fsyncAndClose is the next such call.
        AtomicBoolean rollingBack = new AtomicBoolean(false);
        AtomicInteger rollbackFsyncFailures = new AtomicInteger(0);
        FilesFacade dodgyFacade = new TestFilesFacadeImpl() {
            @Override
            public void fsyncAndClose(long fd) {
                if (rollingBack.compareAndSet(true, false)) {
                    // Mirror the real fsyncAndClose: close the fd even on fsync
                    // failure (so it never leaks), then throw -- the exact hazard
                    // C2 guards against.
                    rollbackFsyncFailures.incrementAndGet();
                    super.close(fd);
                    throw CairoException.critical(5).put("injected _pm rollback fsync failure");
                }
                super.fsyncAndClose(fd);
            }

            @Override
            public long openRO(LPSZ name) {
                if (armed.get() && Utf8s.endsWithAscii(name, "data.parquet")) {
                    // 2nd openRO (updateParquetIndexes) fails: the in-place update
                    // rolls back, after updateFileMetadata grew _pm.
                    if (openROCount.incrementAndGet() == 2) {
                        rollingBack.set(true);
                        return -1;
                    }
                }
                return super.openRO(name);
            }
        };

        assertMemoryLeak(dodgyFacade, () -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            TableToken tableToken = engine.verifyTableName("x");

            armed.set(true);
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();
            armed.set(false);

            // The rollback fsync fault fired, yet the table still suspended: the
            // swallowed exception did not skip o3BumpErrorCount.
            Assert.assertEquals("rollback fsync fault must have fired once", 1, rollbackFsyncFailures.get());
            Assert.assertTrue(
                    "table must suspend despite the rollback fsync failure",
                    engine.getTableSequencerAPI().isSuspended(tableToken)
            );

            // Recovery still works once the fault is disarmed: the retried update
            // overwrites the dead tail from the committed head.
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertQuery("SELECT count() FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n11\n");
        });
    }

    @Test
    public void testUpdateRollbackDoesNotTruncateParquetMeta() throws Exception {
        // Headline of the _pm SIGBUS change: an in-place parquet O3 update that
        // fails after updateFileMetadata grew _pm must NOT truncate _pm on
        // rollback. Truncating pages out from under a concurrent header-mapping
        // reader's mmap is the SIGBUS hazard. The header is patched only by
        // commitParquetMeta after the index build, so this failure leaves it at
        // the committed head with the grown bytes as an invisible dead tail; the
        // retried update overwrites that tail from the committed head.
        //
        // Force in-place (update) mode: 2 row groups + permissive rewrite
        // thresholds. The injected failure is the 2nd data.parquet openRO, the
        // mapRO inside updateParquetIndexes -- after updateFileMetadata, before
        // commitParquetMeta.
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_ROW_GROUP_SIZE, 4);
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_RATIO, "1.0");
        node1.setProperty(PropertyKey.CAIRO_PARTITION_ENCODER_PARQUET_O3_REWRITE_UNUSED_MAX_BYTES, Long.MAX_VALUE);

        AtomicBoolean armed = new AtomicBoolean(false);
        FilesFacade dodgyFacade = getFilesFacade(armed);

        assertMemoryLeak(dodgyFacade, () -> {
            execute(
                    """
                            CREATE TABLE x (x INT, ts TIMESTAMP)
                            TIMESTAMP(ts) PARTITION BY DAY WAL
                            """
            );
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (1, '2020-01-01T00:00:00.000Z'),
                            (2, '2020-01-01T01:00:00.000Z'),
                            (3, '2020-01-01T02:00:00.000Z'),
                            (4, '2020-01-01T03:00:00.000Z'),
                            (5, '2020-01-01T04:00:00.000Z'),
                            (6, '2020-01-01T05:00:00.000Z'),
                            (7, '2020-01-01T06:00:00.000Z'),
                            (8, '2020-01-01T07:00:00.000Z')
                            """
            );
            execute("INSERT INTO x(x, ts) VALUES (100, '2020-01-02T00:00:00.000Z')");
            drainWalQueue();

            // 8 rows / row-group-size 4 → 2 row groups: the O3 stays in update mode.
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01-01'");
            drainWalQueue();

            TableToken tableToken = engine.verifyTableName("x");
            File tableDir = new File(root, tableToken.getDirName());
            final long parquetMetaSizeBefore = parquetMetaLength(tableDir);
            Assert.assertTrue(parquetMetaSizeBefore > 0);

            // Arm the failure and run an in-place O3 update (merges into rg0).
            armed.set(true);
            execute(
                    """
                            INSERT INTO x(x, ts) VALUES
                            (9, '2020-01-01T00:30:00.000Z'),
                            (10, '2020-01-01T01:30:00.000Z')
                            """
            );
            drainWalQueue();
            armed.set(false);

            // The update failed and suspended the table.
            Assert.assertTrue(engine.getTableSequencerAPI().isSuspended(tableToken));

            // In-place mode left no new partition directory, and the rollback
            // left _pm GROWN (the orphaned dead tail) rather than truncated back
            // to the committed size -- the old code truncated here.
            Assert.assertEquals("in-place update must not create a new dir", 1, countPartitionDirs(tableDir));
            final long parquetMetaSizeAfterFailure = parquetMetaLength(tableDir);
            Assert.assertTrue(
                    "_pm must not be truncated on rollback [before=" + parquetMetaSizeBefore + ", after=" + parquetMetaSizeAfterFailure + "]",
                    parquetMetaSizeAfterFailure > parquetMetaSizeBefore
            );

            // The committed snapshot is still readable: the header was never
            // patched (no SIGBUS; resolves the pre-update footer).
            assertQuery("SELECT count() FROM x WHERE ts < '2020-01-02'")
                    .noLeakCheck()
                    .expectSize()
                    .noRandomAccess()
                    .returns("count\n8\n");

            // Recover: the re-applied update overwrites the dead tail from the
            // committed head (the header still points there).
            execute("ALTER TABLE x RESUME WAL");
            drainWalQueue();
            Assert.assertFalse(engine.getTableSequencerAPI().isSuspended(tableToken));

            assertQuery("SELECT * FROM x")
                    .noLeakCheck()
                    .expectSize()
                    .timestamp("ts")
                    .returns("""
                            x\tts
                            1\t2020-01-01T00:00:00.000000Z
                            9\t2020-01-01T00:30:00.000000Z
                            2\t2020-01-01T01:00:00.000000Z
                            10\t2020-01-01T01:30:00.000000Z
                            3\t2020-01-01T02:00:00.000000Z
                            4\t2020-01-01T03:00:00.000000Z
                            5\t2020-01-01T04:00:00.000000Z
                            6\t2020-01-01T05:00:00.000000Z
                            7\t2020-01-01T06:00:00.000000Z
                            8\t2020-01-01T07:00:00.000000Z
                            100\t2020-01-02T00:00:00.000000Z
                            """);
        });
    }

    private static int countPartitionDirs(File tableDir) {
        String[] dirs = tableDir.list((_, name) -> name.startsWith("2020-01-01"));
        return dirs != null ? dirs.length : 0;
    }

    /**
     * Rewrites the committed {@code _pm} of the 2020-01-01 partition into the
     * crash-window "dirty ahead" state: appends an orphaned dead footer past the
     * committed footer at N, then publishes a header (offset 0) that points at the
     * grown size M. Models commitParquetMeta having published the header before a
     * crash skipped the {@code _txn} commit. {@code _txn} still records the
     * committed parquet size, so resolveFooter walks back from M to N. Returns M.
     * Callers must release every reader/writer first so no live mmap is mapped at
     * the old size. Reads and writes via {@link ByteOrder#nativeOrder()} to match
     * the native order used by Unsafe and the Rust writer.
     */
    private static long makeParquetMetaDirtyAhead(File tableDir) throws IOException {
        final File pm = parquetMetaFile(tableDir);
        final byte[] committed = java.nio.file.Files.readAllBytes(pm.toPath());
        final int committedHead = committed.length; // N: committed _pm size
        final ByteBuffer in = ByteBuffer.wrap(committed).order(ByteOrder.nativeOrder());

        // Locate the committed footer C from its trailer (last 4 bytes hold the
        // footer length). Fixed-position fields, robust to optional feature bytes.
        final int footerLength = in.getInt(committedHead - Integer.BYTES);
        final int footerOffset = committedHead - Integer.BYTES - footerLength;
        Assert.assertEquals("committed footer must carry two row groups", 2, in.getInt(footerOffset + 12));
        final long parquetFooterOffset = in.getLong(footerOffset);
        final int parquetFooterLength = in.getInt(footerOffset + 8);
        final int rowGroupBlock0 = in.getInt(footerOffset + 40); // C's first row group block

        // Dead footer C': fixed(40) + 1 rg entry(4) + CRC(4) + trailer(4) = 52.
        // prev points back to C; its derived parquet size differs from the
        // committed token so resolveFooter walks past C' to C. C' is never the
        // resolved footer, so its CRC is left blank (the walk does not read it).
        final int deadFooterBytes = 52;
        final int dirtyLen = committedHead + deadFooterBytes; // M
        final byte[] out = java.util.Arrays.copyOf(committed, dirtyLen);
        final ByteBuffer dead = ByteBuffer.wrap(out).order(ByteOrder.nativeOrder());
        dead.putLong(committedHead, parquetFooterOffset);         // parquet_footer_offset
        dead.putInt(committedHead + 8, parquetFooterLength + 64); // parquet_footer_length -> derived size != token
        dead.putInt(committedHead + 12, 1);                       // row_group_count (fewer than C's two)
        dead.putLong(committedHead + 16, 0L);                     // unused_bytes
        dead.putLong(committedHead + 24, committedHead);          // prev_parquet_meta_file_size -> C
        dead.putLong(committedHead + 32, 0L);                     // footer_feature_flags
        dead.putInt(committedHead + 40, rowGroupBlock0);          // reuse C's first row group block
        dead.putInt(committedHead + 44, 0);                       // CRC placeholder
        dead.putInt(committedHead + 48, 48);                      // trailer: footer length sans trailer

        // Publish the dirty-ahead header: _pm now physically spans [0, M).
        dead.putLong(0, dirtyLen);

        java.nio.file.Files.write(pm.toPath(), out);
        return dirtyLen;
    }

    private static File parquetMetaFile(File tableDir) {
        String[] dirs = tableDir.list((_, name) -> name.startsWith("2020-01-01"));
        Assert.assertNotNull("no 2020-01-01 partition dir found", dirs);
        Assert.assertEquals("expected exactly one 2020-01-01 partition dir", 1, dirs.length);
        File pm = new File(new File(tableDir, dirs[0]), "_pm");
        Assert.assertTrue("_pm must exist: " + pm, pm.exists());
        return pm;
    }

    private static long parquetMetaHeaderSize(File tableDir) throws IOException {
        byte[] pm = java.nio.file.Files.readAllBytes(parquetMetaFile(tableDir).toPath());
        return ByteBuffer.wrap(pm).order(ByteOrder.nativeOrder()).getLong(0);
    }

    private static long parquetMetaLength(File tableDir) {
        return parquetMetaFile(tableDir).length();
    }

    private static @NotNull FilesFacade getFilesFacade(AtomicBoolean armed) {
        AtomicInteger openROCount = new AtomicInteger(0);

        return new TestFilesFacadeImpl() {
            @Override
            public long openRO(LPSZ name) {
                if (armed.get() && Utf8s.endsWithAscii(name, "data.parquet")) {
                    // 1st openRO on data.parquet: reading original parquet (line 119) → succeed
                    // 2nd openRO on data.parquet: updateParquetIndexes (line 2949) → fail
                    if (openROCount.incrementAndGet() == 2) {
                        return -1;
                    }
                }
                return super.openRO(name);
            }
        };
    }

    private void assertPmAllNullChunkUsesZeroPointers() {
        try (TableReader reader = getReader("x")) {
            for (int i = 0, n = reader.getPartitionCount(); i < n; i++) {
                if (reader.getPartitionFormat(i) != PartitionFormat.PARQUET) {
                    continue;
                }

                reader.openPartition(i);
                ParquetPartitionDecoder decoder = reader.getAndInitParquetPartitionDecoder(i);
                try (
                        RowGroupBuffers rowGroupBuffers = new RowGroupBuffers(MemoryTag.NATIVE_PARQUET_PARTITION_DECODER);
                        DirectIntList parquetColumns = new DirectIntList(4, MemoryTag.NATIVE_PARQUET_PARTITION_DECODER)
                ) {
                    final int fixedColumnIndex = decoder.metadata().getColumnIndex("n");
                    final int varColumnIndex = decoder.metadata().getColumnIndex("v");
                    Assert.assertTrue("n" + " should exist in parquet metadata", fixedColumnIndex >= 0);
                    Assert.assertTrue("v" + " should exist in parquet metadata", varColumnIndex >= 0);

                    parquetColumns.add(fixedColumnIndex);
                    parquetColumns.add(ColumnType.LONG);
                    parquetColumns.add(varColumnIndex);
                    parquetColumns.add(ColumnType.VARCHAR_SLICE);

                    final int rowGroupSize = (int) decoder.metadata().getRowGroupSize(0);
                    decoder.decodeRowGroup(rowGroupBuffers, parquetColumns, 0, 0, rowGroupSize);

                    Assert.assertEquals(0, rowGroupBuffers.getChunkDataPtr(0));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkDataSize(0));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkDataPtr(1));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkDataSize(1));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkAuxPtr(1));
                    Assert.assertEquals(0, rowGroupBuffers.getChunkAuxSize(1));
                }
                return;
            }
        }
        Assert.fail("should find parquet partition");
    }

    // DEBUG helper: copies the on-disk data.parquet for a partition out to an absolute
    // path so it can be inspected with third-party tools (pyarrow/duckdb/parquet-tools).
    private void dumpParquetPartition(String tableName, String partitionDayPrefix, String dstPath) throws Exception {
        TableToken tableToken = engine.verifyTableName(tableName);
        File tableDir = new File(engine.getConfiguration().getDbRoot().toString(), tableToken.getDirName());
        File[] dirs = tableDir.listFiles((dir, name) ->
                name.equals(partitionDayPrefix) || name.startsWith(partitionDayPrefix + "."));
        File srcFile = null;
        if (dirs != null) {
            for (File d : dirs) {
                File p = new File(d, "data.parquet");
                if (p.exists()) {
                    srcFile = p;
                    break;
                }
            }
        }
        if (srcFile == null) {
            String listing = dirs == null ? "<none>" : java.util.Arrays.toString(dirs);
            Assert.fail("data.parquet not found for " + partitionDayPrefix + " under " + tableDir + ", partition dirs: " + listing);
        }
        File dst = new File(dstPath);
        //noinspection ResultOfMethodCallIgnored
        dst.getParentFile().mkdirs();
        java.nio.file.Files.copy(srcFile.toPath(), dst.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        System.out.println("[DUMP] " + srcFile.getAbsolutePath() + " -> " + dst.getAbsolutePath() + " size=" + srcFile.length());
    }

    private long getPartitionNameTxn(long partitionTimestamp) {
        try (TableReader reader = getReader("x")) {
            return reader.getTxFile().getPartitionNameTxnByPartitionTimestamp(partitionTimestamp);
        }
    }

    private void testConvertAllToType(String targetType, String columnDefsSql, String[] cols, String mergeValue) throws Exception {
        assertMemoryLeak(() -> {
            execute(
                    "CREATE TABLE x AS (SELECT\n" + columnDefsSql
                            + ",\ntimestamp_sequence('2020-01-01', 3_600_000_000L) ts"
                            + "\nFROM long_sequence(1000)) TIMESTAMP(ts) PARTITION BY MONTH WAL"
            );
            drainWalQueue();

            execute("CREATE TABLE y AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY MONTH WAL");
            drainWalQueue();

            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01'");
            drainWalQueue();

            for (String col : cols) {
                execute("ALTER TABLE x ALTER COLUMN " + col + " TYPE " + targetType);
                execute("ALTER TABLE y ALTER COLUMN " + col + " TYPE " + targetType);
            }
            drainWalQueue();

            // Read path: compare parquet-backed table x against native reference table y.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");
            }

            // O3 merge: insert a row into the parquet partition to trigger merge.
            StringBuilder sb = new StringBuilder("INSERT INTO %s(");
            for (int i = 0; i < cols.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(cols[i]);
            }
            sb.append(", ts) VALUES (");
            for (int i = 0; i < cols.length; i++) {
                if (i > 0) sb.append(", ");
                sb.append(mergeValue);
            }
            sb.append(", '2020-01-15T12:00:00.000Z')");
            String mergeInsert = sb.toString();

            execute(String.format(mergeInsert, "x"));
            execute(String.format(mergeInsert, "y"));
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            // Compare after O3 merge.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");
            }
        });
    }

    private void testConvertFixedToVar(String varType) throws Exception {
        assertMemoryLeak(() -> {
            // Generate 1000 random rows of all fixed types.
            execute(
                    """
                            CREATE TABLE x AS (
                                SELECT
                                    rnd_boolean() v_bool,
                                    rnd_byte() v_byte,
                                    rnd_short() v_short,
                                    rnd_char() v_char,
                                    rnd_int(0, 1_000_000, 4) v_int,
                                    rnd_long(0, 1_000_000_000L, 4) v_long,
                                    rnd_float(4) v_float,
                                    rnd_double(4) v_double,
                                    rnd_date(
                                        to_date('2000', 'yyyy'),
                                        to_date('2025', 'yyyy'), 4
                                    ) v_date,
                                    rnd_timestamp(
                                        to_timestamp('2000', 'yyyy'),
                                        to_timestamp('2025', 'yyyy'), 4
                                    ) v_ts,
                                    rnd_ipv4() v_ipv4,
                                    rnd_uuid4(4) v_uuid,
                                    rnd_long(
                                        946_684_800_000_000_000L,
                                        1_609_459_200_000_000_000L, 4
                                    )::TIMESTAMP_NS v_tsns,
                                    timestamp_sequence('2020-01-01', 3_600_000_000L) ts
                                FROM long_sequence(1000)
                            ) TIMESTAMP(ts) PARTITION BY MONTH WAL
                            """
            );

            drainWalQueue();

            // Copy to a reference table that stays native throughout.
            execute("CREATE TABLE y AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY MONTH WAL");
            drainWalQueue();

            // Convert the first partition to parquet (2020-01, ~744 rows).
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01'");
            drainWalQueue();

            // ALTER all fixed columns to the target var type on both tables.
            String[] cols = {
                    "v_bool", "v_byte", "v_short", "v_char", "v_int", "v_long",
                    "v_float", "v_double", "v_date", "v_ts", "v_ipv4", "v_uuid", "v_tsns"
            };
            for (String col : cols) {
                execute("ALTER TABLE x ALTER COLUMN " + col + " TYPE " + varType);
                execute("ALTER TABLE y ALTER COLUMN " + col + " TYPE " + varType);
            }
            drainWalQueue();

            // Read path: compare parquet-backed table x against native reference table y.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");
            }

            // O3 merge: insert a row into the parquet partition to trigger merge.
            String mergeInsert = "INSERT INTO %s(v_bool, v_byte, v_short, v_char, v_int, v_long,"
                    + " v_float, v_double, v_date, v_ts, v_ipv4, v_uuid, v_tsns, ts)"
                    + " VALUES ('true', '99', '999', 'X', '999', '999', '9.9', '9.9',"
                    + " '2020-07-01', '2020-07-01', '5.5.5.5', '55555555-5555-5555-5555-555555555555',"
                    + " '2020-07-01', '2020-01-15T12:00:00.000Z')";
            execute(String.format(mergeInsert, "x"));
            execute(String.format(mergeInsert, "y"));
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            // Compare after O3 merge.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");
            }
        });
    }

    private void testConvertVarToAllFixed(String varType) throws Exception {
        assertMemoryLeak(() -> {
            // Generate 1000 random rows. Each column stores string representations
            // of the target fixed type, so we use casts like rnd_int(...)::VARCHAR.
            execute(
                    "CREATE TABLE x AS (\n"
                            + "    SELECT\n"
                            + "        rnd_int(0, 1_000_000, 4)::" + varType + " v_int,\n"
                            + "        rnd_long(0, 1_000_000_000L, 4)::" + varType + " v_long,\n"
                            + "        rnd_float(4)::" + varType + " v_float,\n"
                            + "        rnd_double(4)::" + varType + " v_double,\n"
                            + "        rnd_short()::" + varType + " v_short,\n"
                            + "        rnd_byte()::" + varType + " v_byte,\n"
                            + "        rnd_date(\n"
                            + "            to_date('2000', 'yyyy'),\n"
                            + "            to_date('2025', 'yyyy'), 4\n"
                            + "        )::" + varType + " v_date,\n"
                            + "        rnd_timestamp(\n"
                            + "            to_timestamp('2000', 'yyyy'),\n"
                            + "            to_timestamp('2025', 'yyyy'), 4\n"
                            + "        )::" + varType + " v_ts,\n"
                            + "        timestamp_sequence('2020-01-01', 3_600_000_000L) ts\n"
                            + "    FROM long_sequence(1000)\n"
                            + ") TIMESTAMP(ts) PARTITION BY MONTH WAL"
            );
            drainWalQueue();

            // Copy to a reference table that stays in the same format.
            execute("CREATE TABLE y AS (SELECT * FROM x) TIMESTAMP(ts) PARTITION BY MONTH WAL");
            drainWalQueue();

            // Convert the first partition to parquet (2020-01, ~744 rows).
            execute("ALTER TABLE x CONVERT PARTITION TO PARQUET LIST '2020-01'");
            drainWalQueue();

            // ALTER all var columns to their target fixed types on both tables.
            String[] colsAndTypes = {
                    "v_int", "INT",
                    "v_long", "LONG",
                    "v_float", "FLOAT",
                    "v_double", "DOUBLE",
                    "v_short", "SHORT",
                    "v_byte", "BYTE",
                    "v_date", "DATE",
                    "v_ts", "TIMESTAMP",
            };
            for (int i = 0; i < colsAndTypes.length; i += 2) {
                execute("ALTER TABLE x ALTER COLUMN " + colsAndTypes[i] + " TYPE " + colsAndTypes[i + 1]);
                execute("ALTER TABLE y ALTER COLUMN " + colsAndTypes[i] + " TYPE " + colsAndTypes[i + 1]);
            }
            drainWalQueue();

            // Read path: compare parquet-backed table x against native reference table y.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");
            }

            // O3 merge: insert a row into the parquet partition to trigger merge.
            String mergeInsert = "INSERT INTO %s(v_int, v_long, v_float, v_double, v_short, v_byte, v_date, v_ts, ts)"
                    + " VALUES (999, 999, 9.9, 9.9, 999, 99, '2020-07-01T00:00:00.000Z',"
                    + " '2020-07-01T00:00:00.000000Z', '2020-01-15T12:00:00.000Z')";
            execute(String.format(mergeInsert, "x"));
            execute(String.format(mergeInsert, "y"));
            drainWalQueue();

            Assert.assertFalse(
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("x"))
            );

            // Compare after O3 merge.
            try (SqlCompiler compiler = engine.getSqlCompiler()) {
                TestUtils.assertEquals(compiler, sqlExecutionContext, "y", "x");
            }
        });
    }
}
