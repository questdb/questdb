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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.std.datetime.CommonUtils;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TimestampBoundsTest extends AbstractCairoTest {
    private static final String NANOS_OUT_OF_BOUNDS =
            "designated timestamp_ns before 1970-01-01 and beyond 2261-12-31 23:59:59.999999999 is not allowed";

    private final boolean walEnabled;

    public TimestampBoundsTest(boolean walEnabled) {
        this.walEnabled = walEnabled;
    }

    @Parameterized.Parameters(name = "WAL={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{{false}, {true}});
    }

    @Before
    public void setUp() {
        super.setUp();
        node1.setProperty(PropertyKey.CAIRO_WAL_ENABLED_DEFAULT, walEnabled);
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_ENABLED, true);
        engine.getDependentViewGraph().clear();
    }

    @Test
    public void testDesignatedTimestampBoundsNonPartitioned() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts)");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")")
                    .fails(26, "designated timestamp beyond 9999-12-31 is not allowed");
        });
    }

    @Test
    public void testDesignatedTimestampBoundsPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")")
                    .fails(26, "designated timestamp beyond 9999-12-31 is not allowed");
        });
    }

    @Test
    public void testDesignatedTimestampBoundsWithSwitchPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES (" + 1L + ")");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')")
                    .fails(26, "designated timestamp before 1970-01-01 is not allowed");
            assertQuery("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")")
                    .fails(26, "designated timestamp beyond 9999-12-31 is not allowed");
        });
    }

    @Test
    public void testDesignatedNanosTimestampBoundsNonPartitioned() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts)");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900000000Z')")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + (CommonUtils.MAX_TIMESTAMP + 1) + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + Long.MAX_VALUE + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
        });
    }

    @Test
    public void testDesignatedNanosTimestampBoundsPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900000000Z')")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + (CommonUtils.MAX_TIMESTAMP + 1) + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + Long.MAX_VALUE + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
        });
    }

    @Test
    public void testDesignatedNanosTimestampBoundsWithSwitchPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES (" + 1L + ")");
            assertQuery("INSERT INTO tango VALUES (NULL)")
                    .fails(26, "designated timestamp column cannot be NULL");
            assertQuery("INSERT INTO tango VALUES (" + -1L + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900000000Z')")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + (CommonUtils.MAX_TIMESTAMP + 1) + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            assertQuery("INSERT INTO tango VALUES (" + Long.MAX_VALUE + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
        });
    }

    /**
     * The rejected row must not leave the writer distressed or the partition bookkeeping damaged:
     * before the bound was enforced, a batch that ended past the ceiling threw
     * {@code ArrayIndexOutOfBoundsException} out of {@code TxWriter} and killed the writer.
     */
    @Test
    public void testDesignatedNanosTimestampOutOfBoundsKeepsTableUsable() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES ('2024-01-01T00:00:00.000000000Z')");
            assertQuery("INSERT INTO tango VALUES (" + (Long.MAX_VALUE - 1) + "), (" + Long.MAX_VALUE + ")")
                    .fails(26, NANOS_OUT_OF_BOUNDS);
            execute("INSERT INTO tango VALUES ('2024-01-02T00:00:00.000000000Z')");
            drainWalQueue();
            assertQuery("SELECT * FROM tango").timestamp("ts").expectSize().returns("""
                    ts
                    2024-01-01T00:00:00.000000000Z
                    2024-01-02T00:00:00.000000000Z
                    """);
            assertQuery("SELECT count() FROM tango").noRandomAccess().expectSize().returns("""
                    count
                    2
                    """);
        });
    }

    /**
     * The last legal nanosecond, one below the ceiling the error message names, must still be
     * storable and readable back.
     */
    @Test
    public void testDesignatedNanosTimestampUpperBoundIsInclusive() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS) TIMESTAMP(ts) PARTITION BY DAY "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES (" + CommonUtils.MAX_TIMESTAMP + ")");
            drainWalQueue();
            assertQuery("SELECT * FROM tango").timestamp("ts").expectSize().returns("""
                    ts
                    2261-12-31T23:59:59.999999999Z
                    """);
        });
    }

    @Test
    public void testNanosTimestampBoundsNotDesignated() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP_NS)");
            execute("INSERT INTO tango VALUES (" + Long.MAX_VALUE + ")");
            execute("INSERT INTO tango VALUES (" + -1L + ")");
        });
    }

    @Test
    public void testTimestampBoundsNotDesignated() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP)");
            execute("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")");
            execute("INSERT INTO tango VALUES (" + -1L + ")");
            execute("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')");
        });
    }

}
