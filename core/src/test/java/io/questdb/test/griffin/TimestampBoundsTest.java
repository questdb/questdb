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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoException;
import io.questdb.griffin.SqlException;
import io.questdb.std.datetime.microtime.Timestamps;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.questdb.test.tools.TestUtils.assertContains;

@RunWith(Parameterized.class)
public class TimestampBoundsTest extends AbstractCairoTest {
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
        engine.getMatViewGraph().clear();
    }

    @Test
    public void testDesignatedTimestampBoundsNonPartitioned() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts)");
            try {
                execute("INSERT INTO tango VALUES (NULL)");
                Assert.fail("Inserting NULL timestamp should have failed");
            } catch (SqlException e) {
                assertContains(e.getMessage(), "designated timestamp column cannot be NULL");
            }
            try {
                execute("INSERT INTO tango VALUES (" + -1L + ")");
                Assert.fail("Inserting negative timestamp should have failed");
            } catch (CairoException e) {
                assertContains(e.getMessage(), "designated timestamp before 1970-01-01 is not allowed");
            }
            execute("INSERT INTO tango VALUES (" + Timestamps.YEAR_10000 + ")");
        });
    }

    @Test
    public void testDesignatedTimestampBoundsPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            try {
                execute("INSERT INTO tango VALUES (NULL)");
                Assert.fail("Inserting NULL timestamp should have failed");
            } catch (SqlException e) {
                assertContains(e.getMessage(), "designated timestamp column cannot be NULL");
            }
            try {
                execute("INSERT INTO tango VALUES (" + -1L + ")");
                Assert.fail("Inserting timestamp before 1970 should have failed");
            } catch (CairoException e) {
                assertContains(e.getMessage(), "designated timestamp before 1970-01-01 is not allowed");
            }
            try {
                execute("INSERT INTO tango VALUES (" + Timestamps.YEAR_10000 + ")");
                Assert.fail("Inserting designated timestamp in year 10,000 should have failed");
            } catch (CairoException e) {
                assertContains(e.getMessage(),
                        "designated timestamp beyond 9999-12-31 is not allowed in a partitioned table");
            }
        });
    }

    @Test
    public void testDesignatedTimestampBoundsWithSwitchPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES (" + 1L + ")");
            try {
                execute("INSERT INTO tango VALUES (NULL)");
                Assert.fail("Inserting NULL timestamp should have failed");
            } catch (SqlException e) {
                assertContains(e.getMessage(), "designated timestamp column cannot be NULL");
            }
            try {
                execute("INSERT INTO tango VALUES (" + -1L + ")");
                Assert.fail("Inserting timestamp before 1970 should have failed");
            } catch (CairoException e) {
                assertContains(e.getMessage(), "designated timestamp before 1970-01-01 is not allowed");
            }
            try {
                execute("INSERT INTO tango VALUES (" + Timestamps.YEAR_10000 + ")");
                Assert.fail("Inserting designated timestamp in year 10,000 should have failed");
            } catch (CairoException e) {
                assertContains(e.getMessage(),
                        "designated timestamp beyond 9999-12-31 is not allowed in a partitioned table");
            }
        });
    }

    @Test
    public void testTimestampBoundsNonPartitioned() throws Exception {
        Assume.assumeFalse(walEnabled);
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP)");
            execute("INSERT INTO tango VALUES (" + Timestamps.YEAR_10000 + ")");
            execute("INSERT INTO tango VALUES (" + -1L + ")");
        });
    }

}
