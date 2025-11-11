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
            assertException("INSERT INTO tango VALUES (NULL)", 26, "designated timestamp column cannot be NULL");
            assertException("INSERT INTO tango VALUES (" + -1L + ")", 26, "designated timestamp before 1970-01-01 is not allowed");
            assertException("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')", 26, "designated timestamp before 1970-01-01 is not allowed");
            assertException("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")", 26, "designated timestamp beyond 9999-12-31 is not allowed");
        });
    }

    @Test
    public void testDesignatedTimestampBoundsPartitioned() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            assertException("INSERT INTO tango VALUES (NULL)", 26, "designated timestamp column cannot be NULL");
            assertException("INSERT INTO tango VALUES (" + -1L + ")", 26, "designated timestamp before 1970-01-01 is not allowed");
            assertException("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')", 26, "designated timestamp before 1970-01-01 is not allowed");
            assertException("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")", 26, "designated timestamp beyond 9999-12-31 is not allowed");
        });
    }

    @Test
    public void testDesignatedTimestampBoundsWithSwitchPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE tango (ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR "
                    + (walEnabled ? "" : "BYPASS ") + "WAL");
            execute("INSERT INTO tango VALUES (" + 1L + ")");
            assertException("INSERT INTO tango VALUES (NULL)", 26, "designated timestamp column cannot be NULL");
            assertException("INSERT INTO tango VALUES (" + -1L + ")", 26, "designated timestamp before 1970-01-01 is not allowed");
            assertException("INSERT INTO tango VALUES ('1969-12-31T23:59:59.900Z')", 26, "designated timestamp before 1970-01-01 is not allowed");
            assertException("INSERT INTO tango VALUES (" + Micros.YEAR_10000 + ")", 26, "designated timestamp beyond 9999-12-31 is not allowed");
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
