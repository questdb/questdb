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

package io.questdb.test.cutlass.pgwire;

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class PGInsertErrorTest extends BasePGTest {

    @Test
    public void testInsertComputeFailure() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareCall(
                    "CREATE TABLE IF NOT EXISTS foo (\n" +
                            "            ts TIMESTAMP,\n" +
                            "            bar INT\n" +
                            "        ) TIMESTAMP (ts)\n" +
                            "          PARTITION BY DAY"
            ).execute();

            PreparedStatement ps = connection.prepareStatement("INSERT INTO foo VALUES (?, ?)");
            ps.executeBatch();
            ps.setTimestamp(1, new Timestamp(100002222L));
            ps.setLong(2, 1900L);
            ps.execute();

            ps.setTimestamp(1, new Timestamp(100002222L));
            ps.setLong(2, 19000000000L);
            try {
                ps.execute();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value: 19000000000 [LONG -> INT]");
            }
        });
    }

    @Test
    public void testInsertComputeFailureBatch() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            connection.prepareCall(
                    "CREATE TABLE IF NOT EXISTS foo (\n" +
                            "            ts TIMESTAMP,\n" +
                            "            bar INT\n" +
                            "        ) TIMESTAMP (ts)\n" +
                            "          PARTITION BY DAY"
            ).execute();

            // executing inserts in batches is not random
            // we are aiming to prepare first insert, which will start the implicit transaction
            // the second insert will fail, while writer is divided between insert and "pending" writer map.
            // failing insert must clean up correctly so that writer will not be double-closed.
            PreparedStatement ps = connection.prepareStatement("INSERT INTO foo VALUES (?, ?)");
            ps.executeBatch();
            ps.setTimestamp(1, new Timestamp(100002222L));
            ps.setLong(2, 1900L);
            ps.addBatch();

            ps.setTimestamp(1, new Timestamp(100002222L));
            ps.setLong(2, 19000000000L);
            ps.addBatch();
            try {
                ps.executeBatch();
                Assert.fail();
            } catch (SQLException e) {
                TestUtils.assertContains(e.getMessage(), "inconvertible value: 19000000000 [LONG -> INT]");
            }
        });
    }
}
