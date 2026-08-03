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

package io.questdb.test.cutlass.pgwire;

import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.postgresql.util.PSQLException;

import java.sql.PreparedStatement;

public class IntervalPGTest extends BasePGTest {

    @Test
    public void testIntervalSelect() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement("SELECT interval(100, 200)")) {
                assertResultSet(
                        "interval[VARCHAR]\n" +
                                "('1970-01-01T00:00:00.000Z', '1970-01-01T00:00:00.000Z')\n",
                        sink,
                        ps.executeQuery()
                );
            }
        });
    }

    @Test
    public void testIntervalTooLargeForSendBuffer() throws Exception {
        // An INTERVAL renders as text, so its length is only known once rendered and the send buffer
        // size calculators cannot predict it. A row that outgrows the whole buffer has to come back as
        // the actionable "not enough space" message, not as an internal "unsupported type: 39".
        final StringBuilder columns = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            columns.append(i > 0 ? ", " : "").append("interval(100, 200) i").append(i);
        }
        assertWithPgServer(Mode.EXTENDED, false, -1, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement("SELECT " + columns + " FROM long_sequence(5)")) {
                ps.executeQuery();
                Assert.fail("expected the server to reject a record larger than the send buffer");
            } catch (PSQLException e) {
                TestUtils.assertContains(e.getMessage(), "not enough space in send buffer");
            }
        }, () -> sendBufferSize = 512);
    }
}
