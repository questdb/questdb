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

package io.questdb.test.cutlass.pgwire;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static io.questdb.cairo.sql.SqlExecutionCircuitBreaker.TIMEOUT_FAIL_ON_FIRST_CHECK;

public class PGDeclareSyntaxTest extends BasePGTest {

    @Test
    public void testDeclareSyntaxWorksWithPositionalBindVariables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, TIMEOUT_FAIL_ON_FIRST_CHECK, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement("DECLARE @x := ?, @y := ? SELECT @x::int + @y::int")) {
                ps.setInt(1, 1);
                ps.setInt(2, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "column[INTEGER]\n" +
                                    "3\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }
}
