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

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PGDeclareSyntaxTest extends BasePGTest {

    @Test
    public void testDeclareSyntaxWorksWithPositionalBindVariables() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
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

    @Test
    public void testDeclareSyntaxWorksWithPositionalBindVariables2() throws Exception {
        assertWithPgServer(CONN_AWARE_ALL, (connection, binary, mode, port) -> {
            try (PreparedStatement ps = connection.prepareStatement("DECLARE @x := ? SELECT x as x2, (@x + 5) as x1, @x as x3 FROM (SELECT * FROM long_sequence(10) WHERE x = @x)")) {
                ps.setInt(1, 5);
                try (ResultSet rs = ps.executeQuery()) {
                    assertResultSet(
                            "x2[BIGINT],x1[INTEGER],x3[INTEGER]\n" +
                                    "5,10,5\n",
                            sink,
                            rs
                    );
                }
            }
        });
    }
}


