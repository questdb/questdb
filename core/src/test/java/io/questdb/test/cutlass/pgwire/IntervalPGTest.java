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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class IntervalPGTest extends BasePGTest {

    public IntervalPGTest(@NonNull LegacyMode legacyMode) {
        super(legacyMode);
    }

    @Parameterized.Parameters(name = "{0}, {1}")
    public static Collection<Object[]> testParams() {
        return Arrays.asList(new Object[][]{
                {LegacyMode.MODERN},
                {LegacyMode.LEGACY},
        });
    }

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
}
