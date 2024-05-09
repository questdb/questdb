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

package io.questdb.test.griffin.engine.functions;

import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class InTimestampTimestampTest extends AbstractCairoTest {
    @Test
    public void testBindStr() throws SqlException {
        ddl("create table test as (select rnd_int() a, timestamp_sequence(0, 1000) ts from long_sequence(100))");

        // when more than one argument supplied, the function will match exact values from the list
        assertSql(
                "a\tts\n" +
                        "-1148479920\t1970-01-01T00:00:00.000000Z\n" +
                        "315515118\t1970-01-01T00:00:00.001000Z\n" +
                        "-948263339\t1970-01-01T00:00:00.005000Z\n",
                "test where ts in ($1,$2,$3)",
                () -> {
                    bindVariableService.setInt(0, 0);
                    bindVariableService.setInt(1, 1000);
                    bindVariableService.setStr(2, "1970-01-01T00:00:00.005000Z");
                }
        );

        // for single
        assertSql("", "test where ts in $1", () -> bindVariableService.setInt(1, 10));
    }
}
