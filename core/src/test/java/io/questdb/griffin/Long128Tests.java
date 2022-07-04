/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.griffin;

import org.junit.Test;

public class Long128Tests extends AbstractGriffinTest {
    @Test
    public void testReadLong128Column() throws SqlException {
        assertQuery(
                "ts\tts1\ti\n" +
                        "1-1645660800000000\t2022-02-24T00:00:00.000000Z\t1\n" +
                        "2-1645660801000000\t2022-02-24T00:00:01.000000Z\t2\n" +
                        "3-1645660802000000\t2022-02-24T00:00:02.000000Z\t3\n" +
                        "4-1645660803000000\t2022-02-24T00:00:03.000000Z\t4\n" +
                        "5-1645660804000000\t2022-02-24T00:00:04.000000Z\t5\n" +
                        "6-1645660805000000\t2022-02-24T00:00:05.000000Z\t6\n" +
                        "7-1645660806000000\t2022-02-24T00:00:06.000000Z\t7\n" +
                        "8-1645660807000000\t2022-02-24T00:00:07.000000Z\t8\n" +
                        "9-1645660808000000\t2022-02-24T00:00:08.000000Z\t9\n" +
                        "10-1645660809000000\t2022-02-24T00:00:09.000000Z\t10\n",
                "select" +
                        " to_long128(x, timestamp_sequence('2022-02-24', 1000000L)) ts," +
                        " timestamp_sequence('2022-02-24', 1000000L) ts1," +
                        " cast(x as int) i" +
                        " from long_sequence(10)",
                null,
                false,
                true
        );
    }
}
