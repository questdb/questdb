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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class TruncateUncachedSymbolTest extends AbstractCairoTest {
    @Test
    public void testSimple() throws Exception {
        assertMemoryLeak(
                () -> {
                    execute("CREATE TABLE\n" +
                            "positions(\n" +
                            "\ttime timestamp, \n" +
                            "\tuuid symbol cache, \n" +
                            "\tlatitude double, \n" +
                            "\tlongitude double, \n" +
                            "\thash1 symbol cache, \n" +
                            "\thash2 symbol cache, \n" +
                            "\thash3 symbol cache, \n" +
                            "\thash4 symbol nocache, \n" +
                            "\thash5 symbol nocache, \n" +
                            "\thash6 symbol nocache, \n" +
                            "\thash1i int,\n" +
                            "\thash2i int,\n" +
                            "\thash3i int,\n" +
                            "\thash4i int,\n" +
                            "\thash5i int,\n" +
                            "\thash6i int\n" +
                            ")\n" +
                            "timestamp(time);");
                    execute("alter TABLE positions ALTER COLUMN hash6 ADD INDEX", sqlExecutionContext);
                    execute("INSERT INTO positions\n" +
                            "VALUES(\n" +
                            "    1578506142000000L,\n" +
                            "    '123e4567-e89b-12d3-a456-426614174000',\n" +
                            "    54.1803268,\n" +
                            "    7.8889438,\n" +
                            "    'u',\n" +
                            "    'u1',\n" +
                            "    'u1t',\n" +
                            "    'u1ts',\n" +
                            "    'u1ts5',\n" +
                            "    'u1ts5x',\n" +
                            "    1,\n" +
                            "    2,\n" +
                            "    3,\n" +
                            "    4,\n" +
                            "    5,\n" +
                            "    6\n" +
                            ");");

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "positions",
                            sink,
                            "time\tuuid\tlatitude\tlongitude\thash1\thash2\thash3\thash4\thash5\thash6\thash1i\thash2i\thash3i\thash4i\thash5i\thash6i\n" +
                                    "2020-01-08T17:55:42.000000Z\t123e4567-e89b-12d3-a456-426614174000\t54.1803268\t7.8889438\tu\tu1\tu1t\tu1ts\tu1ts5\tu1ts5x\t1\t2\t3\t4\t5\t6\n"
                    );

                    execute("truncate table positions");

                    TestUtils.assertSql(
                            engine,
                            sqlExecutionContext,
                            "positions",
                            sink,
                            "time\tuuid\tlatitude\tlongitude\thash1\thash2\thash3\thash4\thash5\thash6\thash1i\thash2i\thash3i\thash4i\thash5i\thash6i\n"
                    );
                }
        );
    }
}
