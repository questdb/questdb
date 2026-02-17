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
import org.junit.Test;

public class VarcharConversionTest extends AbstractCairoTest {

    @Test
    public void testConvertToIpv4() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (c_varchar varchar)");
            execute("create table y (c_ipv4 ipv4)");
            execute("insert into x values('127.0.0.1')");
            execute("insert into y select * from x");
            assertSql("c_ipv4\n127.0.0.1\n", "y");
        });
    }

    @Test
    public void testConvertToLong256() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (c_varchar varchar)");
            execute("create table y (c_long256 long256)");
            execute("insert into x values('0x1')");
            execute("insert into y select * from x");
            assertSql("c_long256\n0x01\n", "y");
        });
    }

    @Test
    public void testConvertToGeohash() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table x (c_varchar varchar)");
            execute("create table y (c_geohash geohash(2B))");
            execute("insert into x values('sr')");
            execute("insert into y select * from x");
            assertSql("c_geohash\n11\n", "y");
        });
    }
}
