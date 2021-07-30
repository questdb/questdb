/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.griffin.engine.functions.eq;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class EqGeoHashGeoHashFunctionFactoryTest extends AbstractGriffinTest {

    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testEq() throws Exception {
        final String expected = "a\tb\n" +
                "11010001011\t11010001011\n";

                assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_geohash(11) a," +
                    " rnd_geohash(11) b" +
                    " from long_sequence(5000)" +
                    ")", sqlExecutionContext);
            assertSql(
                    "x where a = b",
                    expected
            );
        });
    }

    @Test
    public void testNotEq() throws Exception {
        final String expected = "a\tb\n";

        assertMemoryLeak(() -> {
            compiler.compile("create table x as (" +
                    " select" +
                    " rnd_geohash(11) a," +
                    " rnd_geohash(13) b" +
                    " from long_sequence(8)" +
                    ")", sqlExecutionContext);
            assertSql(
                    "x where a != b",
                    expected
            );
        });
    }

    // TODO: test =, != null
    // TODO: add more coverage
}
