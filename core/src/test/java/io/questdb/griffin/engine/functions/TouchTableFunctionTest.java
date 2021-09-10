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

package io.questdb.griffin.engine.functions;

import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.engine.functions.rnd.SharedRandom;
import io.questdb.std.Rnd;
import org.junit.Before;
import org.junit.Test;

public class TouchTableFunctionTest extends AbstractGriffinTest {
    @Before
    public void setUp3() {
        SharedRandom.RANDOM.set(new Rnd());
    }

    @Test
    public void testTouchTable() throws Exception {
        assertQuery("touch\n" +
                        "touched dataPages[80], indexKeyPages[1043], indexValuePages[1043]\n",
                "select touch(select * from x)",
                "create table x as " +
                        "(" +
                        "select" +
                        " rnd_geohash(40) g," +
                        " rnd_double(0)*100 a," +
                        " rnd_symbol(5,4,4,1) b," +
                        " timestamp_sequence(0, 100000000000) k" +
                        " from long_sequence(20)" +
                        "), index(b) timestamp(k) partition by DAY",
                null,
                "insert into x select * from (" +
                        " select" +
                        " rnd_geohash(40)," +
                        " rnd_double(0)*100," +
                        " 'VTJW'," +
                        " to_timestamp('2019', 'yyyy') t" +
                        " from long_sequence(100)" +
                        ") timestamp (t)",
                "touch\n" +
                        "touched dataPages[84], indexKeyPages[1044], indexValuePages[1044]\n",
                true,
                true,
                true
        );
    }
}
