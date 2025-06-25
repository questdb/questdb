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

package io.questdb.test.griffin;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class ProjectionReferenceTest extends AbstractCairoTest {

    @Test
    public void testPreferBaseColumnOverProjectionVanilla() throws Exception {
        execute("create table temp (x int)");
        execute("insert into temp values (1), (2), (3)");
        assertQuery(
                "x\tcolumn\n" +
                        "11\t-4\n" +
                        "12\t-3\n" +
                        "13\t-2\n",
                "select x + 10 x, x - 5 from temp",
                true
        );
    }

    @Test
    public void testVanilla() throws Exception {
        execute("create table tmp as" +
                " (select" +
                " rnd_double() a," +
                " timestamp_sequence('2025-06-22'::timestamp, 150099) ts" +
                " from long_sequence(10)" +
                ") timestamp(ts) partition by hour");
        assertQuery(
                "i\tcolumn\n" +
                        "1.3215555788374664\t2.3215555788374664\n" +
                        "0.4492602684994518\t1.4492602684994518\n" +
                        "0.16973928465121335\t1.1697392846512134\n" +
                        "0.59839809192369\t1.59839809192369\n" +
                        "0.4089488367575551\t1.4089488367575551\n" +
                        "1.3017188051710602\t2.30171880517106\n" +
                        "1.684682184176669\t2.684682184176669\n" +
                        "1.9712581691748525\t2.9712581691748525\n" +
                        "0.44904681712176453\t1.4490468171217645\n" +
                        "1.0187654003234814\t2.018765400323481\n",
                "select a * 2 i, i + 1 from tmp;",
                true
        );
    }
}
