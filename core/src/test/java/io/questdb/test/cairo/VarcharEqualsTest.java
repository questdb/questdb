/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class VarcharEqualsTest extends AbstractCairoTest {

    @Test
    public void testConstFullyInlinedAscii() throws Exception {
        assertQuery(
                "name\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\n",
                "x where name = 'bac'::varchar",
                "create table x as (select rnd_varchar(null, 'a', 'bac', 'sntht', 'šěčř', 'gcrlhtneu') name from long_sequence(100))",
                null
        );
    }

    @Test
    public void testConstFullyInlinedLength9() throws Exception {
        assertQuery(
                "name\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\n" +
                        "gcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\ngcrlhtneu\n",
                "x where name = 'gcrlhtneu'::varchar",
                "create table x as (select rnd_varchar(null, 'a', 'bac', 'sntht', 'šěčř', 'gcrlhtneu') name from long_sequence(100))",
                null
        );
    }

    @Test
    public void testConstFullyInlinedNonAscii() throws Exception {
        assertQuery(
                "name\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\n",
                "x where name = 'šěčř'::varchar",
                "create table x as (select rnd_varchar(null, 'a', 'bac', 'sntht', 'šěčř', 'gcrlhtneu') name from long_sequence(100))",
                null
        );
    }

    @Test
    public void testConstSplitAscii() throws Exception {
        assertQuery(
                "name\nqwpgaslvbnsvslhf\nqwpgaslvbnsvslhf\nqwpgaslvbnsvslhf\nqwpgaslvbnsvslhf\nqwpgaslvbnsvslhf\n" +
                        "qwpgaslvbnsvslhf\nqwpgaslvbnsvslhf\nqwpgaslvbnsvslhf\n",
                "x where name = 'qwpgaslvbnsvslhf'::varchar",
                "create table x as (select rnd_varchar(null, 'gcrlhtneuv', 'gcrlht', 'qwpgaslvbnsvslhf') name from long_sequence(30))",
                null
        );
    }

    @Test
    public void testConstSplitNonAscii() throws Exception {
        assertQuery(
                "name\něšščř\něšščř\něšščř\něšščř\něšščř\něšščř\n",
                "x where name = 'ěšščř'::varchar",
                "create table x as (select rnd_varchar(null, 'ěšščř', 'ěššč', 'ěšščřěšščř') name from long_sequence(30))",
                null
        );
    }


    @Test
    public void testColToCol() throws Exception {
        assertQuery(
                "name1\tname2\n" +
                        "snthtneusd\tsnthtneusd\nšěčřž\tšěčřž\nšěčř\tšěčř\nsnthtneusd\tsnthtneusd\nšěčř\tšěčř\nšěčř\tšěčř\n" +
                        "\t\nšěčř\tšěčř\nšěčřž\tšěčřž\nsntht\tsntht\nšěčř\tšěčř\n\t\nsntht\tsntht\nsnthtneusd\tsnthtneusd\n",
                "x where name1 = name2",
                "create table x as (select" +
                        " rnd_varchar(null, 'sntht', 'snthtneusd', 'šěčř', 'šěčřž') name1," +
                        " rnd_varchar(null, 'sntht', 'snthtneusd', 'šěčř', 'šěčřž') name2" +
                        " from long_sequence(80))",
                null
        );
    }
}
