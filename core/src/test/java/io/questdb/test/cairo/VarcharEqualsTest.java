/*+*****************************************************************************
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

package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class VarcharEqualsTest extends AbstractCairoTest {

    @Test
    public void testColToCol() throws Exception {
        assertQuery("x where name1 = name2")
                .ddl("create table x as (select" +
                        " rnd_varchar(null, 'sntht', 'snthtneusd', 'šěčř', 'šěčřž') name1," +
                        " rnd_varchar(null, 'sntht', 'snthtneusd', 'šěčř', 'šěčřž') name2" +
                        " from long_sequence(80))")
                .returns("""
                        name1\tname2
                        snthtneusd\tsnthtneusd
                        šěčřž\tšěčřž
                        šěčř\tšěčř
                        snthtneusd\tsnthtneusd
                        šěčř\tšěčř
                        šěčř\tšěčř
                        \t
                        šěčř\tšěčř
                        šěčřž\tšěčřž
                        sntht\tsntht
                        šěčř\tšěčř
                        \t
                        sntht\tsntht
                        snthtneusd\tsnthtneusd
                        """);
    }

    @Test
    public void testConstFullyInlinedAscii() throws Exception {
        assertQuery("x where name = 'bac'::varchar")
                .ddl("create table x as (select rnd_varchar(null, 'a', 'bac', 'sntht', 'šěčř', 'gcrlhtneu') name from long_sequence(100))")
                .returns("name\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\nbac\n");
    }

    @Test
    public void testConstFullyInlinedLength9() throws Exception {
        assertQuery("x where name = 'gcrlhtneu'::varchar")
                .ddl("create table x as (select rnd_varchar(null, 'a', 'bac', 'sntht', 'šěčř', 'gcrlhtneu') name from long_sequence(100))")
                .returns("""
                        name
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        gcrlhtneu
                        """);
    }

    @Test
    public void testConstFullyInlinedNonAscii() throws Exception {
        assertQuery("x where name = 'šěčř'::varchar")
                .ddl("create table x as (select rnd_varchar(null, 'a', 'bac', 'sntht', 'šěčř', 'gcrlhtneu') name from long_sequence(100))")
                .returns("name\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\nšěčř\n");
    }

    @Test
    public void testConstSplitAscii() throws Exception {
        assertQuery("x where name = 'qwpgaslvbnsvslhf'::varchar")
                .ddl("create table x as (select rnd_varchar(null, 'gcrlhtneuv', 'gcrlht', 'qwpgaslvbnsvslhf') name from long_sequence(30))")
                .returns("""
                        name
                        qwpgaslvbnsvslhf
                        qwpgaslvbnsvslhf
                        qwpgaslvbnsvslhf
                        qwpgaslvbnsvslhf
                        qwpgaslvbnsvslhf
                        qwpgaslvbnsvslhf
                        qwpgaslvbnsvslhf
                        qwpgaslvbnsvslhf
                        """);
    }

    @Test
    public void testConstSplitNonAscii() throws Exception {
        assertQuery("x where name = 'ěšščř'::varchar")
                .ddl("create table x as (select rnd_varchar(null, 'ěšščř', 'ěššč', 'ěšščřěšščř') name from long_sequence(30))")
                .returns("name\něšščř\něšščř\něšščř\něšščř\něšščř\něšščř\n");
    }
}
