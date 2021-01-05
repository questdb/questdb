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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class DescriptionCatalogueFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testPgDescriptionFunc() throws Exception {
        assertQuery(
                "objoid\tclassoid\tobjsubid\tdescription\n" +
                        "1\t1259\t0\ttable\n" +
                        "1\t1259\t1\tcolumn\n" +
                        "11\t2615\t0\tdescription\n" +
                        "2200\t2615\t0\tdescription\n",
                "pg_catalog.pg_description;",
                "create table x(a int)",
                null,
                false,
                false
        );
    }

    @Test
    public void testNoPrefixPgDescriptionFunc() throws Exception {
        assertQuery(
                "objoid\tclassoid\tobjsubid\tdescription\n" +
                        "1\t1259\t0\ttable\n" +
                        "1\t1259\t1\tcolumn\n" +
                        "11\t2615\t0\tdescription\n" +
                        "2200\t2615\t0\tdescription\n",
                "pg_description;",
                "create table x(a int)",
                null,
                false,
                false
        );
    }
}