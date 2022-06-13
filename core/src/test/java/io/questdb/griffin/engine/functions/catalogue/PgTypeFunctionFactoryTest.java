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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class PgTypeFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testPgTypeFunc() throws Exception {
        assertQuery(
                "oid\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\ttyprelid\ttypelem\ttypreceive\ttypdelim\ttypinput\n" +
                        "1043\tvarchar\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "1114\ttimestamp\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "701\tfloat8\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "700\tfloat4\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "23\tint4\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "21\tint2\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "18\tchar\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "20\tint8\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "16\tbool\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "17\tbinary\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "1082\tdate\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n",
                "pg_type;",
                "create table x(a int)",
                null,
                false,
                false,
                true
        );
    }

    @Test
    public void testPrefixedPgTypeFunc() throws Exception {
        assertQuery(
                "oid\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\ttyprelid\ttypelem\ttypreceive\ttypdelim\ttypinput\n" +
                        "1043\tvarchar\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "1114\ttimestamp\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "701\tfloat8\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "700\tfloat4\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "23\tint4\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "21\tint2\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "18\tchar\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "20\tint8\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "16\tbool\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "17\tbinary\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n" +
                        "1082\tdate\t0\t0\t2200\tfalse\t0\tb\tNaN\t0\t0\t0\t0\n",
                "pg_catalog.pg_type;",
                "create table x(a int)",
                null,
                false,
                false,
                true
        );
    }
}