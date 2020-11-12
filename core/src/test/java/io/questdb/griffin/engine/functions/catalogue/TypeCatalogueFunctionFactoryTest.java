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

public class TypeCatalogueFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testPgTypeFunc() throws Exception {
        assertQuery(
                "oid\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\n" +
                        "16\tBOOLEAN\t0\t0\t2200\tfalse\t0\tb\n" +
                        "21\tBYTE\t0\t0\t2200\tfalse\t0\tb\n" +
                        "21\tSHORT\t0\t0\t2200\tfalse\t0\tb\n" +
                        "18\tCHAR\t0\t0\t2200\tfalse\t0\tb\n" +
                        "23\tINT\t0\t0\t2200\tfalse\t0\tb\n" +
                        "20\tLONG\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1114\tDATE\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1114\tTIMESTAMP\t0\t0\t2200\tfalse\t0\tb\n" +
                        "700\tFLOAT\t0\t0\t2200\tfalse\t0\tb\n" +
                        "701\tDOUBLE\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1043\tSTRING\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1043\tSYMBOL\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1700\tLONG256\t0\t0\t2200\tfalse\t0\tb\n" +
                        "17\tBINARY\t0\t0\t2200\tfalse\t0\tb\n",
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
                "oid\ttypname\ttypbasetype\ttyparray\ttypnamespace\ttypnotnull\ttyptypmod\ttyptype\n" +
                        "16\tBOOLEAN\t0\t0\t2200\tfalse\t0\tb\n" +
                        "21\tBYTE\t0\t0\t2200\tfalse\t0\tb\n" +
                        "21\tSHORT\t0\t0\t2200\tfalse\t0\tb\n" +
                        "18\tCHAR\t0\t0\t2200\tfalse\t0\tb\n" +
                        "23\tINT\t0\t0\t2200\tfalse\t0\tb\n" +
                        "20\tLONG\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1114\tDATE\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1114\tTIMESTAMP\t0\t0\t2200\tfalse\t0\tb\n" +
                        "700\tFLOAT\t0\t0\t2200\tfalse\t0\tb\n" +
                        "701\tDOUBLE\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1043\tSTRING\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1043\tSYMBOL\t0\t0\t2200\tfalse\t0\tb\n" +
                        "1700\tLONG256\t0\t0\t2200\tfalse\t0\tb\n" +
                        "17\tBINARY\t0\t0\t2200\tfalse\t0\tb\n",
                "pg_catalog.pg_type;",
                "create table x(a int)",
                null,
                false,
                false,
                true
        );
    }
}