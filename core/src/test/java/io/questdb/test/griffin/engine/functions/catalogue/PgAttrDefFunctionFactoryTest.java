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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PgAttrDefFunctionFactoryTest extends AbstractCairoTest {

    @Test
    public void testPgAttrDefFunc() throws Exception {
        assertQuery("pg_catalog.pg_attrdef;")
                .ddl(null)
                .noRandomAccess()
                .returns("adrelid\tadnum\tadbin\n");
    }

    @Test
    public void testPgAttrDefFuncWith2Tables() throws Exception {
        assertQuery("pg_catalog.pg_attrdef order by 1, 2;")
                .ddl("create table x(a int)")
                .mutateWith("create table y(a double, b string)")
                .returns("""
                        adrelid\tadnum\tadbin
                        1\t1\t
                        """, """
                        adrelid\tadnum\tadbin
                        1\t1\t
                        2\t1\t
                        2\t2\t
                        """);
    }

    @Test
    public void testPgAttrDefFuncWithOneTable() throws Exception {
        assertQuery("pg_catalog.pg_attrdef;")
                .ddl("create table x(a int)")
                .noRandomAccess()
                .returns("""
                        adrelid\tadnum\tadbin
                        1\t1\t
                        """);
    }
}