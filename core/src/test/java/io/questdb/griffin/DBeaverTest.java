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

package io.questdb.griffin;

import org.junit.Test;

public class DBeaverTest extends AbstractGriffinTest {
    @Test
    public void testFrequentSql() throws SqlException {
        assertQuery(
                "current_schema\tsession_user\n" +
                        "public\tadmin\n",
                "SELECT current_schema(),session_user",
                null,
                true,
                sqlExecutionContext,
                false,
                true
        );
    }

    @Test
    public void testNamespaceListSql() throws SqlException {
        String sql = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n\n" +
                "LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass\n" +
                " ORDER BY nspname";

        assertQuery(
                "oid\tnspname\toid1\tdescription\n" +
                        "11\tpg_catalog\t11\tdescription\n" +
                        "2200\tpublic\t2200\tdescription\n",
                sql,
                null,
                true,
                sqlExecutionContext,
                false,
                false
        );
    }

    @Test
    public void testShowSearchPath() throws SqlException {
        String sql = "SHOW search_path";
        assertQuery(
                "search_path\n" +
                        "\"$user\", public\n",
                sql,
                null,
                false,
                sqlExecutionContext,
                false,
                true
        );
    }
}
