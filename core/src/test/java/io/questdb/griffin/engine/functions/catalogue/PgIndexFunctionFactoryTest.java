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

package io.questdb.griffin.engine.functions.catalogue;

import io.questdb.griffin.AbstractGriffinTest;
import org.junit.Test;

public class PgIndexFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testPgIndexFunc() throws Exception {
        assertQuery(
                "indexrelid\tindrelid\tindnatts\tindnkeyatts\tindisunique\tindnullsnotdistinct\tindisprimary\tindisexclusion\tindimmediate\tindisclustered\tindisvalid\tindcheckxmin\tindisready\tindislive\tindisreplident\tindkey\tindcollation\tindclass\tindoption\tindexprs\tindpred\n",
                "pg_index;",
                "create table x(a int)",
                null,
                false,
                false,
                true
        );
    }

    @Test
    public void testPrefixedPgIndexFunc() throws Exception {
        assertQuery(
                "indexrelid\tindrelid\tindnatts\tindnkeyatts\tindisunique\tindnullsnotdistinct\tindisprimary\tindisexclusion\tindimmediate\tindisclustered\tindisvalid\tindcheckxmin\tindisready\tindislive\tindisreplident\tindkey\tindcollation\tindclass\tindoption\tindexprs\tindpred\n",
                "pg_catalog.pg_index;",
                "create table x(a int)",
                null,
                false,
                false,
                true
        );
    }
}