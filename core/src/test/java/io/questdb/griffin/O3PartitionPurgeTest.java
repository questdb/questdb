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

package io.questdb.griffin;

import io.questdb.cairo.TableReader;
import org.junit.Test;

public class O3PartitionPurgeTest extends AbstractGriffinTest {
    @Test
    public void testReaderUsesPartition() throws SqlException {
        compiler.compile("create table tbl as (select x, cast('1970-01-10T10' as timestamp) ts from long_sequence(1)) timestamp(ts) partition by DAY", sqlExecutionContext);

        // OOO insert
        compiler.compile("insert into tbl select 4, '1970-01-10T09'", sqlExecutionContext);

        // This should lock partition 1970-01-10.1 from being deleted from disk
        try (TableReader rdr = engine.getReader(sqlExecutionContext.getCairoSecurityContext(), "tbl")) {

            // in order insert
            compiler.compile("insert into tbl select 2, '1970-01-10T11'", sqlExecutionContext);

            // OOO insert
            compiler.compile("insert into tbl select 4, '1970-01-10T09'", sqlExecutionContext);

            // This should not fail
            rdr.openPartition(0);
        }
    }
}
