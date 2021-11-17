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

import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.UpdateStatement;
import org.junit.Assert;
import org.junit.Test;

public class UpdateBasicTest extends AbstractGriffinTest {
    @Test
    public void testUpdateNoFilter() throws SqlException {
        compiler.compile("create table up as" +
                " (select timestamp_sequence(0, 1000000) ts," +
                " x" +
                " from long_sequence(100))" +
                " timestamp(ts) partition by DAY", sqlExecutionContext);

        CompiledQuery cc = compiler.compile("UPDATE up SET x = 1", sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
        applyUpdate(cc.getUpdateStatement());
    }

    private void applyUpdate(UpdateStatement updateStatement) throws SqlException {
        try(TableWriter tableWriter = engine.getWriter(sqlExecutionContext.getCairoSecurityContext(), updateStatement.getUpdateTableName(), "UPDATE")) {
            tableWriter.executeUpdate(updateStatement, sqlExecutionContext);
        }
    }
}
