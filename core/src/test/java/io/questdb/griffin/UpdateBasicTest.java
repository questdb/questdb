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
                " cast(x as int) x" +
                " from long_sequence(5))" +
                " timestamp(ts) partition by DAY", sqlExecutionContext);

        executeUpdate("UPDATE up SET x = 1");

        assertSql("up", "ts\tx\n" +
                "1970-01-01T00:00:00.000000Z\t1\n" +
                "1970-01-01T00:00:01.000000Z\t1\n" +
                "1970-01-01T00:00:02.000000Z\t1\n" +
                "1970-01-01T00:00:03.000000Z\t1\n" +
                "1970-01-01T00:00:04.000000Z\t1\n");
    }

    @Test
    public void testUpdateWithFilterAndFunction() throws SqlException {
        compiler.compile("create table up as" +
                " (select timestamp_sequence(0, 1000000) ts," +
                " cast(x as int) x," +
                " x as y" +
                " from long_sequence(5))" +
                " timestamp(ts) partition by DAY", sqlExecutionContext);

        executeUpdate("UPDATE up SET y = 10L * x WHERE x > 1 and x < 4");

        assertSql("up", "ts\tx\ty\n" +
                "1970-01-01T00:00:00.000000Z\t1\t1\n" +
                "1970-01-01T00:00:01.000000Z\t2\t20\n" +
                "1970-01-01T00:00:02.000000Z\t3\t30\n" +
                "1970-01-01T00:00:03.000000Z\t4\t4\n" +
                "1970-01-01T00:00:04.000000Z\t5\t5\n");
    }

    private void executeUpdate(String query) throws SqlException {
        CompiledQuery cc = compiler.compile(query, sqlExecutionContext);
        Assert.assertEquals(CompiledQuery.UPDATE, cc.getType());
        try (UpdateStatement updateStatement = cc.getUpdateStatement()) {
            applyUpdate(updateStatement);
        }
    }

    @Test
    public void testUpdateWithFilterAndFunctionValueUpcast() throws SqlException {
        compiler.compile("create table up as" +
                " (select timestamp_sequence(0, 1000000) ts," +
                " cast(x as int) x," +
                " x as y" +
                " from long_sequence(5))" +
                " timestamp(ts) partition by DAY", sqlExecutionContext);

        executeUpdate("UPDATE up SET y = 10 * x WHERE x > 1 and x < 4");

        assertSql("up", "ts\tx\ty\n" +
                "1970-01-01T00:00:00.000000Z\t1\t1\n" +
                "1970-01-01T00:00:01.000000Z\t2\t20\n" +
                "1970-01-01T00:00:02.000000Z\t3\t30\n" +
                "1970-01-01T00:00:03.000000Z\t4\t4\n" +
                "1970-01-01T00:00:04.000000Z\t5\t5\n");
    }

    private void applyUpdate(UpdateStatement updateStatement) throws SqlException {
        try (TableWriter tableWriter = engine.getWriter(
                sqlExecutionContext.getCairoSecurityContext(),
                updateStatement.getUpdateTableName(),
                "UPDATE")) {
            tableWriter.executeUpdate(updateStatement, sqlExecutionContext);
        }
    }
}
