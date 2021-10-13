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

import io.questdb.cairo.sql.PageFrame;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.Ignore;
import org.junit.Test;

public class ReplDataAccessTest extends AbstractGriffinTest {

    @Test
    @Ignore
    public void testSimple() throws SqlException {
        compiler.compile("create table x as (select" +
                " rnd_int() a," +
                        " rnd_str() b," +
                        " timestamp_sequence(0, 1000000) t" +
                        " from long_sequence(10000000)" +
                        ") timestamp (t) partition by DAY",
                sqlExecutionContext
        );

/*
        TestUtils.printSql(
                compiler,
                sqlExecutionContext,
                "x",
                sink
        );

        System.out.println(sink);
*/

        RecordCursorFactory factory = compiler.compile("x", sqlExecutionContext).getRecordCursorFactory();
        PageFrameCursor pageFrameCursor = factory.getPageFrameCursor(sqlExecutionContext);
        PageFrame frame;
        while ((frame = pageFrameCursor.next()) != null) {
            System.out.println("f");
        }
    }

}
