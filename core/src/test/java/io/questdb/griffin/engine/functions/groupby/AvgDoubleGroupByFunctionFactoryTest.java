/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.AbstractGriffinTest;
import io.questdb.griffin.CompiledQuery;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

public class AvgDoubleGroupByFunctionFactoryTest extends AbstractGriffinTest {

    @Test
    public void testAll() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CompiledQuery cq = compiler.compile("select max(x), avg(x), sum(x) from long_sequence(10)");

            try (RecordCursorFactory factory = cq.getRecordCursorFactory()) {
                sink.clear();
                try (RecordCursor cursor = factory.getCursor()) {
                    printer.print(cursor, factory.getMetadata(), true);
                }
            }

            TestUtils.assertEquals("max\tavg\tsum\n" +
                            "10.000000000000\t5.500000000000\t55\n",
                    sink);

            engine.releaseAllWriters();
            engine.releaseAllReaders();
        });
    }

}