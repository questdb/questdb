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

package io.questdb.test.griffin;

import io.questdb.cairo.ColumnType;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Sinkable;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

public class SqlExceptionTest extends AbstractCairoTest {
    @Test
    public void testDuplicateColumn() {
        TestUtils.assertEquals(
                "[17] Duplicate column [name=COL-0]",
                SqlException.duplicateColumn(17, "COL-0").getMessage()
        );

        TestUtils.assertEquals(
                "[2022] Duplicate column [name=我们是最棒的]",
                SqlException.duplicateColumn(2022, "我们是最棒的").getMessage()
        );
    }

    @Test
    public void testParserErr() {
        TestUtils.assertEquals(
                "[17] found [tok=')', len=1] expected ',', or 'colName'",
                SqlException.parserErr(17, ")", "expected ',', or 'colName'").getMessage()
        );

        TestUtils.assertEquals(
                "[17] expected ',', or 'colName'",
                SqlException.parserErr(17, null, "expected ',', or 'colName'").getMessage()
        );
    }

    @Test
    public void testSinkable() {
        sink.clear();
        sink.put((Sinkable) SqlException.$(123, "hello"));
        TestUtils.assertEquals("[123]: hello", sink);
    }

    @Test
    public void testTableBusyFlagDoesNotChangeSqlError() {
        final SqlException exception = SqlException.$(17, "pool unavailable").setTableBusy(true);
        Assert.assertTrue(exception.isTableBusy());
        Assert.assertEquals(0, exception.getErrorCode());
        Assert.assertEquals(17, exception.getPosition());
        TestUtils.assertEquals("[17] pool unavailable", exception.getMessage());
        TestUtils.assertEquals("[17]: pool unavailable", exception);

        exception.setTableBusy(false);
        Assert.assertFalse(exception.isTableBusy());
    }

    @Test
    public void testTableBusyFlagResets() {
        final SqlException busy = SqlException.position(17).setTableBusy(true);
        Assert.assertTrue(busy.isTableBusy());

        // Matching error text alone must not classify a fresh exception as transient.
        final SqlException other = SqlException.$(23, "[-1]: table busy [reason=pool size exceeded]");
        Assert.assertFalse(other.isTableBusy());
        Assert.assertEquals(23, other.getPosition());
    }

    @Test
    public void testTableBusyFlagResetsOnFlyweightReuse() throws Exception {
        // -ea makes position() allocate fresh exceptions. Load just SqlException without assertions
        // to verify that its production flyweight clears the flag before the next throw.
        final Class<?> exceptionClass = TestUtils.loadSqlExceptionWithAssertionsDisabled();
        Assert.assertFalse(exceptionClass.desiredAssertionStatus());
        final Method position = exceptionClass.getMethod("position", int.class);
        final Method isTableBusy = exceptionClass.getMethod("isTableBusy");
        final Object busy = position.invoke(null, 17);
        exceptionClass.getMethod("setTableBusy", boolean.class).invoke(busy, true);
        Assert.assertEquals(true, isTableBusy.invoke(busy));

        final Object other = position.invoke(null, 23);
        Assert.assertSame(busy, other);
        Assert.assertEquals(false, isTableBusy.invoke(other));
    }

    @Test
    public void testUnsupportedCast() {
        TestUtils.assertEquals("[10]: unsupported cast [column=columnName, from=SYMBOL, to=BINARY]",
                SqlException.unsupportedCast(10, "columnName", ColumnType.SYMBOL, ColumnType.BINARY));
    }

}