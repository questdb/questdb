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


package io.questdb.test.jit;

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.plan.logical.FilterPlan;
import io.questdb.griffin.plan.logical.LogicalPlan;
import org.junit.Assert;

/**
 * Binds a WHERE clause through the compiler and hands back the filter plan the code generator
 * sees, so JIT tests feed the IR serializer the same bound shapes production does.
 */
public final class JitFilterBinding {
    private JitFilterBinding() {
    }

    /**
     * The plan is borrowed from {@code compiler} and stays valid until the compiler is used again.
     */
    public static FilterPlan bind(
            SqlCompilerImpl compiler,
            SqlExecutionContext executionContext,
            CharSequence tableName,
            CharSequence where
    ) throws SqlException {
        try (RecordCursorFactory ignore = compiler.compile("SELECT * FROM " + tableName + " WHERE " + where, executionContext).getRecordCursorFactory()) {
            final FilterPlan filter = findFilter(compiler.getPlanForTesting());
            Assert.assertNotNull("no filter plan for: " + where, filter);
            return filter;
        }
    }

    private static FilterPlan findFilter(LogicalPlan plan) {
        if (plan instanceof FilterPlan filter) {
            return filter;
        }
        for (int i = 0, n = plan.inputCount(); i < n; i++) {
            final LogicalPlan input = plan.inputAt(i);
            final FilterPlan filter = input != null ? findFilter(input) : null;
            if (filter != null) {
                return filter;
            }
        }
        return null;
    }
}
