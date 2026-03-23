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

package io.questdb.test.griffin.pt;

import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

public class PayloadFunctionTest extends AbstractCairoTest {

    @Test
    public void testPayloadReturnsNullByDefault() throws Exception {
        assertMemoryLeak(() -> assertSql(
                "payload\n\n",
                "SELECT payload()"
        ));
    }

    @Test
    public void testPayloadReturnsSetValue() throws Exception {
        assertMemoryLeak(() -> {
            ((SqlExecutionContextImpl) sqlExecutionContext).setPayload("{\"key\": \"value\"}");
            try {
                assertSql(
                        "payload\n{\"key\": \"value\"}\n",
                        "SELECT payload()"
                );
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).setPayload(null);
            }
        });
    }

    @Test
    public void testPayloadInExpression() throws Exception {
        assertMemoryLeak(() -> {
            ((SqlExecutionContextImpl) sqlExecutionContext).setPayload("hello world");
            try {
                assertSql(
                        "len\n11\n",
                        "SELECT length(payload()) len"
                );
            } finally {
                ((SqlExecutionContextImpl) sqlExecutionContext).setPayload(null);
            }
        });
    }
}
