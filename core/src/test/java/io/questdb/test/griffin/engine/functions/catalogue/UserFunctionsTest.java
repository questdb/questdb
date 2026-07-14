/*******************************************************************************
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

package io.questdb.test.griffin.engine.functions.catalogue;

import io.questdb.cairo.SecurityContext;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class UserFunctionsTest extends AbstractCairoTest {

    private static final String SQL = "SELECT current_user(), session_user() FROM long_sequence(";

    @Test
    public void testUserFunctionsResolveOncePerCursorNotPerRow() throws Exception {
        // current_user() and session_user() do not depend on the record -- which is exactly what their
        // factories have always declared through isRuntimeConstant(). The functions themselves did not, so
        // nothing downstream could act on it and the value was re-read on EVERY row. On the enterprise
        // dispatching proxy each read routes through a cached delegate: an atomic load, a volatile load and a
        // virtual call, per row, for a value that cannot change mid-traversal.
        //
        // Assert the shape rather than a magic number: the number of times the query reads the security
        // context must not grow with the row count. Before the fix, a 1000-row projection made 1000 reads.
        assertMemoryLeak(() -> {
            final CountingSecurityContext small = readPrincipalsOver(10);
            final CountingSecurityContext large = readPrincipalsOver(10_000);

            Assert.assertEquals("current_user() must resolve once per cursor, not once per row",
                    small.principalCalls, large.principalCalls);
            Assert.assertEquals("session_user() must resolve once per cursor, not once per row",
                    small.sessionPrincipalCalls, large.sessionPrincipalCalls);

            // and it is genuinely resolved -- zero reads would mean the projection never asked at all
            Assert.assertTrue(large.principalCalls > 0);
            Assert.assertTrue(large.sessionPrincipalCalls > 0);
        });
    }

    @Test
    public void testUserFunctionsRefreshOnEveryCursor() throws Exception {
        // Resolving in init() is only sound because init() runs on every getCursor(). Pin that, because the
        // failure mode is ugly: the HTTP select cache is shared across connections, so a compiled factory is
        // routinely reused by a different user. A snapshot that survived into the next cursor would report
        // the previous user's identity to this one.
        assertMemoryLeak(() -> {
            try (RecordCursorFactory factory = select(SQL + "1)")) {
                assertReportsUser(factory, "alice");
                assertReportsUser(factory, "bob");
                // and back again, so it tracks the context rather than merely changing once
                assertReportsUser(factory, "alice");
            }
        });
    }

    private void assertReportsUser(RecordCursorFactory factory, String user) throws SqlException {
        ((SqlExecutionContextImpl) sqlExecutionContext).with(new CountingSecurityContext(user));
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            final Record record = cursor.getRecord();
            Assert.assertTrue(cursor.hasNext());
            TestUtils.assertEquals(user, record.getStrA(0));
            TestUtils.assertEquals(user, record.getStrA(1));
            Assert.assertFalse(cursor.hasNext());
        }
    }

    private CountingSecurityContext readPrincipalsOver(int rowCount) throws SqlException {
        final CountingSecurityContext securityContext = new CountingSecurityContext("bob");
        ((SqlExecutionContextImpl) sqlExecutionContext).with(securityContext);
        try (
                RecordCursorFactory factory = select(SQL + rowCount + ")");
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            final Record record = cursor.getRecord();
            int rows = 0;
            while (cursor.hasNext()) {
                TestUtils.assertEquals("bob", record.getStrA(0));
                TestUtils.assertEquals("bob", record.getStrA(1));
                rows++;
            }
            Assert.assertEquals(rowCount, rows);
        }
        return securityContext;
    }

    private static class CountingSecurityContext extends AllowAllSecurityContext {
        final String user;
        int principalCalls;
        int sessionPrincipalCalls;

        CountingSecurityContext(String user) {
            this.user = user;
        }

        @Override
        public CharSequence getPrincipal() {
            principalCalls++;
            return user;
        }

        @Override
        public CharSequence getSessionPrincipal() {
            sessionPrincipalCalls++;
            return user;
        }

        @Override
        protected SecurityContext newPrincipalContext(CharSequence principal) {
            return this;
        }
    }
}
