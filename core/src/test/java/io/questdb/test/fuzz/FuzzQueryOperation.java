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

package io.questdb.test.fuzz;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriterAPI;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.std.Chars;
import io.questdb.std.LongList;
import io.questdb.std.Rnd;
import io.questdb.std.str.StringSink;
import io.questdb.test.cairo.fuzz.FailureFileFacade;
import io.questdb.test.tools.TestUtils;

// Reads contents of the table while other transactions may be applied to it in parallel.
// That's to verify that the column files from intermediate txns are not corrupted.
public class FuzzQueryOperation implements FuzzTransactionOperation {
    private final int limit;

    public FuzzQueryOperation(int limit) {
        this.limit = limit;
    }

    @Override
    public boolean apply(
            Rnd rnd,
            CairoEngine engine,
            TableWriterAPI tableWriter,
            int virtualTimestampIndex,
            LongList excludedTsIntervals
    ) {
        FailureFileFacade failureFileFacade = null;
        int failuresObserved = 0;
        if (engine.getConfiguration().getFilesFacade() instanceof FailureFileFacade) {
            failureFileFacade = (FailureFileFacade) engine.getConfiguration().getFilesFacade();
            failuresObserved = failureFileFacade.failureGenerated();
        }
        final TableToken tableToken = tableWriter.getTableToken();
        final StringSink sink = new StringSink();
        try (SqlExecutionContext sqlExecutionContext = new SqlExecutionContextImpl(engine, 1).with(AllowAllSecurityContext.INSTANCE)) {
            TestUtils.printSql(engine, sqlExecutionContext, tableToken.getTableName() + " LIMIT " + limit, sink);
        } catch (SqlException e) {
            if (failureFileFacade != null && failureFileFacade.failureGenerated() > failuresObserved) {
                // Failure facade has generated an error either for us or a concurrent thread.
                // In any case, we can't trust this throwable.
                return false;
            }
            if (e.isTableDoesNotExist()) {
                return false; // drop table transactions are BAU
            }
            if (Chars.contains(e.getFlyweightMessage(), "too many cached query plan cannot be used because table schema has changed")) {
                // Just an unlucky live lock conflict, ignore
                return false;
            }
            throw new RuntimeException(e);
        } catch (Throwable th) {
            if (failureFileFacade != null && failureFileFacade.failureGenerated() > failuresObserved) {
                return false;
            }
            if (th instanceof CairoException ce && ce.isTableDoesNotExist()) {
                return false;
            }
            if (th instanceof TableReferenceOutOfDateException) {
                return false;
            }
            throw th;
        }
        return false;
    }
}
