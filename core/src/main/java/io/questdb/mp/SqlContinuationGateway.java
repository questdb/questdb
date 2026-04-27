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

package io.questdb.mp;

import io.questdb.griffin.SqlExecutionContext;

import java.util.concurrent.CountDownLatch;

/**
 * Synchronous continuation gateway. Wraps a body of SQL-evaluation work in a
 * {@link SqlContinuation}, binds it to the execution context so suspending functions
 * (e.g. {@code wait_wal_table}) can discover it, and blocks the caller on a latch if
 * the body suspends. A worker thread running
 * {@link ContinuationResumeJob} eventually resumes the continuation; when its body
 * completes, it counts down the latch and the caller returns.
 *
 * <p>This gateway keeps today's blocking semantics (the caller thread waits), but
 * routes the call through the continuation path so the thread migration works end to
 * end. An async gateway that releases the caller thread on suspend and flushes the
 * response from the resuming worker is a separate, larger piece of work in the
 * protocol handlers.
 *
 * <p>Propagation of exceptions: if the body throws, the exception is captured and
 * re-thrown on the caller thread. Runtime exceptions are rethrown unchanged; checked
 * exceptions are wrapped in {@link RuntimeException}.
 */
public final class SqlContinuationGateway {

    private SqlContinuationGateway() {
    }

    /**
     * Executes {@code body} inside a SqlContinuation bound to {@code ctx}. Blocks the
     * caller until the body completes, possibly across a suspend/resume migration.
     * Exceptions thrown by the body are captured and re-thrown on the caller thread;
     * checked exceptions propagate via the {@code E} type parameter.
     */
    public static void execute(SqlExecutionContext ctx, Runnable body) {
        CountDownLatch done = new CountDownLatch(1);
        Throwable[] failure = new Throwable[1];
        SqlContinuation cont = new SqlContinuation(() -> {
            try {
                body.run();
            } catch (Throwable t) {
                failure[0] = t;
            } finally {
                done.countDown();
            }
        });

        SqlContinuation previous = ctx.getCurrentContinuation();
        ctx.setCurrentContinuation(cont);
        try {
            cont.run();
            if (!cont.isDone()) {
                try {
                    done.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        } finally {
            ctx.setCurrentContinuation(previous);
        }

        if (failure[0] != null) {
            sneakyThrow(failure[0]);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
        throw (T) t;
    }

}
