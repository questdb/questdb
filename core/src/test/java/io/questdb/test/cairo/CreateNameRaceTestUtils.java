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

package io.questdb.test.cairo;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.ops.Operation;
import io.questdb.std.str.Path;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class CreateNameRaceTestUtils {
    private static final long WAIT_TIMEOUT_SECONDS = 30;

    private CreateNameRaceTestUtils() {
    }

    public static TableToken newDroppingNonWalTableToken(CairoConfiguration configuration, String tableName) {
        // a non-WAL table directory has no table id, so it is the same every time the name is created
        return new TableToken(tableName, TableUtils.getTableDir(configuration.mangleTableDirNames(), tableName, 0, false), null, 0, false, false, false);
    }

    /**
     * Locks the reader and metadata pools of droppingToken on a separate thread, as a
     * non-WAL DROP holds them after it gave the name back, and runs loserAction while
     * they are locked. The thread releases the pools after holdMillis, or when
     * loserAction returns, whichever comes first.
     */
    public static void runWhileDropHoldsPools(
            CairoEngine engine,
            TableToken droppingToken,
            long holdMillis,
            LoserAction loserAction
    ) throws Exception {
        final CountDownLatch lockedLatch = new CountDownLatch(1);
        final CountDownLatch releaseLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> holderError = new AtomicReference<>();
        final Thread holder = new Thread(() -> {
            try {
                Assert.assertTrue(engine.lockReadersAndMetadata(droppingToken));
                try {
                    lockedLatch.countDown();
                    releaseLatch.await(holdMillis, TimeUnit.MILLISECONDS);
                } finally {
                    engine.unlockReadersAndMetadata(droppingToken);
                }
            } catch (Throwable th) {
                holderError.set(th);
            } finally {
                lockedLatch.countDown();
                Path.clearThreadLocals();
            }
        }, "drop-pool-holder");
        holder.start();
        try {
            Assert.assertTrue(lockedLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            if (holderError.get() != null) {
                throw new AssertionError("holder failed to lock the pools", holderError.get());
            }
            loserAction.run();
        } finally {
            releaseLatch.countDown();
            holder.join(TimeUnit.SECONDS.toMillis(WAIT_TIMEOUT_SECONDS));
        }
        Assert.assertFalse("holder did not finish", holder.isAlive());
        if (holderError.get() != null) {
            throw new AssertionError("holder failed", holderError.get());
        }
    }

    /**
     * Executes winnerDdl on a separate thread and parks that thread in the first call of
     * parkMethodName on the operation that comes after CairoEngine registered the name.
     * That is the window between the name registration and the winner registering its
     * view definition. Runs loserAction while the winner is parked, then releases the
     * winner and waits for it to finish.
     */
    public static void runWhileWinnerParkedAfterNameRegistration(
            CairoEngine engine,
            CharSequence winnerDdl,
            Class<? extends Operation> operationInterface,
            String parkMethodName,
            CharSequence name,
            LoserAction loserAction
    ) throws Exception {
        final CountDownLatch parkedLatch = new CountDownLatch(1);
        final CountDownLatch releaseLatch = new CountDownLatch(1);
        final AtomicBoolean isParked = new AtomicBoolean();
        final AtomicReference<Throwable> winnerError = new AtomicReference<>();
        final Thread winner = new Thread(() -> {
            try (
                    SqlExecutionContext winnerContext = TestUtils.createSqlExecutionCtx(engine);
                    SqlCompiler compiler = engine.getSqlCompiler()
            ) {
                final CompiledQuery cq = compiler.compile(winnerDdl, winnerContext);
                try (Operation op = cq.getOperation()) {
                    final Operation parkingOp = (Operation) Proxy.newProxyInstance(
                            operationInterface.getClassLoader(),
                            new Class<?>[]{operationInterface},
                            (proxy, method, args) -> {
                                if (method.getName().equals(parkMethodName)
                                        && engine.getTableTokenIfExists(name) != null
                                        && isParked.compareAndSet(false, true)) {
                                    parkedLatch.countDown();
                                    if (!releaseLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                                        throw new IllegalStateException("winner was not released");
                                    }
                                }
                                try {
                                    return method.invoke(op, args);
                                } catch (InvocationTargetException e) {
                                    throw e.getCause();
                                }
                            }
                    );
                    compiler.execute(parkingOp, winnerContext);
                }
            } catch (Throwable th) {
                winnerError.set(th);
            } finally {
                parkedLatch.countDown();
                Path.clearThreadLocals();
            }
        }, "create-name-race-winner");
        winner.start();
        try {
            Assert.assertTrue("winner did not reach the name registration", parkedLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            if (winnerError.get() != null) {
                throw new AssertionError("winner failed before it parked", winnerError.get());
            }
            Assert.assertTrue("winner did not park after the name registration", isParked.get());
            loserAction.run();
        } finally {
            releaseLatch.countDown();
            winner.join(TimeUnit.SECONDS.toMillis(WAIT_TIMEOUT_SECONDS));
        }
        Assert.assertFalse("winner did not finish", winner.isAlive());
        if (winnerError.get() != null) {
            throw new AssertionError("winner failed", winnerError.get());
        }
    }

    @FunctionalInterface
    public interface LoserAction {
        void run() throws Exception;
    }
}
