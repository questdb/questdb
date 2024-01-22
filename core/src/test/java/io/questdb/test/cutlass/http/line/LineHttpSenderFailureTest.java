package io.questdb.test.cutlass.http.line;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.pool.ex.EntryLockedException;
import io.questdb.client.Sender;
import io.questdb.std.Misc;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.assertEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LineHttpSenderFailureTest extends AbstractBootstrapTest {

    public static void assertTableExists(CairoEngine engine, CharSequence tableName) {
        try (Path path = new Path()) {
            assertEquals(TableUtils.TABLE_EXISTS, engine.getTableStatus(path, engine.getTableTokenIfExists(tableName)));
        }
    }

    public static void assertTableExistsEventually(CairoEngine engine, CharSequence tableName) {
        assertEventually(() -> assertTableExists(engine, tableName));
    }

    public static void assertTableSizeEventually(CairoEngine engine, CharSequence tableName, long expectedSize) {
        TestUtils.assertEventually(() -> {
            assertTableExists(engine, tableName);

            try (TableReader reader = engine.getReader(tableName)) {
                long size = reader.getCursor().size();
                assertEquals(expectedSize, size);
            } catch (EntryLockedException e) {
                // if table is busy we want to fail this round and have the assertEventually() to retry later
                fail("table +" + tableName + " is locked");
            }
        });
    }

    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testRetryWithDeduplication() throws Exception {
        String tableName = UUID.randomUUID().toString();
        TestUtils.assertMemoryLeak(() -> {
            try (ServerController controller = new ServerController()) {
                controller.startAndExecute("create table '" + tableName + "' (value long, ts timestamp) timestamp (ts) partition by DAY WAL DEDUP UPSERT KEYS(ts)");
                CountDownLatch senderLatch = new CountDownLatch(2); // one for Sender and one for Restarter

                AtomicReference<Exception> senderException = new AtomicReference<>();
                new Thread(() -> {
                    String url = "http://localhost:" + HTTP_PORT;
                    try (Sender sender = Sender.builder().url(url).maxPendingRows(100).retryTimeoutMillis(1000).build()) {
                        for (int i = 0; i < 1_000_000; i++) {
                            sender.table(tableName).longColumn("value", 42).at(i * 10, ChronoUnit.MICROS);
                        }
                    } catch (Exception t) {
                        senderException.set(t);
                    } finally {
                        senderLatch.countDown();
                    }
                }).start();

                new Thread(() -> {
                    // keeping restarting the server until Sender is done
                    while (senderLatch.getCount() == 2) {
                        Os.sleep(500);
                        controller.restart();
                    }
                    controller.stop(); // stop will clear the thread local Path
                    senderLatch.countDown();
                }).start();

                senderLatch.await();

                if (senderException.get() != null) {
                    Assert.fail("Sender failed: " + senderException.get().getMessage());
                }
                controller.start();
                assertTableSizeEventually(controller.getEngine(), tableName, 1_000_000);
            }
        });
    }

    static class ServerController implements Closeable {
        TestServerMain serverMain;

        @Override
        public void close() {
            serverMain = Misc.free(serverMain);
        }

        CairoEngine getEngine() {
            return serverMain.getEngine();
        }

        void restart() {
            stop();
            start();
        }

        void start() {
            serverMain = startWithEnvVariables();
            serverMain.start();
        }

        void startAndExecute(String sqlText) {
            start();
            serverMain.compile(sqlText);
        }

        void stop() {
            serverMain = Misc.free(serverMain);
            Path.clearThreadLocals();
        }
    }
}
