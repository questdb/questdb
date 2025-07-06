package io.questdb.test.griffin;

import io.questdb.test.AbstractGriffinTest;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PGWireConcurrencyTest extends AbstractGriffinTest {

    @Test
    public void testConcurrentInsertionsShouldNotCrash() throws Exception {
        int threads = 10;
        int insertsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        ddl("CREATE TABLE t1(ts timestamp, val int) timestamp(ts) PARTITION BY DAY");

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < insertsPerThread; j++) {
                        insert("INSERT INTO t1 VALUES (now(), " + j + ")");
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertRowCount("SELECT COUNT(*) FROM t1", threads * insertsPerThread);
    }
}
