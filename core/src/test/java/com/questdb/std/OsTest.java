package com.questdb.std;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class OsTest {
    @Test
    public void testAffinity() throws Exception {
        Assert.assertEquals(0, Os.setCurrentThreadAffinity(0));

        AtomicInteger result = new AtomicInteger(-1);
        CountDownLatch threadHalt = new CountDownLatch(1);

        new Thread(() -> {
            result.set(Os.setCurrentThreadAffinity(1));
            threadHalt.countDown();
        }).start();

        Assert.assertTrue(threadHalt.await(1, TimeUnit.SECONDS));
        Assert.assertEquals(0, result.get());

        Assert.assertEquals(0, Os.setCurrentThreadAffinity(-1));
    }
}