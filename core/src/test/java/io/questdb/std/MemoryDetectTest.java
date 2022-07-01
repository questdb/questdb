/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.std;

import io.questdb.ServerMain;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.tools.TestUtils;
import org.junit.*;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class MemoryDetectTest {
    private static final Log LOG = LogFactory.getLog(MemoryDetectTest.class);
    private static long RSS_MEMORY_LIMIT = Long.MAX_VALUE;

    @BeforeClass
    public static void beforeClass() {
        RSS_MEMORY_LIMIT = Unsafe.getRssMemoryLimit();
    }

    @Test
    public void testTotalPhysical() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Object attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
        long mem = (Long)attribute;
        Assert.assertTrue(mem > 0);
        System.out.printf("%,d%n", mem);
    }

    @Test
    public void testFreePhysical() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Object attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreePhysicalMemorySize");
        long mem = (Long)attribute;
        Assert.assertTrue(mem > 0);
        System.out.printf("%,d%n", mem);
    }

    @Test
    public void testFreeSwap() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Object attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "FreeSwapSpaceSize");
        long mem = (Long)attribute;
        Assert.assertTrue(mem >= 0);
        System.out.printf("%,d%n", mem);
    }

    @Test
    public void testSetsUnsafeRssLimit() {
        try {
            ServerMain.setRssMemoryLimit(LOG, 0L);
            Assert.assertTrue("RSS limit evaluated", Unsafe.getRssMemoryLimit() < (1L << 40));
        } finally {
            resetRssLimit();
        }
    }

    @Test
    public void testResetsUnsafeRssLimit() {
        try {
            ServerMain.setRssMemoryLimit(LOG, -1L);
            Assert.assertEquals("RSS limit reset", Long.MAX_VALUE, Unsafe.getRssMemoryLimit());
            Assert.assertTrue("RSS limit reset", Unsafe.OFF_HEAP_CHECK_THRESHOLD > (1L << 40));
        } finally {
            resetRssLimit();
        }
    }

    @Test
    public void testSetsUnsafeRssLimitToConcreteValue() {
        try {
            ServerMain.setRssMemoryLimit(LOG, 1 << 30L);
            Assert.assertEquals("RSS limit reset", 1 << 30L, Unsafe.getRssMemoryLimit());
        } finally {
            resetRssLimit();
        }
    }

    @Test
    public void testOomWhenMemoryExceeded() {
        long gib = 1L << 30;
        Unsafe.setRssMemoryLimit(gib);
        long offheapAllocated = Unsafe.getOffHeapAllocated();

        try {
            Unsafe.malloc(gib, MemoryTag.NATIVE_DEFAULT);
            Assert.fail();
        } catch (OutOfMemoryError err) {
            TestUtils.assertContains(err.getMessage(), "exceeded configured limit of 1,073,741,824");
            Assert.assertEquals(offheapAllocated, Unsafe.getOffHeapAllocated());
        } finally {
            // Restore global limit
            resetRssLimit();
        }
    }

    @Test
    public void testReallocOomWhenMemoryExceeded() {
        long gib = 1L << 30;
        long ptr = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
        Unsafe.setRssMemoryLimit(gib);

        long offheapAllocated = Unsafe.getOffHeapAllocated();
        try {
            Unsafe.realloc(ptr, gib / 100, gib, MemoryTag.NATIVE_DEFAULT);
            Assert.fail();
        } catch (OutOfMemoryError err) {
            TestUtils.assertContains(err.getMessage(), "exceeded configured limit of 1,073,741,824");
            Assert.assertEquals(offheapAllocated, Unsafe.getOffHeapAllocated());
        } finally {
            Unsafe.free(ptr, 1, MemoryTag.NATIVE_DEFAULT);
            // Restore global limit
            resetRssLimit();
        }
    }

    @Test
    public void testOffHeapAllocationReevaluatesCheckThreshold() {
        // Windows likely to fail to allocate big block of memory
        Assume.assumeTrue(Os.type != Os.WINDOWS);

        try {
            ServerMain.setRssMemoryLimit(LOG, 0L);
            long fiveMib = 5L * (1 << 20);
            long offHeapCheckThreshold = Unsafe.OFF_HEAP_CHECK_THRESHOLD;
            long offheapAllocated = Unsafe.getOffHeapAllocated();
            long size = offHeapCheckThreshold - offheapAllocated + fiveMib;

            long ptr = 0L;
            if (size > 0) {
                boolean failedToAllocate = false;
                try {
                    ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
                    Assert.assertEquals(offheapAllocated + size, Unsafe.getOffHeapAllocated());
                } catch (OutOfMemoryError err) {
                    // Nothing we can do OS, cannot allocate single big chunk sometimes
                    failedToAllocate = true;
                } finally {
                    if (ptr != 0) {
                        Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
                    }
                }

                if (!failedToAllocate) {
                    Assert.assertEquals(offheapAllocated, Unsafe.getOffHeapAllocated());
                    Assert.assertTrue(Unsafe.OFF_HEAP_CHECK_THRESHOLD > offHeapCheckThreshold);
                }
            }
        } finally {
            // Restore global limit
            resetRssLimit();
        }
    }

    private void resetRssLimit() {
        Unsafe.setRssMemoryLimit(RSS_MEMORY_LIMIT);
    }

}
