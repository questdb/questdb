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

import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class MemoryDetectTest {
    @Test
    public void testPhysical() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, NumericException {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Object attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalPhysicalMemorySize");
        long mem = Numbers.parseLong(attribute.toString());
        Assert.assertTrue(mem > 0);
        System.out.printf("%,d%n", mem);
    }

    @Test
    public void testSwapl() throws MalformedObjectNameException, ReflectionException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, NumericException {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        Object attribute = mBeanServer.getAttribute(new ObjectName("java.lang","type","OperatingSystem"), "TotalSwapSpaceSize");
        long mem = Numbers.parseLong(attribute.toString());
        Assert.assertTrue(mem >= 0);
        System.out.printf("%,d%n", mem);
    }

    @Test
    public void testOomWhenMemoryExceeded() {
        long gb = 1024L * 1024L * 1024L;
        Unsafe.setMallocMemoryLimit((long) (0.5 * gb));

        try {
            Unsafe.malloc(gb, MemoryTag.NATIVE_DEFAULT);
            Assert.fail();
        } catch (OutOfMemoryError err) {
            TestUtils.assertContains(err.getMessage(), "exceeded configured limit of 536,870,912");
        } finally {
            // Restore global limit
            Unsafe.setMallocMemoryLimit(new DefaultCairoConfiguration("").getOutOfHeapMallocMemoryLimit());
        }
    }
}
