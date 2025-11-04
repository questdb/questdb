/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test;

import io.questdb.griffin.engine.functions.catalogue.DumpThreadStacksFunctionFactory;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Os;
import org.junit.runner.Description;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

@RunListener.ThreadSafe
public class TestListener extends RunListener {
    private static final Log LOG = LogFactory.getLog(TestListener.class);
    long testStartMs = -1;

    public static void dumpThreadStacks() {
        StringBuilder s = new StringBuilder();

        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 20);

        s.append("Thread dump: ");
        for (ThreadInfo threadInfo : threadInfos) {
            if (threadInfo == null) {
                continue;
            }
            final Thread.State state = threadInfo.getThreadState();
            s.append('\n');
            s.append('\'').append(threadInfo.getThreadName()).append("': ").append(state);
            final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (final StackTraceElement stackTraceElement : stackTraceElements) {
                s.append("\n\t\tat ").append(stackTraceElement);
            }
            s.append("\n\n");
        }
        System.out.println(s);
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
        Description description = failure.getDescription();
        LOG.error()
                .$("***** Test Assumption Violated ***** ")
                .$safe(description.getClassName()).$('.')
                .$safe(description.getMethodName())
                .$(" duration_ms=")
                .$(getTestDuration())
                .$(" : ")
                .$(failure.getException()).$();
    }

    @Override
    public void testFailure(Failure failure) {
        Description description = failure.getDescription();
        LOG.error()
                .$("***** Test Failed ***** ")
                .$safe(description.getClassName()).$('.')
                .$safe(description.getMethodName())
                .$(" duration_ms=").$(getTestDuration())
                .$(" : ")
                .$(failure.getException()).$();
    }

    @Override
    public void testFinished(Description description) {
        LOG.infoW().$("<<<< ")
                .$safe(description.getClassName()).$('.')
                .$safe(description.getMethodName())
                .$(" duration_ms=").$(getTestDuration()).$();
        System.out.println("<<<<= " + description.getClassName() + '.' + description.getMethodName() + " duration_ms=" + getTestDuration());
    }

    @Override
    public void testStarted(Description description) {
        testStartMs = System.currentTimeMillis();
        LOG.infoW().$(">>>> ")
                .$safe(description.getClassName()).$('.')
                .$safe(description.getMethodName())
                .$();
        System.out.println(">>>>= " + description.getClassName() + '.' + description.getMethodName());
    }

    private long getTestDuration() {
        return (System.currentTimeMillis() - testStartMs);
    }

    static {
        Thread monitor = new Thread(() -> {
            try {
                while (true) {
                    dumpThreadStacks();
                    Os.sleep(10 * 60 * 1000);
                }
            } catch (Throwable t) {
                System.out.println("Thread dumper failed! [kind=java]");
                t.printStackTrace(System.out);
            }
        });

        monitor.setDaemon(true);
        monitor.start();
    }

    static {
        Thread monitor = new Thread(() -> {
            try {
                while (true) {
                    DumpThreadStacksFunctionFactory.dumpThreadStacks();
                    Os.sleep(10 * 60 * 1000);
                }
            } catch (Throwable t) {
                System.out.println("Thread dumper failed! [kind=native]");
                t.printStackTrace(System.out);
            }
        });

        monitor.setDaemon(true);
        monitor.start();
    }
}
