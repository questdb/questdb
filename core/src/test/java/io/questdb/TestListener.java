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

package io.questdb;

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
    private final static Log LOG = LogFactory.getLog(TestListener.class);

    public static void dumpThreadStacks() {
        StringBuilder s = new StringBuilder();

        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 20);

        s.append("Thread dump: ");
        for (ThreadInfo threadInfo : threadInfos) {
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

    static {
        Thread monitor = new Thread(() -> {
            try {
                while (true) {
                    dumpThreadStacks();
                    Os.sleep(10 * 60 * 1000);
                }
            } catch (Throwable t) {
                System.out.println("Thread dumper failed!");
                t.printStackTrace();
            }
        });

        monitor.setDaemon(true);
        monitor.start();
    }

    @Override
    public void testStarted(Description description) {
        LOG.infoW().$(">>>> ").$(description.getClassName()).$('.').$(description.getMethodName()).$();
    }

    @Override
    public void testFinished(Description description) {
        LOG.infoW().$("<<<< ").$(description.getClassName()).$('.').$(description.getMethodName()).$();
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
        testAssumptionFailure(failure);
    }


    @Override
    public void testAssumptionFailure(Failure failure) {
        Description description = failure.getDescription();
        LOG.error()
                .$("***** Test Failed *****")
                .$(description.getClassName()).$('.')
                .$(description.getMethodName())
                .$(" : ")
                .$(failure.getException()).$();
    }
}
