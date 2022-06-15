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

package io.questdb.test.tools;

import io.questdb.cairo.CairoEngineTest;
import io.questdb.std.ThreadLocal;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Collection;
import java.util.List;

public final class NativeLeakDetector implements TestRule {
    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                long memUsedBefore = Unsafe.getMemUsed();
                Collection<StackTraceElement[]> stacktraceBefore = Unsafe.getAllocationStacktraces();

                try {
                    base.evaluate();
                } finally {
                    Path.clearThreadLocals();
                    long memUsedAfter = Unsafe.getMemUsed();
                    if (memUsedAfter > memUsedBefore) {
                        List<StackTraceElement[]> stacktraceAfter = Unsafe.getAllocationStacktraces();
                        stacktraceAfter.removeAll(stacktraceBefore);
                        stacktraceAfter.removeIf(NativeLeakDetector::isWhitelisted);
                        if (stacktraceAfter.size() != 0) {
                            for (StackTraceElement[] frames : stacktraceAfter) {
                                printStacktrace(frames);
                            }
                            throw new AssertionError("Memory leak detected. Memory used before: "
                                    + memUsedBefore +" B, memory used after: " + memUsedAfter + " B");
                        }
                    }
                }
            }
        };
    }

    static boolean isWhitelisted(StackTraceElement[] stacktrace) {
//  0     java.base/java.lang.Thread.getStackTrace(Thread.java:1602) // skipping
//  1     io.questdb/io.questdb.std.Unsafe.recordAllocationStacktrace(Unsafe.java:232) //skipping
//  2     io.questdb/io.questdb.std.Unsafe.malloc(Unsafe.java:225)
//  3     io.questdb/io.questdb.std.str.Path.<init>(Path.java:58)
//  4     io.questdb/io.questdb.std.str.Path.<init>(Path.java:53)
//  5     io.questdb/io.questdb.std.ThreadLocal.initialValue(ThreadLocal.java:36)
//  6     java.base/java.lang.ThreadLocal.setInitialValue(ThreadLocal.java:195)
//  7     java.base/java.lang.ThreadLocal.get(ThreadLocal.java:172)
//  8     io.questdb/io.questdb.std.str.Path.getThreadLocal(Path.java:70)
        if (whitelisted(2,
                frame(Unsafe.class, "malloc"),
                frame(Path.class, "<init>"),
                frame(Path.class, "<init>"),
                frame(ThreadLocal.class, "initialValue"),
                frame(java.lang.ThreadLocal.class, "setInitialValue"),
                frame(java.lang.ThreadLocal.class, "get"),
                frame(Path.class, "getThreadLocal")
        ).isMatching(stacktrace)) {
            return true;
        }


//        java.base/java.lang.Thread.getStackTrace(Thread.java:1602)
//        io.questdb/io.questdb.std.Unsafe.recordAllocationStacktrace(Unsafe.java:232)
//        io.questdb/io.questdb.std.Unsafe.malloc(Unsafe.java:225)
//        io.questdb/io.questdb.std.str.Path.<init>(Path.java:58)
//        io.questdb/io.questdb.std.str.Path.<init>(Path.java:53)
//        io.questdb/io.questdb.cairo.CairoEngineTest.<clinit>(CairoEngineTest.java:51)
        if (whitelisted(2,
                frame(Unsafe.class, "malloc"),
                frame(Path.class, "<init>"),
                frame(Path.class, "<init>"),
                frame(CairoEngineTest.class, "<clinit>")
        ).isMatching(stacktrace)) {
            return true;
        }
        return false;
    }

    private static class WhitelistEntry {
        private final int offset;
        private final FrameDefinition[] frameDefinitions;

        private WhitelistEntry(int offset, FrameDefinition[] frameDefinitions) {
            this.offset = offset;
            this.frameDefinitions = frameDefinitions;
        }

        private boolean isMatching(StackTraceElement[] stackTraceElements) {
            if (stackTraceElements.length < offset + frameDefinitions.length) {
                return false;
            }
            for (int i = 0; i < frameDefinitions.length; i++) {
               if (!isFromClassAndMethod(stackTraceElements[i + offset], frameDefinitions[i].getClazz(), frameDefinitions[i].getMethodName())) {
                   return false;
               }
            }
            return true;
        }

        private static boolean isFromClassAndMethod(StackTraceElement element, Class<?> clazz, String methodName) {
            return clazz.getName().equals(element.getClassName()) && methodName.equals(element.getMethodName());
        }
    }

    private static FrameDefinition frame(Class<?> clazz, String methodName) {
        return new FrameDefinition(clazz, methodName);
    }

    private static WhitelistEntry whitelisted(int offset, FrameDefinition...frames) {
        return new WhitelistEntry(offset, frames);
    }

    private static class FrameDefinition {
        private final Class<?> clazz;
        private final String methodName;

        private FrameDefinition(Class<?> clazz, String methodName) {
            this.clazz = clazz;
            this.methodName = methodName;
        }

        public Class<?> getClazz() {
            return clazz;
        }

        public String getMethodName() {
            return methodName;
        }
    }

    private static void printStacktrace(StackTraceElement[] frames) {
        for (StackTraceElement frame : frames) {
            System.out.println(frame);
        }
        System.out.println();
    }
}
