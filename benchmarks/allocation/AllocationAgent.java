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

package org.questdb;

import org.objectweb.asm.*;
import java.lang.instrument.*;
import java.security.ProtectionDomain;
import java.util.Arrays;

/** Exact allocation-bytecode census and shared-bookkeeping byte attribution. */
public final class AllocationAgent {
    private static final com.sun.management.ThreadMXBean BEAN = (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
    private static final int THREADS = 32;
    private static final int SITES = 65536;
    private static final String[] threadNames = new String[THREADS];
    private static final long[] ids = new long[THREADS];
    private static final long[][] counts = new long[THREADS][SITES];
    private static final String[] sites = new String[SITES];
    private static final StackTraceElement[][] stacks = new StackTraceElement[SITES][];
    private static final int[] scopeDepth = new int[THREADS];
    private static final long[] scopeStart = new long[THREADS];
    private static final long[] sharedBytes = new long[THREADS];
    private static int siteCount;
    private static final long[] work = new long[THREADS];
    public static volatile boolean enabled;
    public static volatile boolean profile;

    public static void premain(String args, Instrumentation instrumentation) {
        boolean frameworkOnly = "framework".equals(args);
        instrumentation.addTransformer(new ClassFileTransformer() {
            @Override
            public byte[] transform(ClassLoader loader, String name, Class<?> type, ProtectionDomain domain, byte[] bytes) {
                if (name == null || !name.startsWith("io/questdb/") || name.startsWith("io/questdb/log/")) return null;
                if (frameworkOnly && !name.equals("io/questdb/mp/FanOut") && !name.equals("io/questdb/std/ConcurrentLongHashMap") && !name.equals("io/questdb/std/Unsafe")) return null;
                try {
                    ClassReader reader = new ClassReader(bytes);
                    ClassWriter writer = new ClassWriter(reader, ClassWriter.COMPUTE_MAXS);
                    reader.accept(new ClassVisitor(Opcodes.ASM9, writer) {
                        @Override
                        public MethodVisitor visitMethod(int access, String method, String descriptor, String signature, String[] exceptions) {
                            return new MethodVisitor(Opcodes.ASM9, super.visitMethod(access, method, descriptor, signature, exceptions)) {
                                int line;
                                final boolean sharedScope = (name.equals("io/questdb/mp/FanOut") && (method.equals("and") || method.equals("remove")))
                                        || (name.equals("io/questdb/std/ConcurrentLongHashMap") && method.equals("putVal"))
                                        || (name.equals("io/questdb/std/Unsafe") && method.equals("recordMemAlloc"));
                                @Override public void visitCode() {
                                    super.visitCode();
                                    if (sharedScope) super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/questdb/AllocationAgent", "enterShared", "()V", false);
                                    if (name.equals("io/questdb/griffin/engine/table/AsyncHashJoinGroupByRecordCursorFactory") && method.equals("aggregate")) {
                                        super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/questdb/AllocationAgent", "work", "()V", false);
                                    }
                                }
                                @Override public void visitInsn(int opcode) {
                                    if (sharedScope && opcode >= Opcodes.IRETURN && opcode <= Opcodes.RETURN) {
                                        super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/questdb/AllocationAgent", "exitShared", "()V", false);
                                    }
                                    super.visitInsn(opcode);
                                }
                                @Override public void visitLineNumber(int number, Label start) { line = number; super.visitLineNumber(number, start); }
                                void allocation(String operation) {
                                    int id = addSite(name + "." + method + ":" + line + " " + operation);
                                    super.visitLdcInsn(id);
                                    super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/questdb/AllocationAgent", "hit", "(I)V", false);
                                }
                                @Override public void visitTypeInsn(int opcode, String type) {
                                    super.visitTypeInsn(opcode, type);
                                    if (opcode == Opcodes.NEW || opcode == Opcodes.ANEWARRAY) {
                                        if (!frameworkOnly || type.equals("io/questdb/mp/FanOut$Holder") || type.equals("io/questdb/std/ConcurrentLongHashMap$Node")) allocation(type);
                                    }
                                }
                                @Override public void visitIntInsn(int opcode, int operand) {
                                    if (!frameworkOnly && opcode == Opcodes.NEWARRAY) allocation("primitive-array");
                                    super.visitIntInsn(opcode, operand);
                                }
                                @Override public void visitMultiANewArrayInsn(String descriptor, int dimensions) {
                                    if (!frameworkOnly) allocation(descriptor); super.visitMultiANewArrayInsn(descriptor, dimensions);
                                }
                                @Override public void visitInvokeDynamicInsn(String n, String d, Handle bootstrap, Object... arguments) {
                                    if (!frameworkOnly && (bootstrap.getOwner().equals("java/lang/invoke/StringConcatFactory")
                                            || (bootstrap.getOwner().equals("java/lang/invoke/LambdaMetafactory") && !d.startsWith("()")))) {
                                        allocation("invokedynamic:" + bootstrap.getOwner());
                                    }
                                    super.visitInvokeDynamicInsn(n, d, bootstrap, arguments);
                                }
                            };
                        }
                    }, 0);
                    return writer.toByteArray();
                } catch (Throwable failure) {
                    failure.printStackTrace();
                    Runtime.getRuntime().halt(2);
                    return null;
                }
            }
        });
    }

    private static synchronized int addSite(String site) {
        if (siteCount == SITES) throw new AssertionError("allocation site capacity");
        sites[siteCount] = site;
        return siteCount++;
    }

    public static void hit(int site) {
        if (!enabled) return;
        long thread = Thread.currentThread().threadId();
        int slot = 0;
        while (slot < THREADS && ids[slot] != thread) slot++;
        if (slot == THREADS) slot = register(thread);
        counts[slot][site]++;
        if (profile && stacks[site] == null) {
            synchronized (stacks) {
                if (stacks[site] == null) stacks[site] = Thread.currentThread().getStackTrace();
            }
        }
    }

    public static void work() {
        if (!enabled) return;
        long thread = Thread.currentThread().threadId();
        int slot = 0;
        while (slot < THREADS && ids[slot] != thread) slot++;
        if (slot == THREADS) slot = register(thread);
        work[slot]++;
    }

    private static synchronized int register(long thread) {
        for (int i = 0; i < THREADS; i++) {
            if (ids[i] == thread) return i;
            if (ids[i] == 0) { threadNames[i] = Thread.currentThread().getName(); ids[i] = thread; return i; }
        }
        throw new AssertionError("allocation thread capacity");
    }

    public static void reset() {
        for (long[] row : counts) Arrays.fill(row, 0);
        Arrays.fill(stacks, null);
        Arrays.fill(work, 0);
        Arrays.fill(sharedBytes, 0);
        Arrays.fill(scopeDepth, 0);
    }

    public static void report(String phase) {
        for (int t = 0; t < THREADS; t++) {
            if (work[t] != 0) System.out.println("WORK," + phase + "," + ids[t] + "," + threadNames[t] + "," + work[t]);
            for (int s = 0; s < siteCount; s++) {
                if (counts[t][s] != 0) {
                    if (phase.startsWith("execution") && !sharedSite(s)) throw new AssertionError("unexplained allocation site: " + sites[s] + " " + Arrays.toString(stacks[s]));
                    System.out.println("SITE," + phase + "," + ids[t] + "," + counts[t][s] + "," + sites[s]);
                    if (stacks[s] != null) for (StackTraceElement frame : stacks[s]) System.out.println("STACK," + sites[s] + "," + frame);
                }
            }
        }
    }
    private static int slot() {
        long thread = Thread.currentThread().threadId();
        int slot = 0;
        while (slot < THREADS && ids[slot] != thread) slot++;
        return slot == THREADS ? register(thread) : slot;
    }

    public static void enterShared() {
        if (!enabled) return;
        int slot = slot();
        if (scopeDepth[slot]++ == 0) scopeStart[slot] = BEAN.getCurrentThreadAllocatedBytes();
    }

    public static void exitShared() {
        if (!enabled) return;
        int slot = slot();
        if (--scopeDepth[slot] == 0) sharedBytes[slot] += BEAN.getCurrentThreadAllocatedBytes() - scopeStart[slot];
    }

    public static long frameworkBytes(long thread) {
        for (int t = 0; t < THREADS; t++) if (ids[t] == thread) return sharedBytes[t];
        return 0;
    }

    private static boolean sharedSite(int site) {
        String name = sites[site];
        if (name.startsWith("io/questdb/mp/FanOut.and:") || name.startsWith("io/questdb/mp/FanOut.remove:")
                || name.startsWith("io/questdb/mp/FanOut$Holder.<init>:")
                || name.startsWith("io/questdb/std/ConcurrentLongHashMap.putVal:")) return true;
        if (name.startsWith("io/questdb/std/ObjList.<init>:") && stacks[site] != null) {
            for (StackTraceElement frame : stacks[site]) if (frame.getClassName().equals("io.questdb.mp.FanOut$Holder")) return true;
        }
        return false;
    }

}
