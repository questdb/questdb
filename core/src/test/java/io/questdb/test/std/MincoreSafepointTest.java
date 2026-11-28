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

package io.questdb.test.std;

import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.test.tools.TestUtils.assertMemoryLeak;

/**
 * Linux takes the mmap lock of the process for every mincore(2) call, so the call waits
 * while other threads map, remap or unmap memory. A thread that waits in a downcall must be
 * in native code for the JVM, which then reaches a safepoint without it; a critical downcall
 * keeps the thread in Java code, and every safepoint waits until the call returns.
 * <p>
 * The test holds a call in the kernel for as long as it needs: a seccomp(2) filter on the
 * calling thread hands its mincore(2) calls to the test, see seccomp_unotify(2), and the
 * kernel runs a call once the test answers it.
 */
public class MincoreSafepointTest {

    @Test
    public void testSafepointDoesNotWaitForMincore() throws Exception {
        // only Linux asks mincore(2), see Files.isPageCacheResident()
        Assume.assumeTrue(Files.isMadviseWillNeedSupported());
        assertMemoryLeak(() -> {
            final long size = Files.PAGE_SIZE;
            final long address = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.setMemory(address, size, (byte) 1);
                // binds mincore(2) before a filter traps it
                Assert.assertTrue(Files.isPageCacheResident(address, size));
                // reading the state of a thread takes no safepoint; this thread runs Java code
                final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
                Assert.assertFalse(threads.getThreadInfo(Thread.currentThread().threadId()).isInNative());

                final AtomicInteger listenerFd = new AtomicInteger(Integer.MIN_VALUE);
                final AtomicBoolean isResident = new AtomicBoolean();
                final AtomicReference<Throwable> error = new AtomicReference<>();
                final CountDownLatch isTrapSet = new CountDownLatch(1);
                final CountDownLatch isCallAllowed = new CountDownLatch(1);
                final Thread caller = new Thread(() -> {
                    try {
                        final int fd = MincoreTrap.trapCurrentThread();
                        listenerFd.set(fd);
                        isTrapSet.countDown();
                        if (fd >= 0) {
                            TestUtils.await(isCallAllowed);
                            isResident.set(Files.isPageCacheResident(address, size));
                        }
                    } catch (Throwable th) {
                        error.set(th);
                    } finally {
                        isTrapSet.countDown();
                    }
                }, "mincore-caller");
                caller.start();
                TestUtils.await(isTrapSet);
                final int fd = listenerFd.get();
                if (fd < 0) {
                    caller.join();
                    if (error.get() != null) {
                        throw new AssertionError(error.get());
                    }
                }
                Assume.assumeTrue("the kernel refuses a seccomp(2) listener, errno=" + -fd, fd >= 0);

                boolean isInNative = false;
                StackTraceElement[] stack = null;
                try (Arena arena = Arena.ofConfined()) {
                    // With a critical binding, a safepoint that starts while the call waits would
                    // wait for the call, and so would this thread, which answers it. The test
                    // makes each call it makes while the call waits once before, so that it then
                    // loads and links nothing.
                    final int rc = MincoreTrap.prepare(arena, fd);
                    Assume.assumeTrue("the kernel cannot run a trapped call, errno=" + -rc, rc != -MincoreTrap.EINVAL);
                    Assert.assertEquals(-MincoreTrap.ENOENT, rc);
                    // the caller waits for the go-ahead outside native code, and reads so
                    Assert.assertFalse(threads.getThreadInfo(caller.threadId()).isInNative());
                    isCallAllowed.countDown();

                    // the timeout bounds only a failure: the caller calls mincore(2) right away
                    Assert.assertTrue("mincore(2) was never called", MincoreTrap.awaitCall(arena, fd, 60_000));
                    final MemorySegment call = MincoreTrap.receive(arena, fd);
                    // the caller waits inside mincore(2) until the test answers
                    isInNative = threads.getThreadInfo(caller.threadId()).isInNative();
                    if (isInNative) {
                        // the VM walks the stack of a thread in native code without waiting for it
                        stack = caller.getStackTrace();
                    }
                    MincoreTrap.resume(arena, fd, call);
                    Assert.assertEquals(MincoreTrap.NR_MINCORE, call.get(ValueLayout.JAVA_INT, MincoreTrap.NOTIF_NR_OFFSET));
                    Assert.assertEquals(address & -Files.PAGE_SIZE, call.get(ValueLayout.JAVA_LONG, MincoreTrap.NOTIF_ARG0_OFFSET));
                } finally {
                    isCallAllowed.countDown();
                    // a call the test did not answer fails with ENOSYS, and so does any later one
                    MincoreTrap.close(fd);
                    caller.join();
                }
                if (error.get() != null) {
                    throw new AssertionError(error.get());
                }
                Assert.assertTrue("the caller ran Java code while it waited in mincore(2): every safepoint waits for such a call", isInNative);
                Assert.assertTrue(
                        "the caller was not inside Files.isPageCacheResident(): " + Arrays.toString(stack),
                        hasFrame(stack, Files.class.getName(), "isPageCacheResident")
                );
                // the kernel ran the call the test let through
                Assert.assertTrue(isResident.get());
            } finally {
                Unsafe.free(address, size, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static boolean hasFrame(StackTraceElement[] stack, String className, String methodName) {
        for (StackTraceElement e : stack) {
            if (className.equals(e.getClassName()) && methodName.equals(e.getMethodName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Makes the kernel hand the mincore(2) calls of a thread to a listener, and hold each call
     * until the listener answers it. Binds the C library on first use, so only Linux loads it.
     */
    private static final class MincoreTrap {
        static final int EINVAL = 22;
        static final int ENOENT = 2;
        // struct seccomp_notif: id, pid, flags, then struct seccomp_data: nr, arch, instruction_pointer, args[6]
        static final long NOTIF_ARG0_OFFSET = 32;
        static final long NOTIF_NR_OFFSET = 16;
        static final int NR_MINCORE = Os.arch == Os.ARCH_AARCH64 ? 232 : 27;
        private static final int AUDIT_ARCH = Os.arch == Os.ARCH_AARCH64 ? 0xC00000B7 : 0xC000003E;
        private static final int BPF_JEQ_K = 0x15;
        private static final int BPF_LD_W_ABS = 0x20;
        private static final int BPF_RET_K = 0x06;
        private static final StructLayout CALL_STATE = Linker.Option.captureStateLayout();
        private static final MethodHandle CLOSE;
        private static final int EINTR = 4;
        private static final VarHandle ERRNO = CALL_STATE.varHandle(MemoryLayout.PathElement.groupElement("errno"));
        private static final long FILTER_FLAG_NEW_LISTENER = 8;
        private static final MethodHandle IOCTL;
        // _IOWR('!', 0, struct seccomp_notif) and _IOWR('!', 1, struct seccomp_notif_resp)
        private static final long IOCTL_NOTIF_RECV = 0xC0502100L;
        private static final long IOCTL_NOTIF_SEND = 0xC0182101L;
        private static final long NOTIF_SIZE = 80;
        private static final long NR_SECCOMP = Os.arch == Os.ARCH_AARCH64 ? 277 : 317;
        private static final MethodHandle POLL;
        private static final short POLLIN = 1;
        private static final int PR_SET_NO_NEW_PRIVS = 38;
        private static final MethodHandle PRCTL;
        // struct seccomp_notif_resp: id, val, error, flags
        private static final long RESP_FLAGS_OFFSET = 20;
        private static final long RESP_SIZE = 24;
        private static final int RET_ALLOW = 0x7FFF0000;
        private static final int RET_USER_NOTIF = 0x7FC00000;
        private static final long SET_MODE_FILTER = 1;
        private static final MethodHandle SYSCALL;
        private static final int USER_NOTIF_FLAG_CONTINUE = 1;

        // Whether a call waits for an answer within the timeout.
        static boolean awaitCall(Arena arena, int fd, int timeoutMillis) {
            final MemorySegment state = arena.allocate(CALL_STATE);
            // struct pollfd: fd, events, revents
            final MemorySegment pollFd = arena.allocate(8, 4);
            pollFd.set(ValueLayout.JAVA_INT, 0, fd);
            pollFd.set(ValueLayout.JAVA_SHORT, 4, POLLIN);
            int n;
            do {
                n = poll(state, pollFd, timeoutMillis);
            } while (n < 0 && errno(state) == EINTR);
            if (n < 0) {
                Assert.fail("poll(2) failed, errno=" + errno(state));
            }
            return n == 1 && (pollFd.get(ValueLayout.JAVA_SHORT, 6) & POLLIN) != 0;
        }

        static void close(int fd) {
            final int rc;
            try {
                rc = (int) CLOSE.invokeExact(fd);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            Assert.assertEquals(0, rc);
        }

        // Makes each call to the listener that the test makes while a mincore(2) call waits:
        // waits for no time, and answers a call that no thread made, as resume() answers one.
        // Returns -errno of the answer: -ENOENT, or -EINVAL when the kernel cannot run a call
        // it trapped.
        static int prepare(Arena arena, int fd) {
            Assert.assertFalse(awaitCall(arena, fd, 0));
            return send(arena, fd, arena.allocate(NOTIF_SIZE, 8));
        }

        // Receives a call that waits; the call goes on waiting until resume() answers it.
        static MemorySegment receive(Arena arena, int fd) {
            // the kernel wants the struct zeroed, as the arena allocates it
            final MemorySegment call = arena.allocate(NOTIF_SIZE, 8);
            final int rc = ioctl(arena, fd, IOCTL_NOTIF_RECV, call);
            if (rc != 0) {
                Assert.fail("SECCOMP_IOCTL_NOTIF_RECV failed, errno=" + -rc);
            }
            return call;
        }

        // Lets the kernel run the call.
        static void resume(Arena arena, int fd, MemorySegment call) {
            final int rc = send(arena, fd, call);
            if (rc != 0) {
                Assert.fail("SECCOMP_IOCTL_NOTIF_SEND failed, errno=" + -rc);
            }
        }

        // Traps the mincore(2) calls of the current thread, and of no other. Returns the fd of
        // the listener, or -errno when the kernel refuses it.
        static int trapCurrentThread() {
            try (Arena arena = Arena.ofConfined()) {
                final MemorySegment state = arena.allocate(CALL_STATE);
                // a thread without privileges installs a filter only once it cannot gain any
                final int rc = prctl(state, PR_SET_NO_NEW_PRIVS, 1);
                Assert.assertEquals("prctl(PR_SET_NO_NEW_PRIVS) failed, errno=" + errno(state), 0, rc);
                // the program reads struct seccomp_data: nr at offset 0, arch at offset 4
                final MemorySegment filter = arena.allocate(6 * 8, 8);
                setInstruction(filter, 0, BPF_LD_W_ABS, 0, 0, 4);
                // a call through another ABI goes through
                setInstruction(filter, 1, BPF_JEQ_K, 0, 3, AUDIT_ARCH);
                setInstruction(filter, 2, BPF_LD_W_ABS, 0, 0, 0);
                setInstruction(filter, 3, BPF_JEQ_K, 0, 1, NR_MINCORE);
                setInstruction(filter, 4, BPF_RET_K, 0, 0, RET_USER_NOTIF);
                setInstruction(filter, 5, BPF_RET_K, 0, 0, RET_ALLOW);
                // struct sock_fprog: the count of instructions, then the address of the first
                final MemorySegment program = arena.allocate(16, 8);
                program.set(ValueLayout.JAVA_SHORT, 0, (short) 6);
                program.set(ValueLayout.ADDRESS, 8, filter);
                final long fd = seccomp(state, SET_MODE_FILTER, FILTER_FLAG_NEW_LISTENER, program);
                return fd >= 0 ? (int) fd : -errno(state);
            }
        }

        private static MethodHandle bind(String name, FunctionDescriptor descriptor, Linker.Option... options) {
            final Linker linker = Linker.nativeLinker();
            return linker.downcallHandle(linker.defaultLookup().find(name).orElseThrow(), descriptor, options);
        }

        private static int errno(MemorySegment state) {
            return (int) ERRNO.get(state, 0L);
        }

        // the one call site of ioctl(2); returns 0, or -errno
        private static int ioctl(Arena arena, int fd, long request, MemorySegment arg) {
            final MemorySegment state = arena.allocate(CALL_STATE);
            final int rc;
            try {
                rc = (int) IOCTL.invokeExact(state, fd, request, arg);
            } catch (Throwable t) {
                throw rethrow(t);
            }
            return rc == 0 ? 0 : -errno(state);
        }

        private static int poll(MemorySegment state, MemorySegment pollFd, int timeoutMillis) {
            try {
                return (int) POLL.invokeExact(state, pollFd, 1L, timeoutMillis);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }

        private static int prctl(MemorySegment state, int option, long arg) {
            try {
                return (int) PRCTL.invokeExact(state, option, arg, 0L, 0L, 0L);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }

        private static RuntimeException rethrow(Throwable t) {
            if (t instanceof RuntimeException e) {
                throw e;
            }
            if (t instanceof Error e) {
                throw e;
            }
            // unreachable: a downcall throws no checked exception
            throw new AssertionError(t);
        }

        private static long seccomp(MemorySegment state, long operation, long flags, MemorySegment args) {
            try {
                return (long) SYSCALL.invokeExact(state, NR_SECCOMP, operation, flags, args);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }

        // Answers the call, a struct seccomp_notif: the kernel runs it. Returns 0, or -errno.
        private static int send(Arena arena, int fd, MemorySegment call) {
            final MemorySegment resp = arena.allocate(RESP_SIZE, 8);
            resp.set(ValueLayout.JAVA_LONG, 0, call.get(ValueLayout.JAVA_LONG, 0));
            resp.set(ValueLayout.JAVA_INT, RESP_FLAGS_OFFSET, USER_NOTIF_FLAG_CONTINUE);
            return ioctl(arena, fd, IOCTL_NOTIF_SEND, resp);
        }

        // struct sock_filter: code, jt, jf, k
        private static void setInstruction(MemorySegment filter, int index, int code, int jt, int jf, int k) {
            final long offset = index * 8L;
            filter.set(ValueLayout.JAVA_SHORT, offset, (short) code);
            filter.set(ValueLayout.JAVA_BYTE, offset + 2, (byte) jt);
            filter.set(ValueLayout.JAVA_BYTE, offset + 3, (byte) jf);
            filter.set(ValueLayout.JAVA_INT, offset + 4, k);
        }

        static {
            // no binding is critical: poll(2) waits, and the test must not hold safepoints off itself
            final Linker.Option errno = Linker.Option.captureCallState("errno");
            CLOSE = bind("close", FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT));
            // int ioctl(int fd, unsigned long request, ...)
            IOCTL = bind(
                    "ioctl",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS),
                    Linker.Option.firstVariadicArg(2),
                    errno
            );
            // int poll(struct pollfd *fds, nfds_t nfds, int timeout)
            POLL = bind(
                    "poll",
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT),
                    errno
            );
            // int prctl(int option, ...)
            PRCTL = bind(
                    "prctl",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_INT,
                            ValueLayout.JAVA_INT,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.JAVA_LONG
                    ),
                    Linker.Option.firstVariadicArg(1),
                    errno
            );
            // long syscall(long number, ...), for seccomp(2), which the C library does not wrap
            SYSCALL = bind(
                    "syscall",
                    FunctionDescriptor.of(
                            ValueLayout.JAVA_LONG,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.JAVA_LONG,
                            ValueLayout.ADDRESS
                    ),
                    Linker.Option.firstVariadicArg(1),
                    errno
            );
        }
    }
}
