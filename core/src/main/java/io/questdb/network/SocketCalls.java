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


package io.questdb.network;

import io.questdb.std.LongList;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

/**
 * Socket calls that go through the foreign function API rather than the bundled native libraries.
 * Each call captures {@code errno} right after the C library returns, before the JVM can overwrite
 * it, and reports it as its result. The JVM resolves each symbol on first use.
 */
final class SocketCalls {
    static final int ENOMEM = 12;
    static final int ENOPROTOOPT = 92;
    private static final int SO_ATTACH_REUSEPORT_CBPF = 51;
    private static final int SO_DETACH_REUSEPORT_BPF = 68;
    // struct sock_fprog: an unsigned short length, padding, and a pointer to the instructions
    private static final long SOCK_FPROG_FILTER_OFFSET = 8;
    private static final long SOCK_FPROG_SIZE = 16;
    private static final int SOL_SOCKET_LINUX = 1;

    private SocketCalls() {
    }

    /**
     * Attaches a classic BPF program that picks the listener of the {@code SO_REUSEPORT} group that
     * {@code osFd} belongs to, replacing the group's previous program. Linux only.
     *
     * @return 0, or the errno of the failed {@code setsockopt()}
     */
    static int attachReuseportProgram(int osFd, LongList program) {
        final int count = program.size();
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment filter = arena.allocate((long) count * Long.BYTES, Long.BYTES);
            for (int i = 0; i < count; i++) {
                filter.setAtIndex(ValueLayout.JAVA_LONG, i, program.getQuick(i));
            }
            final MemorySegment fprog = arena.allocate(SOCK_FPROG_SIZE, Long.BYTES);
            fprog.set(ValueLayout.JAVA_SHORT, 0, (short) count);
            fprog.set(ValueLayout.ADDRESS, SOCK_FPROG_FILTER_OFFSET, filter);
            return setSockOpt(arena, osFd, SO_ATTACH_REUSEPORT_CBPF, fprog, (int) SOCK_FPROG_SIZE);
        }
    }

    /**
     * Detaches the program of the {@code SO_REUSEPORT} group that {@code osFd} belongs to. Linux 5.3
     * or later.
     *
     * @return 0, or the errno of the failed {@code setsockopt()}
     */
    static int detachReuseportProgram(int osFd) {
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment unused = arena.allocate(ValueLayout.JAVA_INT);
            return setSockOpt(arena, osFd, SO_DETACH_REUSEPORT_BPF, unused, Integer.BYTES);
        }
    }

    /**
     * Calls {@code listen()}. POSIX systems only.
     *
     * @return 0, or the errno of the failed {@code listen()}
     */
    static int listen(int osFd, int backlog) {
        try (Arena arena = Arena.ofConfined()) {
            final MemorySegment state = arena.allocate(Calls.CAPTURE_STATE);
            final int rc = (int) Calls.LISTEN.invokeExact(state, osFd, backlog);
            return rc == 0 ? 0 : (int) Calls.ERRNO.get(state, 0L);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            // unreachable: everything an int(int, int) downcall can throw is a RuntimeException or Error
            throw new AssertionError(t);
        }
    }

    private static int setSockOpt(Arena arena, int osFd, int option, MemorySegment value, int length) {
        final MemorySegment state = arena.allocate(Calls.CAPTURE_STATE);
        try {
            final int rc = (int) Calls.SETSOCKOPT.invokeExact(state, osFd, SOL_SOCKET_LINUX, option, value, length);
            return rc == 0 ? 0 : (int) Calls.ERRNO.get(state, 0L);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            // unreachable: everything an int(int, int, int, pointer, int) downcall can throw is a
            // RuntimeException or Error
            throw new AssertionError(t);
        }
    }

    // Holds the downcalls, which the JVM resolves on the first socket call.
    private static final class Calls {
        private static final StructLayout CAPTURE_STATE = Linker.Option.captureStateLayout();
        private static final VarHandle ERRNO = CAPTURE_STATE.varHandle(MemoryLayout.PathElement.groupElement("errno"));
        private static final MethodHandle LISTEN = downcall(
                "listen",
                FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)
        );
        private static final MethodHandle SETSOCKOPT = downcall(
                "setsockopt",
                FunctionDescriptor.of(
                        ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_INT,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_INT
                )
        );

        private static MethodHandle downcall(String name, FunctionDescriptor descriptor) {
            final Linker linker = Linker.nativeLinker();
            return linker.downcallHandle(
                    linker.defaultLookup().find(name).orElseThrow(
                            () -> new UnsupportedOperationException("symbol " + name + " not found in the C library")),
                    descriptor,
                    Linker.Option.captureCallState("errno")
            );
        }
    }
}
