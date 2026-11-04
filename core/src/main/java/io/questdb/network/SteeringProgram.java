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

/**
 * Builds the classic BPF program that picks a listener of a port's {@code SO_REUSEPORT} group for
 * each new connection. The kernel runs the program on the CPU that processes the connection's SYN,
 * and the program returns the index of the listener that CPU's SYNs should reach. An index at or
 * past the group's listener count makes the kernel fall back to its hash.
 * <p>
 * The input gives each CPU number a target: a listener index, {@link #TARGET_OUT_OF_RANGE},
 * {@link #TARGET_REDIRECT} for a random pick among the redirect indices, or {@link #TARGET_OFFLINE}
 * for a CPU that processes no SYN. The program has one of two forms:
 * <ul>
 *     <li>a chain over the CPU ranges that share a target: for each range, a jump past the next
 *     instruction when the CPU is above the range's upper bound, then a return of the range's index.
 *     Offline CPUs count as out of range, and the chain ends with a return of out of range;</li>
 *     <li>when the targets repeat with the CPU number modulo the period, as on machines that number
 *     CPUs alternately across sockets, the CPU number modulo the period and a return per residue,
 *     with offline CPUs fitting any residue.</li>
 * </ul>
 * The builder emits the shorter form. A redirected range jumps to a shared block after either form,
 * which loads a random number, takes it modulo the redirect count and returns the matching index.
 * <p>
 * Each instruction is a {@code struct sock_filter} packed into a long as it lies in little-endian
 * memory: the code in bits 0 to 15, {@code jt} in 16 to 23, {@code jf} in 24 to 31 and {@code k} in
 * 32 to 63.
 */
public final class SteeringProgram {
    public static final int CODE_ALU_MOD_K = 0x94;
    public static final int CODE_JMP_JA = 0x05;
    public static final int CODE_JMP_JEQ_K = 0x15;
    public static final int CODE_JMP_JGT_K = 0x25;
    public static final int CODE_LD_W_ABS = 0x20;
    public static final int CODE_RET_K = 0x06;
    // the value a program returns for a CPU whose SYNs the kernel's hash places
    public static final int OUT_OF_RANGE_INDEX = -1;
    public static final int SKF_AD_CPU = 36;
    public static final int SKF_AD_OFF = -0x1000;
    public static final int SKF_AD_RANDOM = 56;
    public static final int TARGET_OFFLINE = -3;
    public static final int TARGET_OUT_OF_RANGE = -1;
    public static final int TARGET_REDIRECT = -2;

    private SteeringProgram() {
    }

    /**
     * Builds the program into {@code program}, which it clears first.
     *
     * @param targets        the target of each CPU number, from 0 to {@code targets.length - 1}
     * @param period         the period of the modulo form, at least 2 for it to apply
     * @param redirects      the listener indices of {@link #TARGET_REDIRECT}, of which there are at
     *                       least two when any CPU's target is a redirect
     * @param redirectCount  how many of {@code redirects} apply
     */
    public static void build(int[] targets, int period, int[] redirects, int redirectCount, LongList program) {
        program.clear();
        boolean hasRedirect = false;
        for (int target : targets) {
            if (target == TARGET_REDIRECT) {
                hasRedirect = true;
                break;
            }
        }
        if (hasRedirect && redirectCount < 2) {
            throw new IllegalArgumentException("a redirect needs at least two listeners [redirectCount=" + redirectCount + ']');
        }

        final int rangeCount = countRanges(targets);
        final int chainLength = rangeCount == 0 ? 1 : 2 * rangeCount + 2;
        final int[] residueTargets = period >= 2 ? residueTargets(targets, period) : null;
        final int moduloLength = 2 * period + 1;
        if (residueTargets != null && moduloLength < chainLength) {
            emitModulo(residueTargets, moduloLength, program);
        } else {
            emitChain(targets, rangeCount, chainLength, program);
        }
        if (hasRedirect) {
            emitRedirect(redirects, redirectCount, program);
        }
    }

    /**
     * An upper bound of the size, in bytes, of the kernel's copy of a program of
     * {@code instructionCount} classic instructions: the kernel converts each one into at most two
     * extended instructions of 8 bytes, adds a short prologue, and charges the program's header.
     * The kernel refuses a program larger than {@code net.core.optmem_max}.
     */
    public static long estimateKernelBytes(int instructionCount) {
        return 256 + 16L * (instructionCount + 8);
    }

    public static int getCode(long instruction) {
        return (int) (instruction & 0xFFFF);
    }

    public static int getJf(long instruction) {
        return (int) ((instruction >>> 24) & 0xFF);
    }

    public static int getJt(long instruction) {
        return (int) ((instruction >>> 16) & 0xFF);
    }

    public static int getK(long instruction) {
        return (int) (instruction >>> 32);
    }

    public static long instruction(int code, int jt, int jf, int k) {
        return (code & 0xFFFFL) | (jt & 0xFFL) << 16 | (jf & 0xFFL) << 24 | (k & 0xFFFF_FFFFL) << 32;
    }

    private static int chainTarget(int target) {
        return target == TARGET_OFFLINE ? TARGET_OUT_OF_RANGE : target;
    }

    // ranges of equal target in CPU order, offline CPUs counting as out of range, without the
    // out-of-range ranges at the end, which the chain's last return covers
    private static int countRanges(int[] targets) {
        int count = 0;
        int last = -1;
        for (int cpu = 0; cpu < targets.length; cpu++) {
            if (chainTarget(targets[cpu]) != TARGET_OUT_OF_RANGE) {
                last = cpu;
            }
        }
        for (int cpu = 0; cpu <= last; cpu++) {
            if (cpu == 0 || chainTarget(targets[cpu]) != chainTarget(targets[cpu - 1])) {
                count++;
            }
        }
        return count;
    }

    private static void emitChain(int[] targets, int rangeCount, int chainLength, LongList program) {
        if (rangeCount == 0) {
            program.add(instruction(CODE_RET_K, 0, 0, OUT_OF_RANGE_INDEX));
            return;
        }
        program.add(instruction(CODE_LD_W_ABS, 0, 0, SKF_AD_OFF + SKF_AD_CPU));
        int emitted = 0;
        int cpu = 0;
        while (emitted < rangeCount) {
            final int target = chainTarget(targets[cpu]);
            int hi = cpu;
            while (hi + 1 < targets.length && chainTarget(targets[hi + 1]) == target) {
                hi++;
            }
            program.add(instruction(CODE_JMP_JGT_K, 1, 0, hi));
            emitReturn(target, chainLength, program);
            emitted++;
            cpu = hi + 1;
        }
        program.add(instruction(CODE_RET_K, 0, 0, OUT_OF_RANGE_INDEX));
    }

    private static void emitModulo(int[] residueTargets, int moduloLength, LongList program) {
        final int period = residueTargets.length;
        program.add(instruction(CODE_LD_W_ABS, 0, 0, SKF_AD_OFF + SKF_AD_CPU));
        program.add(instruction(CODE_ALU_MOD_K, 0, 0, period));
        for (int r = 0; r < period - 1; r++) {
            program.add(instruction(CODE_JMP_JEQ_K, 0, 1, r));
            emitReturn(residueTargets[r], moduloLength, program);
        }
        emitReturn(residueTargets[period - 1], moduloLength, program);
    }

    private static void emitRedirect(int[] redirects, int redirectCount, LongList program) {
        program.add(instruction(CODE_LD_W_ABS, 0, 0, SKF_AD_OFF + SKF_AD_RANDOM));
        program.add(instruction(CODE_ALU_MOD_K, 0, 0, redirectCount));
        for (int i = 0; i < redirectCount - 1; i++) {
            program.add(instruction(CODE_JMP_JEQ_K, 0, 1, i));
            program.add(instruction(CODE_RET_K, 0, 0, redirects[i]));
        }
        program.add(instruction(CODE_RET_K, 0, 0, redirects[redirectCount - 1]));
    }

    // a return of the target's index, or a jump to the redirect block that starts at redirectStart
    private static void emitReturn(int target, int redirectStart, LongList program) {
        if (target == TARGET_REDIRECT) {
            program.add(instruction(CODE_JMP_JA, 0, 0, redirectStart - program.size() - 1));
        } else {
            program.add(instruction(CODE_RET_K, 0, 0, target < 0 ? OUT_OF_RANGE_INDEX : target));
        }
    }

    // the target of each residue of the CPU number modulo the period, or null when the targets
    // don't follow the residues; offline CPUs fit any residue, and a residue with only offline CPUs
    // maps out of range
    private static int[] residueTargets(int[] targets, int period) {
        final int[] residues = new int[period];
        final boolean[] seen = new boolean[period];
        for (int cpu = 0; cpu < targets.length; cpu++) {
            final int target = targets[cpu];
            if (target == TARGET_OFFLINE) {
                continue;
            }
            final int r = cpu % period;
            if (!seen[r]) {
                residues[r] = target;
                seen[r] = true;
            } else if (residues[r] != target) {
                return null;
            }
        }
        for (int r = 0; r < period; r++) {
            if (!seen[r]) {
                residues[r] = TARGET_OUT_OF_RANGE;
            }
        }
        return residues;
    }
}
