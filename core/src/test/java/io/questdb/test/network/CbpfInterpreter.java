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


package io.questdb.test.network;

import io.questdb.network.SteeringProgram;
import io.questdb.std.LongList;
import org.junit.Assert;

/**
 * Runs a connection steering program the way the kernel would, for the instructions the program
 * uses, after the checks the kernel's classic BPF checker applies to them.
 */
public final class CbpfInterpreter {
    private static final int MAX_INSTRUCTIONS = 4096;

    private CbpfInterpreter() {
    }

    /**
     * Runs the program on CPU {@code cpu} with {@code random} as the random number, and returns the
     * listener index, or -1 for out of range.
     */
    public static int run(LongList program, int cpu, int random) {
        validate(program);
        int a = 0;
        int pc = 0;
        for (int steps = 0; steps <= program.size(); steps++) {
            final long insn = program.getQuick(pc);
            final int code = SteeringProgram.getCode(insn);
            final int k = SteeringProgram.getK(insn);
            switch (code) {
                case SteeringProgram.CODE_LD_W_ABS -> {
                    if (k == SteeringProgram.SKF_AD_OFF + SteeringProgram.SKF_AD_CPU) {
                        a = cpu;
                    } else if (k == SteeringProgram.SKF_AD_OFF + SteeringProgram.SKF_AD_RANDOM) {
                        a = random;
                    } else {
                        Assert.fail("unexpected load at " + pc + ": k=" + k);
                    }
                    pc++;
                }
                case SteeringProgram.CODE_ALU_MOD_K -> {
                    a = Integer.remainderUnsigned(a, k);
                    pc++;
                }
                case SteeringProgram.CODE_JMP_JA -> pc += 1 + k;
                case SteeringProgram.CODE_JMP_JEQ_K ->
                        pc += 1 + (a == k ? SteeringProgram.getJt(insn) : SteeringProgram.getJf(insn));
                case SteeringProgram.CODE_JMP_JGT_K ->
                        pc += 1 + (Integer.compareUnsigned(a, k) > 0 ? SteeringProgram.getJt(insn) : SteeringProgram.getJf(insn));
                case SteeringProgram.CODE_RET_K -> {
                    return k;
                }
                default -> Assert.fail("unexpected instruction at " + pc + ": code=" + code);
            }
        }
        Assert.fail("the program does not return");
        return -1;
    }

    /**
     * Checks what the kernel's checker checks for these instructions: the length, that every jump
     * lands inside the program, that no modulo divides by zero, and that the last instruction
     * returns.
     */
    public static void validate(LongList program) {
        final int n = program.size();
        Assert.assertTrue("empty program", n > 0);
        Assert.assertTrue("too long: " + n, n <= MAX_INSTRUCTIONS);
        for (int pc = 0; pc < n; pc++) {
            final long insn = program.getQuick(pc);
            final int code = SteeringProgram.getCode(insn);
            final int k = SteeringProgram.getK(insn);
            switch (code) {
                case SteeringProgram.CODE_JMP_JA ->
                        Assert.assertTrue("jump out of the program at " + pc, Integer.toUnsignedLong(k) + pc + 1 < n);
                case SteeringProgram.CODE_JMP_JEQ_K, SteeringProgram.CODE_JMP_JGT_K -> {
                    Assert.assertTrue("jump out of the program at " + pc, pc + 1 + SteeringProgram.getJt(insn) < n);
                    Assert.assertTrue("jump out of the program at " + pc, pc + 1 + SteeringProgram.getJf(insn) < n);
                }
                case SteeringProgram.CODE_ALU_MOD_K -> Assert.assertNotEquals("modulo by zero at " + pc, 0, k);
                case SteeringProgram.CODE_LD_W_ABS, SteeringProgram.CODE_RET_K -> {
                }
                default -> Assert.fail("unexpected instruction at " + pc + ": code=" + code);
            }
        }
        Assert.assertEquals("the last instruction must return", SteeringProgram.CODE_RET_K, SteeringProgram.getCode(program.getQuick(n - 1)));
    }
}
