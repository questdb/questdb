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
import org.junit.Test;

import static io.questdb.network.SteeringProgram.*;

public class SteeringProgramTest {
    private static final int OUT = TARGET_OUT_OF_RANGE;

    @Test
    public void testAllOutOfRangeIsASingleReturn() {
        final LongList program = new LongList();
        SteeringProgram.build(new int[]{OUT, OUT, TARGET_OFFLINE, OUT}, 2, new int[0], 0, program);
        Assert.assertEquals(1, program.size());
        Assert.assertEquals(-1, CbpfInterpreter.run(program, 0, 0));
        Assert.assertEquals(-1, CbpfInterpreter.run(program, 1000, 0));
    }

    @Test
    public void testAlternatingNumberingUsesModulo() {
        // 192 CPUs numbered alternately across two sockets: the chain would need 192 ranges
        final int[] targets = new int[192];
        for (int cpu = 0; cpu < targets.length; cpu++) {
            targets[cpu] = cpu % 2 == 0 ? 1 : 0;
        }
        // offline CPUs fit any residue
        targets[7] = TARGET_OFFLINE;
        final LongList program = new LongList();
        SteeringProgram.build(targets, 2, new int[0], 0, program);
        Assert.assertEquals(5, program.size());
        Assert.assertEquals(CODE_ALU_MOD_K, getCode(program.getQuick(1)));
        for (int cpu = 0; cpu < targets.length; cpu++) {
            Assert.assertEquals("cpu " + cpu, cpu % 2 == 0 ? 1 : 0, CbpfInterpreter.run(program, cpu, 0));
        }
    }

    @Test
    public void testAlternatingNumberingWithRedirect() {
        // three groups numbered in rotation; group 2 redirects to listeners 0 and 1
        final int[] targets = new int[12];
        for (int cpu = 0; cpu < targets.length; cpu++) {
            final int group = cpu % 3;
            targets[cpu] = group == 2 ? TARGET_REDIRECT : group;
        }
        final LongList program = new LongList();
        SteeringProgram.build(targets, 3, new int[]{0, 1}, 2, program);
        Assert.assertEquals(CODE_ALU_MOD_K, getCode(program.getQuick(1)));
        for (int cpu = 0; cpu < targets.length; cpu++) {
            if (cpu % 3 == 2) {
                Assert.assertEquals(0, CbpfInterpreter.run(program, cpu, 0));
                Assert.assertEquals(1, CbpfInterpreter.run(program, cpu, 1));
                Assert.assertEquals(0, CbpfInterpreter.run(program, cpu, 2));
                Assert.assertEquals(1, CbpfInterpreter.run(program, cpu, -1));
            } else {
                Assert.assertEquals(cpu % 3, CbpfInterpreter.run(program, cpu, 7));
            }
        }
    }

    @Test
    public void testChainMergesRangesAndCoversGaps() {
        // CPUs 0-3 -> 0, 4-5 out of range, 6-7 -> 0, 8-11 -> 1, 12-13 offline, 14 -> 1
        final int[] targets = {0, 0, 0, 0, OUT, OUT, 0, 0, 1, 1, 1, 1, TARGET_OFFLINE, TARGET_OFFLINE, 1};
        final LongList program = new LongList();
        SteeringProgram.build(targets, 2, new int[0], 0, program);
        // load, 6 ranges of 2 instructions, final return
        Assert.assertEquals(1 + 2 * 6 + 1, program.size());
        for (int cpu = 0; cpu < targets.length; cpu++) {
            final int expected = targets[cpu] == TARGET_OFFLINE ? -1 : targets[cpu];
            Assert.assertEquals("cpu " + cpu, expected, CbpfInterpreter.run(program, cpu, 0));
        }
        // CPUs past the table, such as one that came online after the build, fall back to the hash
        Assert.assertEquals(-1, CbpfInterpreter.run(program, 15, 0));
        Assert.assertEquals(-1, CbpfInterpreter.run(program, 4000, 0));
    }

    @Test
    public void testChainOmitsTrailingOutOfRange() {
        final int[] targets = {1, 1, 0, 0, OUT, OUT, TARGET_OFFLINE};
        final LongList program = new LongList();
        SteeringProgram.build(targets, 2, new int[0], 0, program);
        Assert.assertEquals(1 + 2 * 2 + 1, program.size());
        for (int cpu = 0; cpu < 10; cpu++) {
            final int expected = cpu < 2 ? 1 : cpu < 4 ? 0 : -1;
            Assert.assertEquals("cpu " + cpu, expected, CbpfInterpreter.run(program, cpu, 0));
        }
    }

    @Test
    public void testContiguousSocketsUseChain() {
        // two sockets of 96 CPUs each
        final int[] targets = new int[192];
        for (int cpu = 0; cpu < targets.length; cpu++) {
            targets[cpu] = cpu < 96 ? 1 : 0;
        }
        final LongList program = new LongList();
        SteeringProgram.build(targets, 2, new int[0], 0, program);
        Assert.assertEquals(CODE_JMP_JGT_K, getCode(program.getQuick(1)));
        Assert.assertEquals(6, program.size());
        for (int cpu = 0; cpu < targets.length; cpu++) {
            Assert.assertEquals(targets[cpu], CbpfInterpreter.run(program, cpu, 0));
        }
    }

    @Test
    public void testEstimateKernelBytes() {
        // 192 alternating CPUs as a chain: 386 instructions fit the default optmem_max of 20 KB
        Assert.assertTrue(SteeringProgram.estimateKernelBytes(386) < 20_480);
        // 1000 alternating ranges don't
        Assert.assertTrue(SteeringProgram.estimateKernelBytes(2002) > 20_480);
    }

    @Test
    public void testInstructionPacking() {
        final long insn = instruction(CODE_JMP_JGT_K, 1, 2, -1);
        Assert.assertEquals(CODE_JMP_JGT_K, getCode(insn));
        Assert.assertEquals(1, getJt(insn));
        Assert.assertEquals(2, getJf(insn));
        Assert.assertEquals(-1, getK(insn));
        // struct sock_filter in little-endian memory: code, jt, jf, k
        Assert.assertEquals(0xFFFF_FFFF_0201_0025L, insn);
    }

    @Test
    public void testRedirectNeedsTwoListeners() {
        try {
            SteeringProgram.build(new int[]{TARGET_REDIRECT, 0}, 2, new int[]{0}, 1, new LongList());
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("at least two"));
        }
    }

    @Test
    public void testRedirectSpreadsOverAcceptingListeners() {
        // three groups on contiguous CPUs; group 0 at its limit redirects to listeners 2 and 0
        final int[] targets = new int[12];
        for (int cpu = 0; cpu < 12; cpu++) {
            targets[cpu] = cpu < 4 ? TARGET_REDIRECT : cpu < 8 ? 2 : 0;
        }
        final int[] redirects = {2, 0};
        final LongList program = new LongList();
        SteeringProgram.build(targets, 3, redirects, 2, program);
        final int[] hits = new int[3];
        for (int random = 0; random < 1000; random++) {
            hits[CbpfInterpreter.run(program, 1, random)]++;
        }
        Assert.assertEquals(500, hits[2]);
        Assert.assertEquals(500, hits[0]);
        Assert.assertEquals(0, hits[1]);
        Assert.assertEquals(2, CbpfInterpreter.run(program, 5, 3));
        Assert.assertEquals(0, CbpfInterpreter.run(program, 9, 3));
    }

    @Test
    public void testRedirectWithThreeTargets() {
        final int[] targets = {TARGET_REDIRECT, TARGET_REDIRECT, 0, 1, 2};
        final int[] redirects = {0, 1, 2};
        final LongList program = new LongList();
        SteeringProgram.build(targets, 4, redirects, 3, program);
        for (int random = 0; random < 9; random++) {
            Assert.assertEquals(random % 3, CbpfInterpreter.run(program, 0, random));
        }
    }
}
