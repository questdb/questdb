/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cairo;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.Files;
import com.questdb.misc.Unsafe;
import com.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class CairoMemoryTest {
    private static final int N = 1000000;
    private static final Log LOG = LogFactory.getLog(CairoMemoryTest.class);

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @BeforeClass
    public static void setUp() throws Exception {
        LOG.info().$("Begin test").$();
    }

    @Test
    public void testAppendAndReadWithReadOnlyMem() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path(temp.newFile().getAbsolutePath())) {
            long size;
            try (AppendMemory mem = new AppendMemory(path, 2 * Files.PAGE_SIZE, 0)) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, size = mem.size());
            }
            try (ReadOnlyMemory mem = new ReadOnlyMemory(path, Files.PAGE_SIZE, size)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testAppendMemoryJump() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path(temp.newFile().getAbsolutePath())) {
            try (AppendMemory mem = new AppendMemory(path, Files.PAGE_SIZE, 0)) {
                for (int i = 0; i < 100; i++) {
                    mem.putLong(i);
                    mem.skip(2 * Files.PAGE_SIZE);
                }
                mem.jumpTo(0);
                Assert.assertEquals((8 + 2 * Files.PAGE_SIZE) * 100, mem.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testAppendMemoryReuse() throws Exception {
        long used = Unsafe.getMemUsed();
        try (AppendMemory mem = new AppendMemory()) {
            for (int j = 0; j < 10; j++) {
                try (Path path = new Path(temp.newFile().getAbsolutePath())) {
                    long size;
                    mem.of(path, 2 * Files.PAGE_SIZE, 0);
                    for (int i = 0; i < N; i++) {
                        mem.putLong(i);
                    }
                    Assert.assertEquals(8L * N, size = mem.size());

                    try (ReadOnlyMemory ro = new ReadOnlyMemory(path, Files.PAGE_SIZE, size)) {
                        for (int i = 0; i < N; i++) {
                            Assert.assertEquals(i, ro.getLong(i * 8));
                        }
                    }
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testReadWriteMemoryJump() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path(temp.newFile().getAbsolutePath())) {
            try (ReadWriteMemory mem = new ReadWriteMemory(path, Files.PAGE_SIZE, 0, Files.PAGE_SIZE)) {
                for (int i = 0; i < 100; i++) {
                    mem.putLong(i);
                    mem.skip(2 * Files.PAGE_SIZE);
                }
                mem.jumpTo(0);
                Assert.assertEquals((8 + 2 * Files.PAGE_SIZE) * 100, mem.size());
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testWriteAndRead() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path(temp.newFile().getAbsolutePath())) {
            long size;
            try (ReadWriteMemory mem = new ReadWriteMemory(path, 2 * Files.PAGE_SIZE, 0, Files.PAGE_SIZE)) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                // read in place
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }

                Assert.assertEquals(8L * N, size = mem.size());
            }
            try (ReadWriteMemory mem = new ReadWriteMemory(path, Files.PAGE_SIZE * Files.PAGE_SIZE, size, Files.PAGE_SIZE)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }

    @Test
    public void testWriteAndReadWithReadOnlyMem() throws Exception {
        long used = Unsafe.getMemUsed();
        try (Path path = new Path(temp.newFile().getAbsolutePath())) {
            long size;
            try (ReadWriteMemory mem = new ReadWriteMemory(path, 2 * Files.PAGE_SIZE, 0, Files.PAGE_SIZE)) {
                for (int i = 0; i < N; i++) {
                    mem.putLong(i);
                }
                Assert.assertEquals(8L * N, size = mem.size());
            }
            try (ReadOnlyMemory mem = new ReadOnlyMemory(path, Files.PAGE_SIZE, size)) {
                for (int i = 0; i < N; i++) {
                    Assert.assertEquals(i, mem.getLong(i * 8));
                }
            }
        }
        Assert.assertEquals(used, Unsafe.getMemUsed());
    }
}