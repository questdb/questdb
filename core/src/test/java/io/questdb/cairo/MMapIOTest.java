/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb.cairo;

import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;


public class MMapIOTest {
    public static final int pageSizeMb = 16;
    public static final double diskSizeMb = 63.5;
    public static final String fileName = "/mnt/60MBdisk/file.name";
    public static final int pageSize = pageSizeMb * 1024 * 1024;
    private static final sun.misc.Unsafe unsafe = Unsafe.getUnsafe();
    private static final String expectedErrorMsg = "fault occurred in a recent unsafe memory access";
    private static final String failureMsg1 = "InternalError not thrown";
    private static final String failureMsg2 = "Wrong InternalError: ";

    @Test
    public void TestInternalError() throws Throwable {

        if (Files.exists(java.nio.file.Path.of(fileName))) {
            Files.delete(java.nio.file.Path.of(fileName));
        }
        Path p = new Path().put(fileName).$();

        int expectedPages = (int)diskSizeMb / pageSizeMb;
        long iteration = -2;
        try (AppendMemory am = new AppendMemory(FilesFacadeImpl.INSTANCE, p, pageSize)) {
            int i = 0;
            long j = 0;
            try {
                for (i = 0; i < expectedPages + 1; i++) {
                    j = 0;
                    am.mapWritePage(i);
                    for (j = 0; j < pageSize / 8; j++) {
                        am.putLong(0L);
                    }
                }
                am.releaseCurrentPage();
            } catch (InternalError e) {
                iteration = i * pageSize + j * 8;
            } catch (CairoException e) {
                if (!e.message.toString().contains("Appender resize failed")) {
                    throw e;
                }
                iteration = i * pageSize + j * 8;
            }
        }
        // 50488040, 52754896
        double missMb = (iteration - expectedPages * pageSize) * 1.0 / 1024 / 1024;
        Assert.assertEquals("InternalError should be thrown on first iteration", 0, missMb, 1E-7);
    }
}
