/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

import io.questdb.std.Filewatcher;
import io.questdb.std.Os;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

public class FilewatcherTest {
    @Rule
    public TemporaryFolder temp = new TemporaryFolder();
    @Test
    public void testFilewatcherSetupAndTeardown() throws Exception {

        final File targetFile = temp.newFile();

        try(Path path = new Path()) {
            path.of(targetFile.getAbsolutePath()).$();
            final long addr = Filewatcher.setup(path.ptr());

            Assert.assertNotEquals(0, addr);
            Filewatcher.teardown(addr);
        }
    }

    @Test
    public void testFilewatcherChanged() throws Exception {

        final File targetFile = temp.newFile();

        try (Path path = new Path()) {
            path.of(targetFile.getAbsolutePath()).$();
            final long addr = Filewatcher.setup(path.ptr());

            Assert.assertNotEquals(0, addr);
            Assert.assertFalse(Filewatcher.changed(addr));

            try (PrintWriter writer = new PrintWriter(path.toString(), StandardCharsets.UTF_8)) {
                writer.println("hello");
            }

            Assert.assertTrue(Filewatcher.changed(addr));
            Assert.assertFalse(Filewatcher.changed(addr));
            Assert.assertFalse(Filewatcher.changed(addr));

            try (PrintWriter writer = new PrintWriter(path.toString(), StandardCharsets.UTF_8)) {
                writer.println("hello again ");
            }

            Assert.assertTrue(Filewatcher.changed(addr));


            Filewatcher.teardown(addr);

        }
    }

    static {
        Os.init();
    }

}
