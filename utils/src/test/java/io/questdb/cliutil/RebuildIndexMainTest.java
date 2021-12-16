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

package io.questdb.cliutil;

import org.junit.Assert;
import org.junit.Test;

public class RebuildIndexMainTest {
    @Test
    public void testNoArgsFails() {
        Assert.assertNull(RebuildIndexMain.parseCommandArgs(
                new String[] {}
        ));
    }

    @Test
    public void testNoTableFails() {
        Assert.assertNull(RebuildIndexMain.parseCommandArgs(
                new String[] {"-p", "2222"}
        ));
    }

    @Test
    public void testTooManyPartitionArgsFails() {
        Assert.assertNull(RebuildIndexMain.parseCommandArgs(
                new String[] {"tablePath", "-p", "2222", "-p", "2223"}
        ));
    }

    @Test
    public void testTooColumnManyArgsFails() {
        Assert.assertNull(RebuildIndexMain.parseCommandArgs(
                new String[] {"tablePath", "-c", "2222", "-c", "2223"}
        ));
        Assert.assertNull(RebuildIndexMain.parseCommandArgs(
                new String[] {"tablePath", "-p", "2222", "-c", "dafda", "-c", "asb"}
        ));
    }

    @Test
    public void testTableNameOnly() {
        RebuildIndexMain.CommandLineArgs params = RebuildIndexMain.parseCommandArgs(
                new String[] {"tablePath"}
        );
        Assert.assertNotNull(params);
        Assert.assertEquals("tablePath",  params.tablePath);
    }

    @Test
    public void testPartitionOnly() {
        RebuildIndexMain.CommandLineArgs params = RebuildIndexMain.parseCommandArgs(
                new String[] {"tablePath", "-p", "9393"}
        );
        Assert.assertNotNull(params);
        Assert.assertEquals("tablePath",  params.tablePath);
        Assert.assertEquals("9393",  params.partition);
    }

    @Test
    public void testColumnOnly() {
        RebuildIndexMain.CommandLineArgs params = RebuildIndexMain.parseCommandArgs(
                new String[] {"tablePath", "-c", "9393"}
        );
        Assert.assertNotNull(params);
        Assert.assertEquals("tablePath",  params.tablePath);
        Assert.assertEquals("9393",  params.column);
    }

    @Test
    public void testColumnAndPartition() {
        RebuildIndexMain.CommandLineArgs params = RebuildIndexMain.parseCommandArgs(
                new String[] {"tablePath", "-c", "abc", "-p", "2020"}
        );
        Assert.assertNotNull(params);
        Assert.assertEquals("tablePath",  params.tablePath);
        Assert.assertEquals("abc",  params.column);
        Assert.assertEquals("2020",  params.partition);

        params = RebuildIndexMain.parseCommandArgs(
                new String[] {"tablePath", "-p", "2020", "-c", "abc"}
        );
        Assert.assertNotNull(params);
        Assert.assertEquals("tablePath",  params.tablePath);
        Assert.assertEquals("abc",  params.column);
        Assert.assertEquals("2020",  params.partition);
    }
}
