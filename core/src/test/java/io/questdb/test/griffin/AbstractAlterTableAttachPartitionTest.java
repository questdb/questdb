/*******************************************************************************
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

package io.questdb.test.griffin;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.Misc;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;


abstract class AbstractAlterTableAttachPartitionTest extends AbstractCairoTest {
    final static StringSink partitions = new StringSink();
    @Rule
    public TestName testName = new TestName();
    protected Path other;
    protected Path path;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        other = new Path();
        path = new Path();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        path = Misc.free(path);
        other = Misc.free(other);
    }

    void copyPartitionAndMetadata(
            CharSequence srcRoot,
            TableToken srcTableToken,
            String srcPartitionName,
            CharSequence dstRoot,
            String dstTableName,
            String dstPartitionName,
            String dstPartitionNameSuffix
    ) {
        path.of(srcRoot)
                .concat(srcTableToken)
                .concat(srcPartitionName)
                .slash$();
        other.of(dstRoot)
                .concat(dstTableName)
                .concat(dstPartitionName);

        if (!Chars.isBlank(dstPartitionNameSuffix)) {
            other.put(dstPartitionNameSuffix);
        }
        other.slash$();

        TestUtils.copyDirectory(path, other, configuration.getMkDirMode());

        // copy _meta
        Files.copy(
                path.parent().parent().concat(TableUtils.META_FILE_NAME).$(),
                other.parent().concat(TableUtils.META_FILE_NAME).$());
        // copy _cv
        Files.copy(
                path.parent().concat(TableUtils.COLUMN_VERSION_FILE_NAME).$(),
                other.parent().concat(TableUtils.COLUMN_VERSION_FILE_NAME).$());
        // copy _txn
        Files.copy(
                path.parent().concat(TableUtils.TXN_FILE_NAME).$(),
                other.parent().concat(TableUtils.TXN_FILE_NAME).$());
    }
}
