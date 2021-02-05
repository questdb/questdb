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

import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class AbstractCairoTest {

    protected static final StringSink sink = new StringSink();
    protected static final RecordCursorPrinter printer = new RecordCursorPrinter(sink);
    private final static Log LOG = LogFactory.getLog(AbstractCairoTest.class);
    @ClassRule
    public static TemporaryFolder temp = new TemporaryFolder();
    protected static CharSequence root;
    protected static CairoConfiguration configuration;
    protected static long currentMicros = -1;
    protected static MicrosecondClock testMicrosClock =
            () -> currentMicros >= 0 ? currentMicros : MicrosecondClockImpl.INSTANCE.getTicks();

    @BeforeClass
    public static void setUp() throws IOException {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("begin").$();
        root = temp.newFolder("dbRoot").getAbsolutePath();
        configuration = new DefaultCairoConfiguration(root) {
            @Override
            public MicrosecondClock getMicrosecondClock() {
                return testMicrosClock;
            }
        };
    }

    @Before
    public void setUp0() {
        try (Path path = new Path().of(root).$()) {
            if (Files.exists(path)) {
                return;
            }
            Files.mkdirs(path.of(root).put(Files.SEPARATOR).$(), configuration.getMkDirMode());
        }
    }

    @After
    public void tearDown0() {
        Path path = Path.getThreadLocal(root);
        Files.rmdir(path.$());
    }

    protected void assertColumn(CharSequence expected, CharSequence tableName, int index) {
        try (TableReader reader = new TableReader(configuration, tableName)) {
            final StringSink sink = new StringSink();
            final RecordCursorPrinter printer = new RecordCursorPrinter(sink);
            sink.clear();
            printer.printFullColumn(reader.getCursor(), reader.getMetadata(), index, false);
            TestUtils.assertEquals(expected, sink);
            reader.getCursor().toTop();
            sink.clear();
            printer.printFullColumn(reader.getCursor(), reader.getMetadata(), index, false);
            TestUtils.assertEquals(expected, sink);
        }
    }

    protected void assertOnce(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header) {
        sink.clear();
        printer.print(cursor, metadata, header);
        TestUtils.assertEquals(expected, sink);
    }

    protected void assertThat(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header) {
        assertOnce(expected, cursor, metadata, header);
        cursor.toTop();
        assertOnce(expected, cursor, metadata, header);
    }
}
