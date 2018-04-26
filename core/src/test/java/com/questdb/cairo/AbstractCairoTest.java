/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.cairo.sql.RecordCursor;
import com.questdb.cairo.sql.RecordMetadata;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Files;
import com.questdb.std.str.Path;
import com.questdb.std.str.StringSink;
import com.questdb.test.tools.TestUtils;
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

    @BeforeClass
    public static void setUp() {
        // it is necessary to initialise logger before tests start
        // logger doesn't relinquish memory until JVM stops
        // which causes memory leak detector to fail should logger be
        // created mid-test
        LOG.info().$("begin").$();
        root = temp.getRoot().getAbsolutePath();
        configuration = new DefaultCairoConfiguration(root);
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
        try (Path path = new Path().of(root)) {
            Files.rmdir(path.$());
        }
    }

    protected void assertThat(CharSequence expected, RecordCursor cursor, RecordMetadata metadata, boolean header) throws IOException {
        sink.clear();
        printer.print(cursor, header, metadata);
        TestUtils.assertEquals(expected, sink);

        cursor.toTop();

        sink.clear();
        printer.print(cursor, header, metadata);
        TestUtils.assertEquals(expected, sink);
    }
}
