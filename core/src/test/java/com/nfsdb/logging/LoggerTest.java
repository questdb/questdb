/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.logging;

import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.Sequence;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class LoggerTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testMultiplexing() throws Exception {
        final File x = temp.newFile();
        final File y = temp.newFile();

        LoggerFactory.INSTANCE.add(
                new LogWriterConfig("", LoggerFactory.LOG_LEVEL_INFO, new LogWriterFactory() {
                    @Override
                    public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                        LogFileWriter w = new LogFileWriter(ring, seq);
                        w.setLocation(x.getAbsolutePath());
                        return w;
                    }
                }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
        );

        LoggerFactory.INSTANCE.add(
                new LogWriterConfig("", LoggerFactory.LOG_LEVEL_INFO, new LogWriterFactory() {
                    @Override
                    public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                        LogFileWriter w = new LogFileWriter(ring, seq);
                        w.setLocation(y.getAbsolutePath());
                        return w;
                    }
                }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
        );

        LoggerFactory.INSTANCE.bind();
        LoggerFactory.INSTANCE.startThread();

        AsyncLogger logger = LoggerFactory.getLogger("x");
        for (int i = 0; i < 100000; i++) {
            logger.xinfo()._("test ")._(i).$();
        }

        Assert.assertTrue(x.length() > 0);
        TestUtils.assertEquals(x, y);
    }

    @Test
    public void testNoDefault() throws Exception {
        System.setProperty("nfslog", "/nfslog1.conf");

        AsyncLogger logger = LoggerFactory.getLogger("x");
        assertDisabled(logger.debug());
        assertDisabled(logger.info());
        assertDisabled(logger.error());

        AsyncLogger logger1 = LoggerFactory.getLogger("com.nfsdb.x.y");
        assertEnabled(logger1.debug());
        assertDisabled(logger1.info());
        assertEnabled(logger1.error());
    }

    @Test
    public void testProgrammaticConfig() throws Exception {
        LoggerFactory.INSTANCE.add(
                new LogWriterConfig("", LoggerFactory.LOG_LEVEL_INFO | LoggerFactory.LOG_LEVEL_DEBUG, new LogWriterFactory() {
                    @Override
                    public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                        return new StdOutWriter(ring, seq);
                    }
                }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
        );

        LoggerFactory.INSTANCE.bind();

        AsyncLogger logger = LoggerFactory.getLogger("x");
        assertEnabled(logger.info());
        assertDisabled(logger.error());
        assertEnabled(logger.debug());
    }

    @Test
    public void testStaticRouting() throws Exception {
        AsyncLogger logger = LoggerFactory.getLogger("x");
        assertEnabled(logger.info());
        assertEnabled(logger.error());
        assertDisabled(logger.debug());
    }

    private static void assertEnabled(LogRecord r) {
        Assert.assertTrue(r.isEnabled());
        r.$();
    }

    private static void assertDisabled(LogRecord r) {
        Assert.assertFalse(r.isEnabled());
        r.$();
    }
}
