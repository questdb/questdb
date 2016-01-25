/*
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
 */

package com.nfsdb.logging;

import com.nfsdb.concurrent.RingQueue;
import com.nfsdb.concurrent.Sequence;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Misc;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class LoggerFactoryTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test(expected = LoggerError.class)
    public void testBadWriter() throws Exception {
        System.setProperty("nfslog", "/nfslog-bad-writer.conf");

        try (LoggerFactory factory = new LoggerFactory()) {
            LoggerFactory.configureFromSystemProperties(factory);
        }
    }

    @Test
    public void testMultiplexing() throws Exception {
        final File x = temp.newFile();
        final File y = temp.newFile();

        try (LoggerFactory factory = new LoggerFactory()) {

            factory.add(
                    new LogWriterConfig(LoggerFactory.LOG_LEVEL_INFO, new LogWriterFactory() {
                        @Override
                        public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                            LogFileWriter w = new LogFileWriter(ring, seq);
                            w.setLocation(x.getAbsolutePath());
                            return w;
                        }
                    }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
            );

            factory.add(
                    new LogWriterConfig(LoggerFactory.LOG_LEVEL_INFO, new LogWriterFactory() {
                        @Override
                        public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                            LogFileWriter w = new LogFileWriter(ring, seq);
                            w.setLocation(y.getAbsolutePath());
                            return w;
                        }
                    }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
            );

            factory.bind();
            factory.startThread();

            try {
                AsyncLogger logger = factory.create("x");
                for (int i = 0; i < 100000; i++) {
                    logger.xinfo().$("test ").$(i).$();
                }

                Thread.sleep(100);
                Assert.assertTrue(x.length() > 0);
                TestUtils.assertEquals(x, y);
            } finally {
                factory.haltThread();
            }
        }
    }

    @Test
    public void testNoConfig() throws Exception {
        System.setProperty("nfslog", "/nfslog2.conf");

        try (LoggerFactory factory = new LoggerFactory()) {
            LoggerFactory.configureFromSystemProperties(factory);

            AsyncLogger logger = factory.create("x");
            assertDisabled(logger.debug());
            assertEnabled(logger.info());
            assertEnabled(logger.error());
        }
    }

    @Test
    public void testNoDefault() throws Exception {
        System.setProperty("nfslog", "/nfslog1.conf");

        try (LoggerFactory factory = new LoggerFactory()) {
            LoggerFactory.configureFromSystemProperties(factory);

            AsyncLogger logger = factory.create("x");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());

            AsyncLogger logger1 = factory.create("com.nfsdb.x.y");
            assertEnabled(logger1.debug());
            assertDisabled(logger1.info());
            assertEnabled(logger1.error());
        }
    }

    @Test
    public void testPackageHierarchy() throws Exception {
        final File a = temp.newFile();
        final File b = temp.newFile();

        try (LoggerFactory factory = new LoggerFactory()) {
            factory.add(
                    new LogWriterConfig("com.nfsdb", LoggerFactory.LOG_LEVEL_INFO, new LogWriterFactory() {
                        @Override
                        public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                            LogFileWriter w = new LogFileWriter(ring, seq);
                            w.setLocation(a.getAbsolutePath());
                            return w;
                        }
                    }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
            );

            factory.add(
                    new LogWriterConfig("com.nfsdb.collections", LoggerFactory.LOG_LEVEL_INFO, new LogWriterFactory() {
                        @Override
                        public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                            LogFileWriter w = new LogFileWriter(ring, seq);
                            w.setLocation(b.getAbsolutePath());
                            return w;
                        }
                    }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
            );

            factory.bind();
            factory.startThread();

            AsyncLogger logger = factory.create("com.nfsdb.collections.X");
            logger.xinfo().$("this is for collections").$();

            AsyncLogger logger1 = factory.create("com.nfsdb.net.Y");
            logger1.xinfo().$("this is for network").$();

            // let async writer catch up in a busy environment
            Thread.sleep(100);

            Assert.assertEquals("this is for network" + Misc.EOL, Files.readStringFromFile(a));
            Assert.assertEquals("this is for collections" + Misc.EOL, Files.readStringFromFile(b));
        }
    }

    @Test
    public void testProgrammaticConfig() throws Exception {
        try (LoggerFactory factory = new LoggerFactory()) {
            factory.add(
                    new LogWriterConfig(LoggerFactory.LOG_LEVEL_INFO | LoggerFactory.LOG_LEVEL_DEBUG, new LogWriterFactory() {
                        @Override
                        public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                            return new StdOutWriter(ring, seq);
                        }
                    }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
            );

            factory.bind();

            AsyncLogger logger = factory.create("x");
            assertEnabled(logger.info());
            assertDisabled(logger.error());
            assertEnabled(logger.debug());
        }
    }

    @Test
    public void testSilent() throws Exception {
        System.setProperty("nfslog", "/nfslog-silent.conf");

        try (LoggerFactory factory = new LoggerFactory()) {
            LoggerFactory.configureFromSystemProperties(factory);

            AsyncLogger logger = factory.create("x");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());

            AsyncLogger logger1 = factory.create("com.nfsdb.x.y");
            assertDisabled(logger1.debug());
            assertDisabled(logger1.info());
            assertDisabled(logger1.error());
        }
    }

    @Test
    public void testStaticRouting() throws Exception {
        System.getProperties().remove("nfslog");
        AsyncLogger logger = LoggerFactory.getLogger("x");
        assertEnabled(logger.info());
        assertEnabled(logger.error());
        assertDisabled(logger.debug());
    }

    @Test
    public void testZeroLevel() throws Exception {
        try (LoggerFactory factory = new LoggerFactory()) {
            factory.add(
                    new LogWriterConfig(0, new LogWriterFactory() {
                        @Override
                        public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq) {
                            return new StdOutWriter(ring, seq);
                        }
                    }, LoggerFactory.DEFAULT_QUEUE_DEPTH, LoggerFactory.DEFAULT_MSG_SIZE)
            );

            factory.bind();

            AsyncLogger logger = factory.create("x");
            assertEnabled(logger.info());
            assertEnabled(logger.error());
            assertEnabled(logger.debug());
        }
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
