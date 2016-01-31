/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

package com.nfsdb.log;

import com.nfsdb.misc.Files;
import com.nfsdb.misc.Misc;
import com.nfsdb.mp.RingQueue;
import com.nfsdb.mp.Sequence;
import com.nfsdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class LogFactoryTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test(expected = LogError.class)
    public void testBadWriter() throws Exception {
        System.setProperty("nfslog", "/nfslog-bad-writer.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);
        }
    }

    @Test
    public void testDefaultLevel() throws Exception {
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_ALL, new LogWriterFactory() {
                @Override
                public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                    return new LogConsoleWriter(ring, seq, level);
                }
            }));

            factory.bind();

            Log logger = factory.create("x");
            assertEnabled(logger.info());
            assertEnabled(logger.error());
            assertEnabled(logger.debug());
        }
    }

    @Test
    public void testMultiplexing() throws Exception {
        final File x = temp.newFile();
        final File y = temp.newFile();

        try (LogFactory factory = new LogFactory()) {

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, new LogWriterFactory() {
                @Override
                public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                    LogFileWriter w = new LogFileWriter(ring, seq, level);
                    w.setLocation(x.getAbsolutePath());
                    return w;
                }
            }));

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, new LogWriterFactory() {
                @Override
                public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                    LogFileWriter w = new LogFileWriter(ring, seq, level);
                    w.setLocation(y.getAbsolutePath());
                    return w;
                }
            }));

            factory.bind();
            factory.startThread();

            try {
                Log logger = factory.create("x");
                for (int i = 0; i < 100000; i++) {
                    logger.xinfo().$("test ").$(' ').$(i).$();
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

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertEnabled(logger.info());
            assertEnabled(logger.error());
        }
    }

    @Test
    public void testNoDefault() throws Exception {
        System.setProperty("nfslog", "/nfslog1.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());

            Log logger1 = factory.create("com.nfsdb.x.y");
            assertEnabled(logger1.debug());
            assertDisabled(logger1.info());
            assertEnabled(logger1.error());
        }
    }

    @Test
    public void testOverlappedMultiplexing() throws Exception {
        final File x = temp.newFile();
        final File y = temp.newFile();

        try (LogFactory factory = new LogFactory()) {

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO | LogLevel.LOG_LEVEL_DEBUG, new LogWriterFactory() {
                @Override
                public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                    LogFileWriter w = new LogFileWriter(ring, seq, level);
                    w.setLocation(x.getAbsolutePath());
                    return w;
                }
            }));

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_DEBUG | LogLevel.LOG_LEVEL_ERROR, new LogWriterFactory() {
                @Override
                public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                    LogFileWriter w = new LogFileWriter(ring, seq, level);
                    w.setLocation(y.getAbsolutePath());
                    return w;
                }
            }));

            factory.bind();
            factory.startThread();

            try {
                Log logger = factory.create("x");
                for (int i = 0; i < 1000; i++) {
                    logger.xerror().$("test ").$(i).$();
                }

                Thread.sleep(100);

                Assert.assertEquals(0, x.length());
                Assert.assertEquals(9890, y.length());

                for (int i = 0; i < 1000; i++) {
                    logger.xinfo().$("test ").$(i).$();
                }

                Assert.assertEquals(9890, x.length());
                Assert.assertEquals(9890, y.length());

            } finally {
                factory.haltThread();
            }
        }
    }

    @Test
    public void testPackageHierarchy() throws Exception {
        final File a = temp.newFile();
        final File b = temp.newFile();

        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig("com.nfsdb", LogLevel.LOG_LEVEL_INFO, new LogWriterFactory() {
                @Override
                public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                    LogFileWriter w = new LogFileWriter(ring, seq, level);
                    w.setLocation(a.getAbsolutePath());
                    return w;
                }
            }));

            factory.add(new LogWriterConfig("com.nfsdb.std", LogLevel.LOG_LEVEL_INFO, new LogWriterFactory() {
                @Override
                public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                    LogFileWriter w = new LogFileWriter(ring, seq, level);
                    w.setLocation(b.getAbsolutePath());
                    return w;
                }
            }));

            factory.bind();
            factory.startThread();

            Log logger = factory.create("com.nfsdb.std.X");
            logger.xinfo().$("this is for std").$();

            Log logger1 = factory.create("com.nfsdb.net.Y");
            logger1.xinfo().$("this is for network").$();

            // let async writer catch up in a busy environment
            Thread.sleep(100);

            Assert.assertEquals("this is for network" + Misc.EOL, Files.readStringFromFile(a));
            Assert.assertEquals("this is for std" + Misc.EOL, Files.readStringFromFile(b));
        }
    }

    @Test
    public void testProgrammaticConfig() throws Exception {
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO | LogLevel.LOG_LEVEL_DEBUG, new LogWriterFactory() {
                @Override
                public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                    return new LogConsoleWriter(ring, seq, level);
                }
            }));

            factory.bind();

            Log logger = factory.create("x");
            assertEnabled(logger.info());
            assertDisabled(logger.error());
            assertEnabled(logger.debug());
        }
    }

    @Test
    public void testSetProperties() throws Exception {
        File conf = temp.newFile();
        File out = temp.newFile();

        Files.writeStringToFile(conf, "writers=file\n" +
                "recordLength=4096\n" +
                "queueDepth=1024\n" +
                "w.file.class=com.nfsdb.log.LogFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath() + "\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.bufferSize=4M"
        );

        System.setProperty("nfslog", conf.getAbsolutePath());

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log log = factory.create("xyz");

            log.xinfo().$("hello").$();

            Assert.assertEquals(1, factory.getJobs().size());
            Assert.assertTrue(factory.getJobs().get(0) instanceof LogFileWriter);

            LogFileWriter w = (LogFileWriter) factory.getJobs().get(0);

            Assert.assertEquals(4 * 1024 * 1024, w.getBufSize());

            Assert.assertEquals(1024, factory.getQueueDepth());
            Assert.assertEquals(4096, factory.getRecordLength());
        }
    }

    @Test
    public void testSilent() throws Exception {
        System.setProperty("nfslog", "/nfslog-silent.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());

            Log logger1 = factory.create("com.nfsdb.x.y");
            assertDisabled(logger1.debug());
            assertDisabled(logger1.info());
            assertDisabled(logger1.error());
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
