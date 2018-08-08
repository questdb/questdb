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

package com.questdb.log;

import com.questdb.std.Misc;
import com.questdb.store.Files;
import com.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class LogFactoryTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test(expected = LogError.class)
    public void testBadWriter() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/nfslog-bad-writer.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);
        }
    }

    @Test
    public void testDefaultLevel() {
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_ALL, LogConsoleWriter::new));

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

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(y.getAbsolutePath());
                return w;
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
    public void testNoConfig() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/nfslog2.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertEnabled(logger.info());
            assertEnabled(logger.error());
        }
    }

    @Test
    public void testNoDefault() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/nfslog1.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());

            Log logger1 = factory.create("com.questdb.x.y");
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

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO | LogLevel.LOG_LEVEL_DEBUG, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_DEBUG | LogLevel.LOG_LEVEL_ERROR, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(y.getAbsolutePath());
                return w;
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

                Thread.sleep(100);

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
            factory.add(new LogWriterConfig("com.questdb", LogLevel.LOG_LEVEL_INFO, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(a.getAbsolutePath());
                return w;
            }));

            factory.add(new LogWriterConfig("com.questdb.std", LogLevel.LOG_LEVEL_INFO, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(b.getAbsolutePath());
                return w;
            }));

            factory.bind();
            factory.startThread();

            Log logger = factory.create("com.questdb.std.X");
            logger.xinfo().$("this is for std").$();

            Log logger1 = factory.create("com.questdb.net.Y");
            logger1.xinfo().$("this is for network").$();

            // let async writer catch up in a busy environment
            Thread.sleep(100);

            Assert.assertEquals("this is for network" + Misc.EOL, com.questdb.store.Files.readStringFromFile(a));
            Assert.assertEquals("this is for std" + Misc.EOL, com.questdb.store.Files.readStringFromFile(b));
        }
    }

    @Test
    public void testProgrammaticConfig() {
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO | LogLevel.LOG_LEVEL_DEBUG, LogConsoleWriter::new));

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
                "w.file.class=com.questdb.log.LogFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.bufferSize=4M"
        );

        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());

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
    public void testSilent() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/nfslog-silent.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());

            Log logger1 = factory.create("com.questdb.x.y");
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
