/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import com.questdb.std.*;
import com.questdb.std.microtime.DateFormatUtils;
import com.questdb.std.microtime.MicrosecondClock;
import com.questdb.std.str.NativeLPSZ;
import com.questdb.std.str.Path;
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

            Assert.assertEquals("this is for network" + Misc.EOL, Files.readStringFromFile(a));
            Assert.assertEquals("this is for std" + Misc.EOL, Files.readStringFromFile(b));
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
    public void testRollingFileWriterByDay() throws Exception {
        testRollOnDate("mylog-${date:yyyy-MM-dd}.log", 24 * 60000, "day", "mylog-2015-05");
    }

    @Test
    public void testRollingFileWriterByHour() throws Exception {
        testRollOnDate("mylog-${date:yyyy-MM-dd-hh}.log", 100000, "hour", "mylog-2015-05-03");
    }

    @Test
    public void testRollingFileWriterByMinute() throws Exception {
        testRollOnDate("mylog-${date:yyyy-MM-dd-hh-mm}.log", 1000, "minute", "mylog-2015-05-03");
    }

    @Test
    public void testRollingFileWriterByMonth() throws Exception {
        testRollOnDate("mylog-${date:yyyy-MM}.log", 30 * 24 * 60000, "month", "mylog-2015");
    }

    @Test
    public void testRollingFileWriterBySize() throws Exception {
        String base = temp.getRoot().getAbsolutePath() + com.questdb.std.Files.SEPARATOR;
        String logFile = base + "mylog-${date:yyyy-MM-dd}.log";
        String expectedLogFile = base + "mylog-2015-05-03.log";

        try (LogFactory factory = new LogFactory()) {
            final MicrosecondClock clock = new TestMicrosecondClock(DateFormatUtils.parseDateTime("2015-05-03T10:35:00.000Z"), 1);

            try (Path path = new Path()) {
                // create rogue file that would be in a way of logger rolling existing files
                path.of(base);
                Assert.assertTrue(com.questdb.std.Files.touch(path.concat("mylog-2015-05-03.log.2").$()));
            }

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, (ring, seq, level) -> {
                LogRollingFileWriter w = new LogRollingFileWriter(FilesFacadeImpl.INSTANCE, clock, ring, seq, level);
                w.setLocation(logFile);
                // 1Mb log file limit, we will create 4 of them
                w.setRollSize("1m");
                w.setBufferSize("1k");
                w.setRollEvery("day");
                return w;
            }));

            factory.bind();
            factory.startThread();

            try {
                Log logger = factory.create("x");
                for (int i = 0; i < 1000000; i++) {
                    logger.xinfo().$("test ").$(' ').$(i).$();
                }

                // logger is async, we need to let it finish writing
                Thread.sleep(2000);

            } finally {
                factory.haltThread();
            }
        }
        assertFileLength(expectedLogFile);
        assertFileLength(expectedLogFile + ".1");
        assertFileLength(expectedLogFile + ".2");
        assertFileLength(expectedLogFile + ".3");
    }

    @Test
    public void testRollingFileWriterByYear() throws Exception {
        testRollOnDate("mylog-${date:yyyy-MM}.log", 12 * 30 * 24 * 60000L, "month", "mylog-201");
    }

    @Test
    public void testRollingFileWriterDateParse() throws Exception {
        String base = temp.getRoot().getAbsolutePath() + com.questdb.std.Files.SEPARATOR;
        String logFile = base + "mylog-${date:yyyy-MM-dd}.log";
        String expectedLogFile = base + "mylog-2015-05-03.log";
        try (LogFactory factory = new LogFactory()) {
            final MicrosecondClock clock = new TestMicrosecondClock(DateFormatUtils.parseDateTime("2015-05-03T11:35:00.000Z"), 1);

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, (ring, seq, level) -> {
                LogRollingFileWriter w = new LogRollingFileWriter(FilesFacadeImpl.INSTANCE, clock, ring, seq, level);
                w.setLocation(logFile);
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
            } finally {
                factory.haltThread();
            }
        }
        Assert.assertTrue(new File(expectedLogFile).length() > 0);
    }

    @Test
    public void testRollingFileWriterDateParsePushFilesMid() throws Exception {
        String base = temp.getRoot().getAbsolutePath() + com.questdb.std.Files.SEPARATOR;
        String expectedLogFile = base + "mylog-2015-05-03.log";
        try (LogFactory factory = new LogFactory()) {

            String logFile = base + "mylog-${date:yyyy-MM-dd}.log";

            final MicrosecondClock clock = new TestMicrosecondClock(DateFormatUtils.parseDateTime("2015-05-03T10:35:00.000Z"), 1);

            try (Path path = new Path()) {

                path.of(base);
                Assert.assertTrue(com.questdb.std.Files.touch(path.concat("mylog-2015-05-03.log").$()));

                path.of(base);
                Assert.assertTrue(com.questdb.std.Files.touch(path.concat("mylog-2015-05-03.log.1").$()));

                path.of(base);
                Assert.assertTrue(com.questdb.std.Files.touch(path.concat("mylog-2015-05-03.log.2").$()));

                // there is a gap here, .3 is available
                path.of(base);
                Assert.assertTrue(com.questdb.std.Files.touch(path.concat("mylog-2015-05-03.log.4").$()));
            }

            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, (ring, seq, level) -> {
                LogRollingFileWriter w = new LogRollingFileWriter(FilesFacadeImpl.INSTANCE, clock, ring, seq, level);
                w.setLocation(logFile);
                w.setSpinBeforeFlush("1000000");
                return w;
            }));

            factory.bind();
            factory.startThread();

            try {
                Log logger = factory.create("x");
                for (int i = 0; i < 100000; i++) {
                    logger.xinfo().$("test ").$(' ').$(i).$();
                }

                Thread.sleep(1000);
            } finally {
                factory.haltThread();
            }
        }
        Assert.assertTrue(new File(expectedLogFile).length() > 0);
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

    private void assertFileLength(String file) {
        long len = new File(file).length();
        Assert.assertTrue("oops: " + len, len > 0L && len < 1073741824L);
    }

    private void testRollOnDate(
            String fileTemplate,
            long speed,
            String rollEvery,
            String mustContain
    ) throws NumericException {

        final MicrosecondClock clock = new TestMicrosecondClock(DateFormatUtils.parseDateTime("2015-05-03T10:35:00.000Z"), speed);

        long expectedFileCount = com.questdb.std.Files.getOpenFileCount();
        long expectedMemUsage = Unsafe.getMemUsed();

        String base = temp.getRoot().getAbsolutePath() + com.questdb.std.Files.SEPARATOR;
        String logFile = base + fileTemplate;

        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO, (ring, seq, level) -> {
                LogRollingFileWriter w = new LogRollingFileWriter(FilesFacadeImpl.INSTANCE, clock, ring, seq, level);
                w.setLocation(logFile);
                // 1Mb log file limit, we will create 4 of them
                w.setBufferSize("4k");
                w.setRollEvery(rollEvery);
                return w;
            }));

            factory.bind();
            factory.startThread();

            try {
                Log logger = factory.create("x");
                for (int i = 0; i < 10000; i++) {
                    logger.xinfo().$("test ").$(' ').$(i).$();
                }
            } finally {
                factory.haltThread();
            }
        }

        int fileCount = 0;
        try (Path path = new Path()) {
            NativeLPSZ lpsz = new NativeLPSZ();
            path.of(base).$();
            long pFind = com.questdb.std.Files.findFirst(path);
            try {
                Assert.assertTrue(pFind != 0);
                do {
                    lpsz.of(com.questdb.std.Files.findName(pFind));
                    if (com.questdb.std.Files.isDots(lpsz)) {
                        continue;
                    }
                    // don't hardcode hour, it is liable to vary
                    // because of different default timezones
                    TestUtils.assertContains(lpsz, mustContain);
                    Assert.assertFalse(Chars.contains(lpsz, ".1"));
                    fileCount++;
                } while (com.questdb.std.Files.findNext(pFind) > 0);

            } finally {
                com.questdb.std.Files.findClose(pFind);
            }
        }

        // this is a very weak assertion but we have to live with it
        // logger runs asynchronously, it doesn't offer any synchronisation
        // support right now, which leaves tests at a mercy of the hardware/OS/other things
        // consuming CPU and potentially starving logger of execution time
        // when this happens there is no guarantees on how many files it will create
        Assert.assertTrue(fileCount > 0);
        Assert.assertEquals(expectedFileCount, com.questdb.std.Files.getOpenFileCount());
        Assert.assertEquals(expectedMemUsage, Unsafe.getMemUsed());
    }

    private static class TestMicrosecondClock implements MicrosecondClock {
        private final long base;
        private final long start;
        private final long speed;

        public TestMicrosecondClock(long start, long speed) {
            this.base = Os.currentTimeMicros();
            this.start = start;
            this.speed = speed;
        }

        @Override
        public long getTicks() {
            long wall = Os.currentTimeMicros();
            return (wall - base) * speed + start;
        }
    }
}
