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

package io.questdb.log;

import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClock;
import io.questdb.std.datetime.microtime.TimestampFormatUtils;
import io.questdb.std.str.Path;
import io.questdb.std.str.StringSink;
import io.questdb.test.tools.TestUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.CoreMatchers.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogFactoryTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testBadWriter() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/test-log-bad-writer.conf");
        try (LogFactory factory = new LogFactory()) {
            try {
                LogFactory.configureFromSystemProperties(factory, null, null);
                Assert.fail();
            } catch (LogError e) {
                Assert.assertEquals("Class not found com.questdb.log.StdOutWriter2", e.getMessage());
            }
        }
    }

    @Test
    public void testDefaultLevel() {
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.ALL, LogConsoleWriter::new));

            factory.bind();

            Log logger = factory.create("x");
            assertEnabled(logger.info());
            assertEnabled(logger.error());
            assertEnabled(logger.critical());
            assertEnabled(logger.debug());
            assertEnabled(logger.advisory());
        }
    }

    @Test
    public void testHexLongWrite() throws Exception {
        final File x = temp.newFile();
        final File y = temp.newFile();

        try (LogFactory factory = new LogFactory()) {

            factory.add(new LogWriterConfig(LogLevel.INFO | LogLevel.DEBUG, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.add(new LogWriterConfig(LogLevel.DEBUG | LogLevel.ERROR, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(y.getAbsolutePath());
                return w;
            }));

            factory.bind();
            factory.startThread();

            try {
                Log logger = factory.create("x");
                for (int i = 0; i < 64; i++) {
                    logger.xerror().$("test ").$hex(i).$();
                }

                Os.sleep(100);

                Assert.assertEquals(0, x.length());
                Assert.assertEquals(576, y.length());
            } finally {
                factory.haltThread();
            }
        }
    }

    @Test
    public void testMultiplexing() throws Exception {
        final File x = temp.newFile();
        final File y = temp.newFile();

        try (LogFactory factory = new LogFactory()) {

            factory.add(new LogWriterConfig(LogLevel.INFO, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.add(new LogWriterConfig(LogLevel.INFO, (ring, seq, level) -> {
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

                Os.sleep(100);
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
            assertEnabled(logger.critical());
            assertEnabled(logger.advisory());
        }
    }

    @Test
    public void testNoDefault() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/test-log.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());
            assertDisabled(logger.critical());
            assertDisabled(logger.advisory());

            Log logger1 = factory.create("com.questdb.x.y");
            assertEnabled(logger1.debug());
            assertDisabled(logger1.info());
            assertEnabled(logger1.error());
            assertEnabled(logger1.critical());
            assertEnabled(logger1.advisory());
        }
    }

    @Test
    public void testOverlappedMultiplexing() throws Exception {
        final File x = temp.newFile();
        final File y = temp.newFile();

        try (LogFactory factory = new LogFactory()) {

            final SOUnboundedCountDownLatch xLatch = new SOUnboundedCountDownLatch();
            final SOUnboundedCountDownLatch yLatch = new SOUnboundedCountDownLatch();

            factory.add(new LogWriterConfig(LogLevel.INFO | LogLevel.DEBUG, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                final QueueConsumer<LogRecordSink> consumer = w.getMyConsumer();
                w.setMyConsumer(slot -> {
                    xLatch.countDown();
                    consumer.consume(slot);
                });
                return w;
            }));

            factory.add(new LogWriterConfig(LogLevel.DEBUG | LogLevel.ERROR, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(y.getAbsolutePath());
                final QueueConsumer<LogRecordSink> consumer = w.getMyConsumer();
                w.setMyConsumer(slot -> {
                    yLatch.countDown();
                    consumer.consume(slot);
                });
                return w;
            }));

            factory.bind();
            factory.startThread();

            try {
                Log logger = factory.create("x");
                for (int i = 0; i < 1000; i++) {
                    logger.xerror().$("test ").$(i).$();
                }

                yLatch.await(-1000);
                Os.sleep(500);

                for (int i = 0; i < 1000; i++) {
                    logger.xinfo().$("test ").$(i).$();
                }

                xLatch.await(-1000);
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
            factory.add(new LogWriterConfig("com.questdb", LogLevel.INFO, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(a.getAbsolutePath());
                return w;
            }));

            factory.add(new LogWriterConfig("com.questdb.std", LogLevel.INFO, (ring, seq, level) -> {
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
            Os.sleep(100);

            Assert.assertEquals("this is for network" + Misc.EOL, TestUtils.readStringFromFile(a));
            Assert.assertEquals("this is for std" + Misc.EOL, TestUtils.readStringFromFile(b));
        }
    }

    @Test
    public void testProgrammaticConfig() {
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.INFO | LogLevel.DEBUG, LogConsoleWriter::new));

            factory.bind();

            Log logger = factory.create("x");
            assertEnabled(logger.info());
            assertDisabled(logger.error());
            assertDisabled(logger.critical());
            assertEnabled(logger.debug());
            assertDisabled(logger.advisory());
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
        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String logFile = base + "mylog-${date:yyyy-MM-dd}.log";
        String expectedLogFile = base + "mylog-2015-05-03.log";

        final MicrosecondClock clock = new TestMicrosecondClock(TimestampFormatUtils.parseTimestamp("2015-05-03T10:35:00.000Z"), 1);

        try (Path path = new Path()) {
            // create rogue file that would be in a way of logger rolling existing files
            path.of(base);
            Assert.assertTrue(Files.touch(path.concat("mylog-2015-05-03.log.2").$()));
        }

        RingQueue<LogRecordSink> queue = new RingQueue<>(
                LogRecordSink::new,
                1024,
                1024,
                MemoryTag.NATIVE_DEFAULT
        );

        SPSequence pubSeq = new SPSequence(queue.getCycle());
        SCSequence subSeq = new SCSequence();
        pubSeq.then(subSeq).then(pubSeq);

        try (final LogRollingFileWriter writer = new LogRollingFileWriter(
                FilesFacadeImpl.INSTANCE,
                clock,
                queue,
                subSeq,
                LogLevel.INFO
        )) {

            writer.setLocation(logFile);
            writer.setRollSize("1m");
            writer.setBufferSize("64k");
            writer.bindProperties(LogFactory.INSTANCE);

            AtomicBoolean running = new AtomicBoolean(true);
            SOCountDownLatch halted = new SOCountDownLatch();
            halted.setCount(1);

            new Thread(() -> {
                while (running.get()) {
                    writer.runSerially();
                }

                //noinspection StatementWithEmptyBody
                while (writer.runSerially()) ;

                halted.countDown();
            }).start();


            // now publish
            int published = 0;
            int toPublish = 100_000;
            while (published < toPublish) {
                long cursor = pubSeq.next();

                if (cursor < 0) {
                    Os.pause();
                    continue;
                }

                final long available = pubSeq.available();

                while (cursor < available && published < toPublish) {
                    LogRecordSink sink = queue.get(cursor++);
                    sink.setLevel(LogLevel.INFO);
                    sink.put("test");
                    published++;
                }

                pubSeq.done(cursor - 1);
            }

            running.set(false);
            halted.await();
        }
        assertFileLength(expectedLogFile);
        assertFileLength(expectedLogFile + ".1");
    }

    @Test
    public void testRollingFileWriterByYear() throws Exception {
        testRollOnDate("mylog-${date:yyyy-MM}.log", 12 * 30 * 24 * 60000L, "year", "mylog-201");
    }

    @Test
    public void testRollingFileWriterDateParse() throws Exception {
        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String logFile = base + "mylog-${date:yyyy-MM-dd}.log";
        String expectedLogFile = base + "mylog-2015-05-03.log";
        try (LogFactory factory = new LogFactory()) {
            final MicrosecondClock clock = new TestMicrosecondClock(TimestampFormatUtils.parseTimestamp("2015-05-03T11:35:00.000Z"), 1);

            factory.add(new LogWriterConfig(LogLevel.INFO, (ring, seq, level) -> {
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

                Os.sleep(100);
            } finally {
                factory.haltThread();
            }
        }
        Assert.assertTrue(new File(expectedLogFile).length() > 0);
    }

    @Test
    public void testRollingFileWriterDateParsePushFilesMid() throws Exception {
        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String expectedLogFile = base + "mylog-2015-05-03.log";
        try (LogFactory factory = new LogFactory()) {

            String logFile = base + "mylog-${date:yyyy-MM-dd}.log";

            final MicrosecondClock clock = new TestMicrosecondClock(TimestampFormatUtils.parseTimestamp("2015-05-03T10:35:00.000Z"), 1);

            try (Path path = new Path()) {

                path.of(base);
                Assert.assertTrue(Files.touch(path.concat("mylog-2015-05-03.log").$()));

                path.of(base);
                Assert.assertTrue(Files.touch(path.concat("mylog-2015-05-03.log.1").$()));

                path.of(base);
                Assert.assertTrue(Files.touch(path.concat("mylog-2015-05-03.log.2").$()));

                // there is a gap here, .3 is available
                path.of(base);
                Assert.assertTrue(Files.touch(path.concat("mylog-2015-05-03.log.4").$()));
            }

            factory.add(new LogWriterConfig(LogLevel.INFO, (ring, seq, level) -> {
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

                Os.sleep(1000);
            } finally {
                factory.haltThread();
            }
        }
        Assert.assertTrue(new File(expectedLogFile).length() > 0);
    }

    @Test
    public void testSetIncorrectQueueDepthProperty() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");
        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "recordLength=4096\n" +
                "queueDepth=banana\n" +
                "w.file.class=io.questdb.log.LogFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.bufferSize=4M"
        );
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Invalid value for queueDepth", e.getMessage());
        }
    }

    @Test
    public void testSetIncorrectRecordLengthProperty() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");
        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "recordLength=coconut\n" +
                "queueDepth=1024\n" +
                "w.file.class=io.questdb.log.LogFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.bufferSize=4M"
        );
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Invalid value for recordLength", e.getMessage());
        }
    }

    @Test
    public void testSetProperties() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");

        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "recordLength=4096\n" +
                "queueDepth=1024\n" +
                "w.file.class=io.questdb.log.LogFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.bufferSize=4M"
        );

        LogFactory.envEnabled = false;
        try {
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
        } finally {
            LogFactory.envEnabled = true;
        }
    }

    @Test
    public void testSetUnknownProperty() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");
        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "recordLength=4092\n" +
                "queueDepth=1024\n" +
                "w.file.class=io.questdb.log.LogFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.avocado=tasty\n" +
                "w.file.bufferSize=4M"
        );
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Unknown property: w.file.avocado", e.getMessage());
        }
    }

    @Test
    public void testSilent() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/test-log-silent.conf");

        try (LogFactory factory = new LogFactory()) {
            LogFactory.configureFromSystemProperties(factory);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());
            assertDisabled(logger.advisory());

            Log logger1 = factory.create("com.questdb.x.y");
            assertDisabled(logger1.debug());
            assertDisabled(logger1.info());
            assertDisabled(logger1.error());
            assertDisabled(logger1.advisory());
        }
    }

    @Test //also tests ${log.di} resolution
    public void testWhenCustomLogLocationIsNotSpecifiedThenDefaultLogFileIsUsed() throws Exception {
        System.clearProperty(LogFactory.CONFIG_SYSTEM_PROPERTY);

        testCustomLogIsCreated(true);
    }

    @Test
    public void testWhenCustomLogLocationIsSpecifiedThenDefaultLogFileIsNotUsed() throws IOException {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "test-log.conf");

        testCustomLogIsCreated(false);
    }

    private void testCustomLogIsCreated(boolean isCreated) throws IOException {
        try (LogFactory factory = new LogFactory()) {
            File logConfDir = Paths.get(temp.getRoot().getPath(), "conf").toFile();
            Assert.assertTrue(logConfDir.mkdir());

            File logConfFile = Paths.get(logConfDir.getPath(), LogFactory.DEFAULT_CONFIG_NAME).toFile();

            Properties props = new Properties();
            props.put("writers", "log_test");
            props.put("w.log_test.class", "io.questdb.log.LogFileWriter");
            props.put("w.log_test.location", "${log.dir}\\test.log");
            props.put("w.log_test.level", "INFO,ERROR");
            try (FileOutputStream stream = new FileOutputStream(logConfFile)) {
                props.store(stream, "");
            }

            LogFactory.configureFromSystemProperties(factory, null, temp.getRoot().getPath());

            File logFile = Paths.get(temp.getRoot().getPath(), "log\\test.log").toFile();
            MatcherAssert.assertThat(logFile.getAbsolutePath(), logFile.exists(), is(isCreated));
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

        final MicrosecondClock clock = new TestMicrosecondClock(TimestampFormatUtils.parseTimestamp("2015-05-03T10:35:00.000Z"), speed);

        long expectedFileCount = Files.getOpenFileCount();
        long expectedMemUsage = Unsafe.getMemUsed();

        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String logFile = base + fileTemplate;

        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.INFO, (ring, seq, level) -> {
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
            StringSink fileNameSink = new StringSink();
            path.of(base).$();
            long pFind = Files.findFirst(path);
            try {
                Assert.assertTrue(pFind != 0);
                do {
                    fileNameSink.clear();
                    Chars.utf8DecodeZ(Files.findName(pFind), fileNameSink);
                    if (Files.isDots(fileNameSink)) {
                        continue;
                    }
                    // don't hardcode hour, it is liable to vary
                    // because of different default timezones
                    TestUtils.assertContains(fileNameSink, mustContain);
                    Assert.assertFalse(Chars.contains(fileNameSink, ".1"));
                    fileCount++;
                } while (Files.findNext(pFind) > 0);

            } finally {
                Files.findClose(pFind);
            }
        }

        // this is a very weak assertion but we have to live with it
        // logger runs asynchronously, it doesn't offer any synchronisation
        // support right now, which leaves tests at a mercy of the hardware/OS/other things
        // consuming CPU and potentially starving logger of execution time
        // when this happens there is no guarantees on how many files it will create
        Assert.assertTrue(fileCount > 0);
        Assert.assertEquals(expectedFileCount, Files.getOpenFileCount());
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
