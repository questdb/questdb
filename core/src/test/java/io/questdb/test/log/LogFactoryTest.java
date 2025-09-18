/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.log;

import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.griffin.engine.QueryProgress;
import io.questdb.log.GuaranteedLogger;
import io.questdb.log.Log;
import io.questdb.log.LogConsoleWriter;
import io.questdb.log.LogError;
import io.questdb.log.LogFactory;
import io.questdb.log.LogFileWriter;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecord;
import io.questdb.log.LogRecordUtf8Sink;
import io.questdb.log.LogRollingFileWriter;
import io.questdb.log.LogWriter;
import io.questdb.log.LogWriterConfig;
import io.questdb.log.Logger;
import io.questdb.mp.QueueConsumer;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.SOCountDownLatch;
import io.questdb.mp.SOUnboundedCountDownLatch;
import io.questdb.mp.SPSequence;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Rnd;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.Micros;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.GcUtf8String;
import io.questdb.std.str.Path;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8s;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LogFactoryTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testBadWriter() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, Files.getResourcePath(getClass().getResource("/test-log-bad-writer.conf")));
        try (LogFactory factory = new LogFactory()) {
            try {
                factory.init(null);
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
    public void testDirectUtf8Sequence() throws Exception {
        final File x = temp.newFile();
        final String orig = "Здравей свят";
        final GcUtf8String src = new GcUtf8String(orig);

        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.ERROR, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.bind();
            factory.startThread();

            final Log logger = factory.create("x");
            logger.xerror().$(src).$();

            System.err.println(x.getAbsolutePath());

            Os.sleep(100);
            final String expected = orig + "\r\n";
            final String actual = java.nio.file.Files.readString(x.toPath());
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testFlushJobsAndClose() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/test-log.conf");

        final int messageCount = 20;
        AtomicInteger counter = new AtomicInteger();
        LogFactory factory = new LogFactory();
        try {
            factory.add(new LogWriterConfig(LogLevel.CRITICAL, (ring, seq, level) -> new LogWriter() {
                @Override
                public void bindProperties(LogFactory factory) {
                }

                @Override
                public boolean run(int workerId, @NotNull RunStatus runStatus) {
                    long cursor = seq.next();
                    if (cursor > -1) {
                        counter.incrementAndGet();
                        seq.done(cursor);
                        Os.pause();
                        return true;
                    }
                    Os.pause();
                    return false;
                }
            }));

            // Misbehaving Logger
            factory.add(new LogWriterConfig(LogLevel.CRITICAL, (ring, seq, level) -> new LogWriter() {
                @Override
                public void bindProperties(LogFactory factory) {
                }

                @Override
                public boolean run(int workerId, @NotNull RunStatus runStatus) {
                    throw new UnsupportedOperationException();
                }
            }));

            factory.bind();
            factory.startThread();

            Log logger1 = factory.create("com.questdb.x.y");
            for (int i = 0; i < messageCount; i++) {
                logger1.critical().$("test ").$(i).$();
            }
        } finally {
            factory.close(true);
        }
        Assert.assertEquals(messageCount, counter.get());
    }

    @Test
    public void testGuaranteedLogging() throws Exception {
        final File x = temp.newFile();
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.ERROR, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.bind();
            factory.startThread();

            final Log logger = factory.create("x");
            Assert.assertEquals(Logger.class, logger.getClass());

            LogFactory.enableGuaranteedLogging();

            final Log guaranteedLogger = factory.create("x");
            Assert.assertEquals(GuaranteedLogger.class, guaranteedLogger.getClass());

            LogFactory.disableGuaranteedLogging();

            final Log logger2 = factory.create("x");
            Assert.assertEquals(Logger.class, logger2.getClass());
        }
    }

    @Test
    public void testGuaranteedLoggingForClasses() throws Exception {
        final File x = temp.newFile();
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.ERROR, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(x.getAbsolutePath());
                return w;
            }));

            factory.bind();
            factory.startThread();

            Assert.assertEquals(Logger.class, getLogger().getClass());

            LogFactory.enableGuaranteedLogging(QueryProgress.class);
            Assert.assertEquals(GuaranteedLogger.class, getLogger().getClass());

            LogFactory.disableGuaranteedLogging(QueryProgress.class);
            Assert.assertEquals(Logger.class, getLogger().getClass());
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

            Log logger = factory.create("x");
            for (int i = 0; i < 64; i++) {
                logger.xerror().$("test ").$hex(i).$();
            }

            Os.sleep(100);

            Assert.assertEquals(0, x.length());
            Assert.assertEquals(576, y.length());
        }
    }

    @Test
    public void testLogAutoDeleteByDirectorySize40k() throws Exception {
        testAutoDelete("40k", null, "30k");
    }

    @Test
    public void testLogAutoDeleteByDirectorySize500k() throws Exception {
        testAutoDelete("500k", null, "30k");
    }

    @Test
    public void testLogAutoDeleteByDirectorySizeRandom() throws Exception {
        Rnd rnd = TestUtils.generateRandom(null);
        int fullSize = 2 + rnd.nextInt(498);
        int rollSize = Math.max(1, (1 + rnd.nextInt(fullSize - 1)) / 2);
        System.out.println("fullSize=" + fullSize + "k, rollSize=" + rollSize + "k");
        testAutoDelete(fullSize + "k", null, rollSize + "k");
    }

    @Test
    public void testLogAutoDeleteByFileAge1Year() throws Exception {
        testAutoDelete(null, "1y", "30k");
    }

    @Test
    public void testLogAutoDeleteByFileAge25days() throws Exception {
        testAutoDelete(null, "25d", "30k");
    }

    @Test
    public void testLogAutoDeleteByFileAge3weeks() throws Exception {
        testAutoDelete(null, "3w", "30k");
    }

    @Test
    public void testLogAutoDeleteByFileAge6months() throws Exception {
        testAutoDelete(null, "6m", "30k");
    }

    @Test
    public void testLogSequenceIsReleasedOnException() {
        try (LogFactory factory = new LogFactory()) {
            final StringSink sink = new StringSink();
            SOCountDownLatch latch = new SOCountDownLatch(1);

            factory.add(new LogWriterConfig(LogLevel.ALL, (ring, seq, level) -> new LogWriter() {
                @Override
                public void bindProperties(LogFactory factory) {
                }

                @Override
                public boolean run(int workerId, @NotNull RunStatus runStatus) {
                    return seq.consumeAll(ring, this::log);
                }

                private void log(LogRecordUtf8Sink record) {
                    sink.clear();
                    sink.put((Sinkable) record);
                    latch.countDown();
                }
            }));

            factory.bind();
            factory.startThread();
            Log logger = factory.create("x");

            try {
                logger.info().$("message 1").$(sink1 -> {
                    throw new NullPointerException();
                }).$(" message 2").$();
                Assert.fail();
            } catch (NullPointerException npe) {
                latch.await();
                TestUtils.assertContains(sink, " I x message 1");
            }

            latch.setCount(1);

            try {
                logger.critical().$("message A").$(new Object() {
                    @Override
                    public String toString() {
                        throw new NullPointerException();
                    }
                }).$(" message B").$();
                Assert.fail();
            } catch (NullPointerException npe) {
                latch.await();
                TestUtils.assertContains(sink, " C x message A");
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

            Log logger = factory.create("x");
            for (int i = 0; i < 100000; i++) {
                logger.xinfo().$("test ").$(' ').$(i).$();
            }

            Os.sleep(100);
            Assert.assertTrue(x.length() > 0);
            TestUtils.assertEquals(x, y);
        }
    }

    @Test
    public void testNoConfig() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, "/nfslog2.conf");

        try (LogFactory factory = new LogFactory()) {
            factory.init(null);

            Log logger = factory.create("x");
            assertDisabled(logger.debug());
            assertEnabled(logger.info());
            assertEnabled(logger.error());
            assertEnabled(logger.critical());
            assertEnabled(logger.advisory());
        }
    }

    @Test
    public void testNonDefault() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, Files.getResourcePath(getClass().getResource("/test-log.conf")));

        try (LogFactory factory = new LogFactory()) {
            factory.init(null);

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
                final QueueConsumer<LogRecordUtf8Sink> consumer = w.getMyConsumer();
                w.setMyConsumer(slot -> {
                    xLatch.countDown();
                    consumer.consume(slot);
                });
                return w;
            }));

            factory.add(new LogWriterConfig(LogLevel.DEBUG | LogLevel.ERROR, (ring, seq, level) -> {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(y.getAbsolutePath());
                final QueueConsumer<LogRecordUtf8Sink> consumer = w.getMyConsumer();
                w.setMyConsumer(slot -> {
                    yLatch.countDown();
                    consumer.consume(slot);
                });
                return w;
            }));

            factory.bind();
            factory.startThread();

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
    public void testRollingFileWriterByDay() {
        testRollOnDate("mylog-${date:yyyy-MM-dd}.log", 24 * 60000, "day", "mylog-2015-05");
    }

    @Test
    public void testRollingFileWriterByHour() {
        testRollOnDate("mylog-${date:yyyy-MM-dd-hh}.log", 100000, "hour", "mylog-2015-05-03");
    }

    @Test
    public void testRollingFileWriterByMinute() {
        testRollOnDate("mylog-${date:yyyy-MM-dd-hh-mm}.log", 1000, "minute", "mylog-2015-05-03");
    }

    @Test
    public void testRollingFileWriterByMonth() {
        testRollOnDate("mylog-${date:yyyy-MM}.log", 30 * 24 * 60000, "month", "mylog-2015");
    }

    @Test
    public void testRollingFileWriterBySize() {
        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String logFile = base + "mylog-${date:yyyy-MM-dd}.log";
        String expectedLogFile = base + "mylog-2015-05-03.log";

        final MicrosecondClock clock = new TestMicrosecondClock(MicrosFormatUtils.parseTimestamp("2015-05-03T10:35:00.000Z"), 1, MicrosTimestampDriver.floor("2019-12-31"));

        try (Path path = new Path()) {
            // create rogue file that would be in a way of logger rolling existing files
            path.of(base);
            Assert.assertTrue(Files.touch(path.concat("mylog-2015-05-03.log.2").$()));
        }

        RingQueue<LogRecordUtf8Sink> queue = new RingQueue<>(
                LogRecordUtf8Sink::new,
                1024,
                1024,
                MemoryTag.NATIVE_DEFAULT
        );

        SPSequence pubSeq = new SPSequence(queue.getCycle());
        SCSequence subSeq = new SCSequence();
        pubSeq.then(subSeq).then(pubSeq);

        try (final LogRollingFileWriter writer = new LogRollingFileWriter(
                TestFilesFacadeImpl.INSTANCE,
                clock,
                queue,
                subSeq,
                LogLevel.INFO
        )) {

            writer.setLocation(logFile);
            writer.setRollSize("1m");
            writer.setBufferSize("64k");
            writer.bindProperties(LogFactory.getInstance());

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
                    LogRecordUtf8Sink sink = queue.get(cursor++);
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
    public void testRollingFileWriterByYear() {
        testRollOnDate("mylog-${date:yyyy-MM}.log", 12 * 30 * 24 * 60000L, "year", "mylog-201");
    }

    @Test
    public void testRollingFileWriterDateParse() {
        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String logFile = base + "mylog-${date:yyyy-MM-dd}.log";
        String expectedLogFile = base + "mylog-2015-05-03.log";
        try (LogFactory factory = new LogFactory()) {
            final MicrosecondClock clock = new TestMicrosecondClock(MicrosFormatUtils.parseTimestamp("2015-05-03T11:35:00.000Z"), 1, MicrosTimestampDriver.floor("2015-05-04"));

            factory.add(new LogWriterConfig(LogLevel.INFO, (ring, seq, level) -> {
                LogRollingFileWriter w = new LogRollingFileWriter(TestFilesFacadeImpl.INSTANCE, clock, ring, seq, level);
                w.setLocation(logFile);
                return w;
            }));

            factory.bind();
            factory.startThread();

            Log logger = factory.create("x");
            for (int i = 0; i < 100000; i++) {
                logger.xinfo().$("test ").$(' ').$(i).$();
            }

            Os.sleep(100);
        }
        Assert.assertTrue(new File(expectedLogFile).length() > 0);
    }

    @Test
    public void testRollingFileWriterDateParsePushFilesMid() {
        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String expectedLogFile = base + "mylog-2015-05-03.log";
        try (LogFactory factory = new LogFactory()) {

            String logFile = base + "mylog-${date:yyyy-MM-dd}.log";

            final MicrosecondClock clock = new TestMicrosecondClock(MicrosFormatUtils.parseTimestamp("2015-05-03T10:35:00.000Z"), 1, MicrosTimestampDriver.floor("2015-05-04"));

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
                LogRollingFileWriter w = new LogRollingFileWriter(TestFilesFacadeImpl.INSTANCE, clock, ring, seq, level);
                w.setLocation(logFile);
                w.setSpinBeforeFlush("1000000");
                return w;
            }));

            factory.bind();
            factory.startThread();

            Log logger = factory.create("x");
            for (int i = 0; i < 100000; i++) {
                logger.xinfo().$("test ").$(' ').$(i).$();
            }

            Os.sleep(1000);
        }
        Assert.assertTrue(new File(expectedLogFile).length() > 0);
    }

    @Test
    public void testSetIncorrectBufferSizeProperty() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");
        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "w.file.class=io.questdb.log.LogRollingFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "questdb-rolling.log.${date:yyyyMMdd}\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.rollEvery=hour\n" +
                "w.file.bufferSize=avocado\n" +
                "w.file.rollSize=10m\n" +
                "w.file.lifeDuration=1d\n" +
                "w.file.sizeLimit=1g"
        );
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            factory.init(null);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Invalid value for bufferSize", e.getMessage());
        }
    }

    @Test
    public void testSetIncorrectLifeDurationProperty() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");
        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "w.file.class=io.questdb.log.LogRollingFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "questdb-rolling.log.${date:yyyyMMdd}\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.rollEvery=hour\n" +
                "w.file.bufferSize=100m\n" +
                "w.file.rollSize=10m\n" +
                "w.file.lifeDuration=avocado\n" +
                "w.file.sizeLimit=1g"
        );
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            factory.init(null);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Invalid value for lifeDuration", e.getMessage());
        }
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
            factory.init(null);
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
            factory.init(null);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Invalid value for recordLength", e.getMessage());
        }
    }

    @Test
    public void testSetIncorrectRollSizeProperty() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");
        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "w.file.class=io.questdb.log.LogRollingFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "questdb-rolling.log.${date:yyyyMMdd}\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.rollEvery=hour\n" +
                "w.file.rollSize=avocado\n" +
                "w.file.lifeDuration=1d\n" +
                "w.file.sizeLimit=1g"
        );
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            factory.init(null);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Invalid value for rollSize", e.getMessage());
        }
    }

    @Test
    public void testSetIncorrectSizeLimitProperty() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");
        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "w.file.class=io.questdb.log.LogRollingFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "questdb-rolling.log.${date:yyyyMMdd}\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.rollEvery=hour\n" +
                "w.file.bufferSize=100m\n" +
                "w.file.rollSize=10m\n" +
                "w.file.lifeDuration=24h\n" +
                "w.file.sizeLimit=avocado"
        );
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            factory.init(null);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Invalid value for sizeLimit", e.getMessage());
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

        LogFactory.disableEnv();
        try {
            System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());

            try (LogFactory factory = new LogFactory()) {
                factory.init(null);

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
            LogFactory.enableEnv();
        }
    }

    @Test
    public void testSetSizeLimitPropertyGreaterThanRollSize() throws Exception {
        File conf = temp.newFile();
        File out = new File(temp.newFolder(), "testSetProperties.log");
        TestUtils.writeStringToFile(conf, "writers=file\n" +
                "w.file.class=io.questdb.log.LogRollingFileWriter\n" +
                "w.file.location=" + out.getAbsolutePath().replaceAll("\\\\", "/") + "questdb-rolling.log.${date:yyyyMMdd}\n" +
                "w.file.level=INFO,ERROR\n" +
                "w.file.rollEvery=hour\n" +
                "w.file.rollSize=10m\n" +
                "w.file.lifeDuration=24h\n" +
                "w.file.sizeLimit=1m"
        );
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, conf.getAbsolutePath());
        try (LogFactory factory = new LogFactory()) {
            factory.init(null);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("sizeLimit must be larger than rollSize", e.getMessage());
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
            factory.init(null);
            Assert.fail();
        } catch (LogError e) {
            Assert.assertEquals("Unknown property: w.file.avocado", e.getMessage());
        }
    }

    @Test
    public void testSilent() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, Files.getResourcePath(getClass().getResource("/test-log-silent.conf")));

        try (LogFactory factory = new LogFactory()) {
            factory.init(null);

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

    @Test
    public void testSpaceInRollEvery() {
        final String logFile = temp.getRoot().getAbsolutePath() + Files.SEPARATOR + "mylog-${date:yyyy-MM-dd}.log";

        final MicrosecondClock clock = new TestMicrosecondClock(
                MicrosFormatUtils.parseTimestamp("2015-05-03T10:35:00.000Z"),
                1,
                MicrosTimestampDriver.floor("2019-12-31")
        );

        final RingQueue<LogRecordUtf8Sink> queue = new RingQueue<>(
                LogRecordUtf8Sink::new,
                1024,
                1024,
                MemoryTag.NATIVE_DEFAULT
        );

        final SPSequence pubSeq = new SPSequence(queue.getCycle());
        final SCSequence subSeq = new SCSequence();
        pubSeq.then(subSeq).then(pubSeq);

        try (final LogRollingFileWriter writer = new LogRollingFileWriter(
                TestFilesFacadeImpl.INSTANCE,
                clock,
                queue,
                subSeq,
                LogLevel.INFO
        )) {
            writer.setLocation(logFile);
            writer.setRollEvery("day  ");
            writer.bindProperties(LogFactory.getInstance());

            Assert.assertNotEquals(Long.MAX_VALUE, writer.getRollDeadlineFunction().getDeadline());
            Assert.assertEquals(1430697600000000L, writer.getRollDeadlineFunction().getDeadline());
        }

        try (final LogRollingFileWriter writer = new LogRollingFileWriter(
                TestFilesFacadeImpl.INSTANCE,
                clock,
                queue,
                subSeq,
                LogLevel.INFO
        )) {
            writer.setLocation(logFile);
            writer.setRollEvery(" minute ");
            writer.bindProperties(LogFactory.getInstance());

            Assert.assertNotEquals(Long.MAX_VALUE, writer.getRollDeadlineFunction().getDeadline());
            Assert.assertEquals(1430649360000000L, writer.getRollDeadlineFunction().getDeadline());
        }
    }

    @Test
    public void testUninitializedFactory() {
        System.setProperty(LogFactory.CONFIG_SYSTEM_PROPERTY, Files.getResourcePath(getClass().getResource("/test-log.conf")));

        try (LogFactory factory = new LogFactory()) {
            // First we get a no-op logger.
            Log logger = factory.create("com.questdb.x.y");
            assertDisabled(logger.debug());
            assertDisabled(logger.info());
            assertDisabled(logger.error());
            assertDisabled(logger.critical());
            assertDisabled(logger.advisory());
            assertDisabled(logger.xdebug());
            assertDisabled(logger.xinfo());
            assertDisabled(logger.xerror());
            assertDisabled(logger.xcritical());
            assertDisabled(logger.xadvisory());
            assertDisabled(logger.debugW());
            assertDisabled(logger.infoW());
            assertDisabled(logger.errorW());
            assertDisabled(logger.advisoryW());

            factory.init(null);

            // Once the factory is initialized, the logger is no longer no-op.
            assertEnabled(logger.debug());
            assertDisabled(logger.info());
            assertEnabled(logger.error());
            assertEnabled(logger.critical());
            assertEnabled(logger.advisory());
            assertEnabled(logger.xdebug());
            assertDisabled(logger.xinfo());
            assertEnabled(logger.xerror());
            assertEnabled(logger.xcritical());
            assertEnabled(logger.xadvisory());
            assertEnabled(logger.debugW());
            assertDisabled(logger.infoW());
            assertEnabled(logger.errorW());
            assertEnabled(logger.advisoryW());
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

    private static void assertDisabled(LogRecord r) {
        Assert.assertFalse(r.isEnabled());
        r.$();
    }

    private static void assertEnabled(LogRecord r) {
        Assert.assertTrue(r.isEnabled());
        r.$();
    }

    private static Log getLogger() {
        try {
            final Field field = QueryProgress.class.getDeclaredField("LOG");
            field.setAccessible(true);
            return (Log) field.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Could not set logger", e);
        }
    }

    private void assertFileLength(String file) {
        long len = new File(file).length();
        Assert.assertTrue("oops: " + len, len > 0L && len < 1073741824L);
    }

    private void testAutoDelete(String sizeLimit, String lifeDuration, String rollSize) throws Exception {
        final int extraFiles = 2;
        String fileTemplate = "mylog-${date:yyyy-MM-dd}.log";
        String extraFilePrefix = "mylog-test";
        long speed = Micros.HOUR_MICROS;

        final MicrosecondClock clock = new TestMicrosecondClock(
                MicrosFormatUtils.parseTimestamp("2015-05-03T10:35:00.000Z"),
                speed,
                MicrosTimestampDriver.floor("2019-12-31")
        );

        long nSizeLimit = sizeLimit != null ? Numbers.parseLongSize(sizeLimit) : 0;
        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String logFile = base + fileTemplate;
        AtomicReference<LogRollingFileWriter> writerRef = new AtomicReference<>();
        try (LogFactory factory = new LogFactory()) {
            LogWriterConfig config = new LogWriterConfig(LogLevel.INFO, (ring, seq, level) -> {
                LogRollingFileWriter w = new LogRollingFileWriter(FilesFacadeImpl.INSTANCE, clock, ring, seq, level);
                w.setLocation(logFile);
                w.setSpinBeforeFlush("10");
                w.setRollEvery("day");
                w.setRollSize(rollSize);
                if (sizeLimit != null) {
                    w.setSizeLimit(sizeLimit);
                }
                if (lifeDuration != null) {
                    w.setLifeDuration(lifeDuration);
                }
                writerRef.set(w);
                return w;
            });

            factory.add(config);
            factory.bind();
            factory.startThread();

            if (sizeLimit != null) {
                // Create files to be deleted based on size.
                try (Path path = new Path()) {
                    for (int i = 0; i < extraFiles; i++) {
                        path.of(base + extraFilePrefix).put(i).put(".log").$();
                        long fd = Files.openRW(path.$());
                        try {
                            Files.allocate(fd, nSizeLimit + 1);
                        } finally {
                            Files.close(fd);
                        }
                        Files.setLastModified(path.$(), clock.getTicks() / 1000 - (i + 1) * 24 * Micros.HOUR_MICROS / 1000);
                    }
                }
            }

            if (lifeDuration != null) {
                // Create files to be deleted based on modification date.
                try (Path path = new Path()) {
                    for (int i = 0; i < extraFiles; i++) {
                        path.of(base + extraFilePrefix).put(i).put(".log").$();
                        Files.touch(path.$());
                        Files.setLastModified(path.$(), clock.getTicks() / 1000 - (i + 1) * Numbers.parseLongDurationMicros(lifeDuration) / 1000);
                    }
                }
            }

            Log logger = factory.create("x");
            int lines = (int) Math.max(100000, (double) nSizeLimit / (5 + 1 + 3) * 2);
            for (int i = 0; i < lines; i++) {
                logger.xinfo().$("test ").$(' ').$(i).$();
            }
            logger.infoW().$("!").$();

            // Wait until we roll log files at least once.
            TestUtils.assertEventually(() -> {
                logger.infoW().$("!").$();
                Assert.assertTrue(writerRef.get().getRolledCount() > 0);
            }, 10);

            factory.close(true);
        }

        int fileCount = 0;
        boolean endFound = false;
        try (Path path = new Path()) {
            StringSink fileNameSink = new StringSink();
            path.of(base).$();
            int len = path.size();
            long pFind = Files.findFirst(path.$());
            try {
                Assert.assertNotEquals(0, pFind);
                do {
                    fileNameSink.clear();
                    Utf8s.utf8ToUtf16Z(Files.findName(pFind), fileNameSink);
                    if (Files.isDots(fileNameSink)) {
                        continue;
                    }
                    // All extra files should be deleted.
                    Assert.assertFalse(Chars.contains(fileNameSink, extraFilePrefix));
                    fileCount++;

                    path.trimTo(len).concat(fileNameSink);
                    long fileSize = Files.length(path.$());

                    long fd = Files.openRO(path.$());
                    char b = (char) Files.readNonNegativeByte(fd, fileSize - 3);
                    Files.close(fd);
                    endFound |= b == '!';
                } while (Files.findNext(pFind) > 0);
            } finally {
                Files.findClose(pFind);
            }
        }

        Assert.assertTrue(fileCount > 0);
        Assert.assertTrue(endFound);
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

            factory.init(temp.getRoot().getPath());

            File logFile = Paths.get(temp.getRoot().getPath(), "log\\test.log").toFile();
            Assert.assertEquals(logFile.getAbsolutePath(), isCreated, logFile.exists());
        }
    }

    private void testRollOnDate(
            String fileTemplate,
            long speed,
            String rollEvery,
            String mustContain
    ) throws NumericException {
        final MicrosecondClock clock = new TestMicrosecondClock(
                MicrosFormatUtils.parseTimestamp("2015-05-03T10:35:00.000Z"),
                speed,
                MicrosTimestampDriver.floor("2019-12-31")
        );

        final long expectedFileCount = Files.getOpenFileCount();
        long expectedMemUsage = Unsafe.getMemUsed();

        String base = temp.getRoot().getAbsolutePath() + Files.SEPARATOR;
        String logFile = base + fileTemplate;
        try (LogFactory factory = new LogFactory()) {
            factory.add(new LogWriterConfig(LogLevel.INFO, (ring, seq, level) -> {
                LogRollingFileWriter w = new LogRollingFileWriter(TestFilesFacadeImpl.INSTANCE, clock, ring, seq, level);
                w.setLocation(logFile);
                // 1Mb log file limit, we will create 4 of them
                w.setBufferSize("4k");
                w.setRollEvery(rollEvery);
                return w;
            }));

            factory.bind();
            factory.startThread();

            Log logger = factory.create("x");
            for (int i = 0; i < 10000; i++) {
                logger.xinfo().$("test ").$(' ').$(i).$();
            }
        }

        int fileCount = 0;
        try (Path path = new Path()) {
            StringSink fileNameSink = new StringSink();
            path.of(base).$();
            long pFind = Files.findFirst(path.$());
            try {
                Assert.assertNotEquals(0, pFind);
                do {
                    fileNameSink.clear();
                    Utf8s.utf8ToUtf16Z(Files.findName(pFind), fileNameSink);
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

        // this is a very weak assertion, but we have to live with it
        // logger runs asynchronously, it doesn't offer any synchronisation
        // support right now, which leaves tests at a mercy of the hardware/OS/other things
        // consuming CPU and potentially starving logger of execution time
        // when this happens there is no guarantees on how many files it will create
        Assert.assertTrue(fileCount > 0);
        Assert.assertEquals(expectedFileCount, Files.getOpenFileCount());
        Assert.assertEquals(expectedMemUsage, Unsafe.getMemUsed());
    }

    private static class TestMicrosecondClock implements MicrosecondClock {
        private final long limit;
        private final long speed;
        private final long start;
        private long k;

        public TestMicrosecondClock(long start, long speed, long limit) {
            this.start = start;
            this.speed = speed;
            this.limit = limit - 1;
            this.k = 0;
        }

        @Override
        public long getTicks() {
            return Math.min(start + (k++) * speed, limit);
        }
    }
}
