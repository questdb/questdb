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

package io.questdb.log;

import io.questdb.Metrics;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TimestampDriver;
import io.questdb.mp.FanOut;
import io.questdb.mp.Job;
import io.questdb.mp.MPSequence;
import io.questdb.mp.RingQueue;
import io.questdb.mp.SCSequence;
import io.questdb.mp.Sequence;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolConfiguration;
import io.questdb.std.CharSequenceHashSet;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjHashSet;
import io.questdb.std.ObjList;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.Clock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.Sinkable;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class LogFactory implements Closeable {

    public static final String CONFIG_SYSTEM_PROPERTY = "out";
    public static final String DEBUG_TRIGGER = "ebug";
    public static final String DEBUG_TRIGGER_ENV = "QDB_DEBUG";
    // name of default logging configuration file (in jar and in $root/conf/ dir)
    public static final String DEFAULT_CONFIG_NAME = "log.conf";
    // placeholder that can be used in log.conf to point to $root/log/ dir
    public static final String LOG_DIR_VAR = "${log.dir}";
    private static final String DEFAULT_CONFIG = "/io/questdb/site/conf/" + DEFAULT_CONFIG_NAME;
    private static final int DEFAULT_LOG_LEVEL = LogLevel.INFO | LogLevel.ERROR | LogLevel.CRITICAL | LogLevel.ADVISORY;
    private static final int DEFAULT_MSG_SIZE = 4 * 1024;
    private static final int DEFAULT_QUEUE_DEPTH = 1024;
    private static final String EMPTY_STR = "";
    private static final LengthDescendingComparator LDC = new LengthDescendingComparator();
    private static final CharSequenceHashSet reserved = new CharSequenceHashSet();
    private static LogFactory INSTANCE;
    private static boolean envEnabled = true;
    private static boolean guaranteedLogging = false;
    private static String rootDir;
    private final Clock clock;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ObjList<DeferredLogger> deferredLoggers = new ObjList<>();
    private final ObjHashSet<LogWriter> jobs = new ObjHashSet<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final CharSequenceObjHashMap<ScopeConfiguration> scopeConfigMap = new CharSequenceObjHashMap<>();
    private final ObjList<ScopeConfiguration> scopeConfigs = new ObjList<>();
    private final StringSink sink = new StringSink();
    private final WorkerPool loggingWorkerPool;
    private boolean configured = false;
    private int queueDepth = DEFAULT_QUEUE_DEPTH;
    private int recordLength = DEFAULT_MSG_SIZE;

    public LogFactory() {
        this(MicrosecondClockImpl.INSTANCE);
    }

    private LogFactory(Clock clock) {
        this.clock = clock;
        loggingWorkerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public Metrics getMetrics() {
                return Metrics.DISABLED;
            }

            @Override
            public String getPoolName() {
                return "logging";
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        });
    }

    public static synchronized void closeInstance() {
        LogFactory logFactory = INSTANCE;
        if (logFactory != null) {
            logFactory.close(true);
            INSTANCE = null;
        }
    }

    public static void configureRootDir(String rootDir) {
        LogFactory.rootDir = rootDir;
    }

    public static void disableEnv() {
        envEnabled = false;
    }

    @TestOnly
    public static void disableGuaranteedLogging() {
        guaranteedLogging = false;
    }

    @TestOnly
    public static void disableGuaranteedLogging(Class<?>... classes) {
        setGuaranteedLogging(false, classes);
    }

    public static void enableEnv() {
        envEnabled = true;
    }

    @TestOnly
    public static void enableGuaranteedLogging() {
        guaranteedLogging = true;
    }

    @TestOnly
    public static void enableGuaranteedLogging(Class<?>... classes) {
        setGuaranteedLogging(true, classes);
    }

    public static synchronized LogFactory getInstance() {
        LogFactory logFactory = INSTANCE;
        if (logFactory == null) {
            logFactory = new LogFactory();
            // Some log writers created in the later init() call may do some logging,
            // so we store the instance before the factory was fully initialized.
            // Any logging calls done on a non-initialized log factory and its loggers
            // are no-op. Once the factory is fully configured, it replaces no-op
            // loggers with the end ones.
            INSTANCE = logFactory;
            logFactory.init(rootDir);
        }
        return logFactory;
    }

    public static Log getLog(Class<?> clazz) {
        return getLog(clazz.getName());
    }

    public static Log getLog(String key) {
        return getInstance().create(key);
    }

    public static synchronized void haltInstance() {
        LogFactory logFactory = INSTANCE;
        if (logFactory != null) {
            logFactory.haltThread();
        }
    }

    @SuppressWarnings({"EmptyMethod", "unused"})
    public static void init() {
    }

    public synchronized void add(final LogWriterConfig config) {
        assert !configured;
        final int index = scopeConfigMap.keyIndex(config.getScope());
        ScopeConfiguration scopeConf;
        if (index > -1) {
            scopeConfigMap.putAt(index, config.getScope(), scopeConf = new ScopeConfiguration(LogLevel.MAX));
            scopeConfigs.add(scopeConf);
        } else {
            scopeConf = scopeConfigMap.valueAtQuick(index);
        }
        scopeConf.add(config);
    }

    public synchronized void bind() {
        if (configured) {
            return;
        }

        configured = true;

        for (int i = 0, n = scopeConfigs.size(); i < n; i++) {
            ScopeConfiguration conf = scopeConfigs.get(i);
            conf.bind(jobs, queueDepth, recordLength);
        }

        scopeConfigMap.sortKeys(LDC);

        for (int i = 0, n = jobs.size(); i < n; i++) {
            LogWriter job = jobs.get(i);
            job.bindProperties(this);
            loggingWorkerPool.assign(job);
        }
    }

    @Override
    public void close() {
        close(false);
    }

    public void close(boolean flush) {
        if (closed.compareAndSet(false, true)) {
            haltThread();
            for (int i = 0, n = jobs.size(); i < n; i++) {
                LogWriter job = jobs.get(i);
                try {
                    if (job != null && flush) {
                        try {
                            // noinspection StatementWithEmptyBody
                            while (job.run(0, Job.TERMINATING_STATUS)) {
                                // Keep running the job until it returns false to log all the buffered messages
                            }
                        } catch (Exception th) {
                            // Exception means we cannot log anymore. Perhaps network is down or disk is full.
                            // Switch to the next job.
                        }
                    }
                } finally {
                    Misc.freeIfCloseable(job);
                }
            }
            for (int i = 0, n = scopeConfigs.size(); i < n; i++) {
                Misc.free(scopeConfigs.getQuick(i));
            }
        }
    }

    public Log create(Class<?> clazz) {
        return create(clazz.getName());
    }

    public synchronized Log create(String key) {
        return create(key, guaranteedLogging);
    }

    public synchronized Log create(String key, boolean guaranteedLogging) {
        if (!configured) {
            DeferredLogger log = new DeferredLogger(key);
            deferredLoggers.add(log);
            return log;
        }

        final ScopeConfiguration scopeConfiguration = find(key);
        if (scopeConfiguration == null) {
            return new Logger(
                    clock,
                    compressScope(key, sink),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
            );
        }
        final Holder dbg = scopeConfiguration.getHolder(Numbers.msb(LogLevel.DEBUG));
        final Holder inf = scopeConfiguration.getHolder(Numbers.msb(LogLevel.INFO));
        final Holder err = scopeConfiguration.getHolder(Numbers.msb(LogLevel.ERROR));
        final Holder cri = scopeConfiguration.getHolder(Numbers.msb(LogLevel.CRITICAL));
        final Holder adv = scopeConfiguration.getHolder(Numbers.msb(LogLevel.ADVISORY));
        if (!guaranteedLogging) {
            return createLogger(key, dbg, inf, err, cri, adv);
        }
        return createGuaranteedLogger(key, dbg, inf, err, cri, adv);
    }

    @TestOnly
    public void flushJobs() {
        pauseThread();
        for (int i = 0, n = jobs.size(); i < n; i++) {
            LogWriter job = jobs.get(i);
            if (job != null) {
                try {
                    job.drain(0);
                } catch (Exception th) {
                    // Exception means we cannot log anymore. Perhaps network is down or disk is full.
                    // Switch to the next job.
                }
            }
        }
        startThread();
    }

    @TestOnly
    public ObjHashSet<LogWriter> getJobs() {
        return jobs;
    }

    public int getQueueDepth() {
        return queueDepth;
    }

    public int getRecordLength() {
        return recordLength;
    }

    public synchronized void init(@Nullable String rootDir) {
        if (configured) {
            return;
        }

        String conf = System.getProperty(CONFIG_SYSTEM_PROPERTY);
        if (conf == null) {
            conf = DEFAULT_CONFIG;
        }

        boolean initialized = false;
        if (rootDir != null && DEFAULT_CONFIG.equals(conf)) {
            String logPath = Paths.get(rootDir, "conf", DEFAULT_CONFIG_NAME).toAbsolutePath().toString();
            File f = new File(logPath);
            if (f.isFile() && f.canRead()) {
                System.err.printf("Reading log configuration from %s%n", logPath);
                try (FileInputStream fis = new FileInputStream(logPath)) {
                    configure(fis, rootDir);
                    initialized = true;
                } catch (IOException e) {
                    throw new LogError("Cannot read " + logPath, e);
                }
            }
        }

        if (!initialized) {
            // in this order of initialization specifying -Dout might end up using internal jar resources ...
            try (InputStream is = LogFactory.class.getResourceAsStream(conf)) {
                if (is != null) {
                    configure(is, rootDir);
                    System.err.println("Log configuration loaded from default internal file.");
                } else {
                    File f = new File(conf);
                    if (f.canRead()) {
                        try (FileInputStream fis = new FileInputStream(f)) {
                            configure(fis, rootDir);
                            System.err.printf("Log configuration loaded from: %s%n", conf);
                        }
                    } else {
                        configureDefaultWriter();
                        System.err.println("Log configuration loaded using factory defaults.");
                    }
                }
            } catch (IOException e) {
                if (!DEFAULT_CONFIG.equals(conf)) {
                    throw new LogError("Cannot read " + conf, e);
                } else {
                    configureDefaultWriter();
                }
            }
        }

        // swap no-op loggers created by the configured log writers with the real ones
        for (int i = 0, n = deferredLoggers.size(); i < n; i++) {
            deferredLoggers.get(i).init(this);
        }
        deferredLoggers.clear();

        startThread();
    }

    public void startThread() {
        assert !closed.get();
        if (running.compareAndSet(false, true)) {
            for (int i = 0, n = jobs.size(); i < n; i++) {
                loggingWorkerPool.assign(jobs.get(i));
            }
            loggingWorkerPool.start();
        }
    }

    /**
     * Converts fully qualified class name into an abbreviated form:
     * com.questdb.mp.Sequence -> c.n.m.Sequence
     *
     * @param key     typically class name
     * @param builder used for producing the resulting form
     * @return abbreviated form of key
     */
    private static CharSequence compressScope(CharSequence key, StringSink builder) {
        builder.clear();
        char c = 0;
        boolean pick = true;
        int z = 0;
        for (int i = 0, n = key.length(); i < n; i++) {
            char a = key.charAt(i);
            if (a == '.') {
                if (!pick) {
                    builder.put(c).put('.');
                    pick = true;
                }
            } else if (pick) {
                c = a;
                z = i;
                pick = false;
            }
        }

        for (; z < key.length(); z++) {
            builder.put(key.charAt(z));
        }

        builder.put(' ');

        return builder.toString();
    }

    @SuppressWarnings("rawtypes")
    private static LogWriterConfig createWriter(final Properties properties, String writerName) {
        final String writer = "w." + writerName + '.';
        final String clazz = getProperty(properties, writer + "class");
        final String levelStr = getProperty(properties, writer + "level");
        final String scope = getProperty(properties, writer + "scope");

        if (clazz == null) {
            return null;
        }

        final Class<?> cl;
        final Constructor constructor;
        try {
            cl = Class.forName(clazz);
            constructor = cl.getDeclaredConstructor(RingQueue.class, SCSequence.class, int.class);
        } catch (ClassNotFoundException e) {
            throw new LogError("Class not found " + clazz, e);
        } catch (NoSuchMethodException e) {
            throw new LogError("Constructor(RingQueue, Sequence, int) expected: " + clazz, e);
        }

        int level = 0;
        if (levelStr != null) {
            for (String s : levelStr.split(",")) {
                switch (s.toUpperCase()) {
                    case "DEBUG":
                        level |= LogLevel.DEBUG;
                        break;
                    case "INFO":
                        level |= LogLevel.INFO;
                        break;
                    case "ERROR":
                        level |= LogLevel.ERROR;
                        break;
                    case "CRITICAL":
                        level |= LogLevel.CRITICAL;
                        break;
                    case "ADVISORY":
                        level |= LogLevel.ADVISORY;
                        break;
                    default:
                        throw new LogError("Unknown level: " + s);
                }
            }
        }

        if (isForcedDebug()) {
            level = level | LogLevel.DEBUG;
        }

        // enable all LOG levels above the minimum set one
        // ((-1 >>> (msb-1)) << msb) | level
        final int msb = Numbers.msb(level);
        level = (((-1 >>> (msb - 1)) << msb) | level) & LogLevel.MASK;

        return new LogWriterConfig(scope == null ? EMPTY_STR : scope, level, (ring, seq, level1) -> {
            try {
                LogWriter w1 = (LogWriter) constructor.newInstance(ring, seq, level1);

                for (String n : properties.stringPropertyNames()) {
                    if (n.startsWith(writer)) {
                        String p = n.substring(writer.length());
                        if (reserved.contains(p)) {
                            continue;
                        }

                        try {
                            Field f = cl.getDeclaredField(p);
                            if (f.getType() == String.class) {
                                String value = getProperty(properties, n);
                                Unsafe.getUnsafe().putObject(w1, Unsafe.getUnsafe().objectFieldOffset(f), value);
                            }
                        } catch (Exception e) {
                            throw new LogError("Unknown property: " + n, e);
                        }
                    }
                }
                return w1;
            } catch (Exception e) {
                throw new LogError("Error creating log writer", e);
            }
        });
    }

    private static String getProperty(final Properties properties, String key) {
        if (envEnabled) {
            final String envKey = "QDB_LOG_" + key.replace('.', '_').toUpperCase();
            final String envValue = System.getenv(envKey);
            if (envValue == null) {
                return properties.getProperty(key);
            }
            System.err.println("    Using env: " + envKey + "=" + envValue);
            return envValue;
        }
        return properties.getProperty(key);
    }

    private static boolean isForcedDebug() {
        return System.getProperty(DEBUG_TRIGGER) != null || System.getenv().containsKey(DEBUG_TRIGGER_ENV);
    }

    private static void setGuaranteedLogging(boolean guaranteedLogging, Class<?>... classes) {
        for (int i = 0, n = classes.length; i < n; i++) {
            final Class<?> clazz = classes[i];
            setLogger(clazz, getInstance().create(clazz.getName(), guaranteedLogging));
        }
    }

    private static void setLogger(Class<?> clazz, Log logger) {
        try {
            final Field field = clazz.getDeclaredField("LOG");
            field.setAccessible(true);
            field.set(null, logger);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Could not set logger", e);
        }
    }

    private void configure(InputStream fis, String rootDir) throws IOException {
        Properties properties = new Properties();
        properties.load(fis);

        // QDB_LOG_LOG_DIR env variable can be used to override log directory
        String logDir = getProperty(properties, "log.dir");
        if (logDir == null) {
            if (rootDir != null) {
                logDir = Paths.get(rootDir, "log").toAbsolutePath().toString();
            } else {
                logDir = ".";
            }
        }
        boolean usesLogDirVar = false;
        for (String n : properties.stringPropertyNames()) {
            String value = getProperty(properties, n);
            if (value.contains(LOG_DIR_VAR)) {
                usesLogDirVar = true;
                value = value.replace(LOG_DIR_VAR, logDir);
                properties.put(n, value);
            }
        }

        if (usesLogDirVar) {
            File logDirFile = new File(logDir);
            if (!logDirFile.exists() && logDirFile.mkdirs()) {
                System.err.printf("Created log directory: %s%n", logDir);
            }
        }

        configureFromProperties(properties);
    }

    private void configureDefaultWriter() {
        int level = DEFAULT_LOG_LEVEL;
        if (isForcedDebug()) {
            level = level | LogLevel.DEBUG;
        }
        add(new LogWriterConfig(level, LogConsoleWriter::new));
        bind();
    }

    private void configureFromProperties(Properties properties) {
        String writers = getProperty(properties, "writers");

        if (writers == null) {
            configured = true;
            return;
        }

        String s = getProperty(properties, "queueDepth");
        if (s != null && !s.isEmpty()) {
            try {
                setQueueDepth(Numbers.parseInt(s));
            } catch (NumericException e) {
                throw new LogError("Invalid value for queueDepth");
            }
        }

        s = getProperty(properties, "recordLength");
        if (s != null && !s.isEmpty()) {
            try {
                setRecordLength(Numbers.parseInt(s));
            } catch (NumericException e) {
                throw new LogError("Invalid value for recordLength");
            }
        }

        // ensure that file location is set, so the env var can be picked up later
        if (properties.getProperty("w.file.location") == null) {
            properties.put("w.file.location", "");
        }

        for (String w : writers.split(",")) {
            LogWriterConfig conf = createWriter(properties, w.trim());
            if (conf != null) {
                add(conf);
            }
        }

        bind();
    }

    @NotNull
    private GuaranteedLogger createGuaranteedLogger(String key, Holder dbg, Holder inf, Holder err, Holder cri, Holder adv) {
        return new GuaranteedLogger(
                clock,
                compressScope(key, sink),
                dbg == null ? null : dbg.ring,
                dbg == null ? null : dbg.lSeq,
                inf == null ? null : inf.ring,
                inf == null ? null : inf.lSeq,
                err == null ? null : err.ring,
                err == null ? null : err.lSeq,
                cri == null ? null : cri.ring,
                cri == null ? null : cri.lSeq,
                adv == null ? null : adv.ring,
                adv == null ? null : adv.lSeq
        );
    }

    @NotNull
    private Logger createLogger(String key, Holder dbg, Holder inf, Holder err, Holder cri, Holder adv) {
        return new Logger(
                clock,
                compressScope(key, sink),
                dbg == null ? null : dbg.ring,
                dbg == null ? null : dbg.lSeq,
                inf == null ? null : inf.ring,
                inf == null ? null : inf.lSeq,
                err == null ? null : err.ring,
                err == null ? null : err.lSeq,
                cri == null ? null : cri.ring,
                cri == null ? null : cri.lSeq,
                adv == null ? null : adv.ring,
                adv == null ? null : adv.lSeq
        );
    }

    private ScopeConfiguration find(CharSequence key) {
        ObjList<CharSequence> keys = scopeConfigMap.keys();
        CharSequence k = null;

        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence s = keys.getQuick(i);
            if (Chars.startsWith(key, s)) {
                k = s;
                break;
            }
        }

        if (k == null) {
            return null;
        }

        return scopeConfigMap.get(k);
    }

    private void haltThread() {
        if (running.compareAndSet(true, false)) {
            loggingWorkerPool.halt();
        }
    }

    @TestOnly
    private void pauseThread() {
        if (running.compareAndSet(true, false)) {
            loggingWorkerPool.pause();
        }
    }

    private void setQueueDepth(int queueDepth) {
        this.queueDepth = queueDepth;
    }

    private void setRecordLength(int recordLength) {
        this.recordLength = recordLength;
    }

    private static class DeferredLogger implements Log {

        private static final NoOpLogRecord noOpRecord = new NoOpLogRecord();

        private final String key;
        private Log delegate;

        public DeferredLogger(String key) {
            this.key = key;
        }

        @Override
        public LogRecord advisory() {
            if (delegate != null) {
                return delegate.advisory();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord advisoryW() {
            if (delegate != null) {
                return delegate.advisoryW();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord critical() {
            if (delegate != null) {
                return delegate.critical();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord debug() {
            if (delegate != null) {
                return delegate.debug();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord debugW() {
            if (delegate != null) {
                return delegate.debugW();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord error() {
            if (delegate != null) {
                return delegate.error();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord errorW() {
            if (delegate != null) {
                return delegate.errorW();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord info() {
            if (delegate != null) {
                return delegate.info();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord infoW() {
            if (delegate != null) {
                return delegate.infoW();
            }
            return noOpRecord;
        }

        public void init(LogFactory logFactory) {
            this.delegate = logFactory.create(key);
        }

        @Override
        public LogRecord xDebugW() {
            if (delegate != null) {
                return delegate.xDebugW();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord xInfoW() {
            if (delegate != null) {
                return delegate.xInfoW();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord xadvisory() {
            if (delegate != null) {
                return delegate.xadvisory();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord xcritical() {
            if (delegate != null) {
                return delegate.xcritical();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord xdebug() {
            if (delegate != null) {
                return delegate.xdebug();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord xerror() {
            if (delegate != null) {
                return delegate.xerror();
            }
            return noOpRecord;
        }

        @Override
        public LogRecord xinfo() {
            if (delegate != null) {
                return delegate.xinfo();
            }
            return noOpRecord;
        }
    }

    private static class Holder implements Closeable {
        private final Sequence lSeq;
        private final RingQueue<LogRecordUtf8Sink> ring;
        private FanOut fanOut;
        private SCSequence wSeq;

        public Holder(int queueDepth, final int recordLength) {
            this.ring = new RingQueue<>(
                    LogRecordUtf8Sink::new,
                    Numbers.ceilPow2(recordLength),
                    queueDepth,
                    MemoryTag.NATIVE_LOGGER
            );
            this.lSeq = new MPSequence(queueDepth);
        }

        @Override
        public void close() {
            Misc.free(ring);
        }
    }

    private static class LengthDescendingComparator implements Comparator<CharSequence>, Serializable {
        @Override
        public int compare(CharSequence o1, CharSequence o2) {
            int l1, l2;
            if ((l1 = o1.length()) < (l2 = o2.length())) {
                return 1;
            }

            if (l1 > l2) {
                return -11;
            }

            return 0;
        }
    }

    private static class NoOpLogRecord implements LogRecord {

        @Override
        public void $() {
        }

        @Override
        public LogRecord $(@Nullable CharSequence sequence) {
            return this;
        }

        @Override
        public LogRecord $(@Nullable Utf8Sequence sequence) {
            return this;
        }

        @Override
        public LogRecord $(@Nullable DirectUtf8Sequence sequence) {
            return this;
        }

        @Override
        public LogRecord $(int x) {
            return this;
        }

        @Override
        public LogRecord $(double x) {
            return this;
        }

        @Override
        public LogRecord $(long l) {
            return this;
        }

        @Override
        public LogRecord $(boolean x) {
            return this;
        }

        @Override
        public LogRecord $(char c) {
            return this;
        }

        @Override
        public LogRecord $(@Nullable Throwable e) {
            return this;
        }

        @Override
        public LogRecord $(@Nullable File x) {
            return this;
        }

        @Override
        public LogRecord $(@Nullable Object x) {
            return this;
        }

        @Override
        public LogRecord $(@Nullable Sinkable x) {
            return this;
        }

        @Override
        public LogRecord $256(long a, long b, long c, long d) {
            return this;
        }

        @Override
        public LogRecord $hex(long value) {
            return this;
        }

        @Override
        public LogRecord $hexPadded(long value) {
            return this;
        }

        @Override
        public LogRecord $ip(long ip) {
            return this;
        }

        @Override
        public LogRecord $safe(@NotNull CharSequence sequence, int lo, int hi) {
            return this;
        }

        @Override
        public LogRecord $safe(@Nullable DirectUtf8Sequence sequence) {
            return this;
        }

        @Override
        public LogRecord $safe(@Nullable Utf8Sequence sequence) {
            return this;
        }

        @Override
        public LogRecord $safe(long lo, long hi) {
            return this;
        }

        @Override
        public LogRecord $safe(@Nullable CharSequence sequence) {
            return this;
        }

        @Override
        public LogRecord $size(long memoryBytes) {
            return this;
        }

        @Override
        public LogRecord $substr(int from, @Nullable DirectUtf8Sequence sequence) {
            return this;
        }

        @Override
        public LogRecord $ts(long x) {
            return this;
        }

        @Override
        public LogRecord $ts(TimestampDriver driver, long x) {
            return this;
        }

        @Override
        public LogRecord $uuid(long lo, long hi) {
            return this;
        }

        @Override
        public boolean isEnabled() {
            return false;
        }

        @Override
        public LogRecord microTime(long x) {
            return this;
        }

        @Override
        public LogRecord put(@Nullable Utf8Sequence us) {
            return this;
        }

        @Override
        public LogRecord put(byte b) {
            return this;
        }

        @Override
        public LogRecord put(char c) {
            return this;
        }

        @Override
        public LogRecord putNonAscii(long lo, long hi) {
            return this;
        }

        @Override
        public LogRecord ts() {
            return this;
        }
    }

    private static class ScopeConfiguration implements Closeable {
        private final int[] channels;
        private final ObjList<Holder> holderList = new ObjList<>();
        private final IntObjHashMap<Holder> holderMap = new IntObjHashMap<>();
        private final ObjList<LogWriterConfig> writerConfigs = new ObjList<>();
        private int ci = 0;

        public ScopeConfiguration(int levels) {
            this.channels = new int[levels];
        }

        public void bind(ObjHashSet<LogWriter> jobs, int queueDepth, int recordLength) {
            // create queues for processed channels
            for (int index : channels) {
                if (index > 0) {
                    int keyIndex = holderMap.keyIndex(index);
                    if (keyIndex > -1) {
                        Holder h = new Holder(queueDepth, recordLength);
                        holderMap.putAt(keyIndex, index, h);
                        holderList.add(h);
                    }
                }
            }

            for (int i = 0, n = writerConfigs.size(); i < n; i++) {
                LogWriterConfig c = writerConfigs.getQuick(i);
                // the channels array has a guarantee that
                // all bits in level mask will point to the same queue,
                // so we just get most significant bit number
                // and dereference queue on its index
                if (c.getLevel() < 1) {
                    throw CairoException.nonCritical().put("logging level not set"); // when `QDB_LOG_W_FILE_LEVEL` is missing (or on another driver)
                }
                Holder h = holderMap.get(channels[Numbers.msb(c.getLevel())]);
                // check if this queue was used by another writer
                if (h.wSeq != null) {
                    // yes, it was
                    if (h.fanOut == null) {
                        h.fanOut = FanOut.to(h.wSeq).and(h.wSeq = new SCSequence());
                    } else {
                        h.fanOut.and(h.wSeq = new SCSequence());
                    }
                } else {
                    // we are here first!
                    h.wSeq = new SCSequence();
                }
                // now h.wSeq contains out writer's sequence
                jobs.add(c.getFactory().createLogWriter(h.ring, h.wSeq, c.getLevel()));
            }

            // and the last step is to link dependent sequences
            for (int i = 0, n = holderList.size(); i < n; i++) {
                Holder h = holderList.getQuick(i);
                if (h.fanOut != null) {
                    h.lSeq.then(h.fanOut).then(h.lSeq);
                } else {
                    h.lSeq.then(h.wSeq).then(h.lSeq);
                }
            }
        }

        @Override
        public void close() {
            for (int i = 0, n = holderList.size(); i < n; i++) {
                Misc.free(holderList.getQuick(i));
            }
        }

        /**
         * Aggregates channels into set of queues. Consumer interest is represented by
         * level, where consumer sets bits corresponding to channel indexes is it interested in.
         * <p>
         * Consumer 1 requires channels D & E. So its interest looks like {1,0,1}
         * Consumer 2 requires channel I, so its interest is {0,1,0}
         * <p>
         * This method combines these interests as follows:
         * <p>
         * channels = {1,2,1}
         * <p>
         * which means that there will be need to 2 queues (1 and 2) and that Consumer 1
         * will be using queue 1 and consumer 2 will be using queue 2.
         * <p>
         * More complex scenario where consumer interests overlap, for example:
         * <p>
         * consumer 1 {1,1,0}
         * consumer 2 {0,1,1}
         * <p>
         * these interests will be combined as follows:
         * <p>
         * channels = {1,1,1}
         * <p>
         * which means that both consumers will be sharing same queue and they will have to
         * filter applicable messages as they get them.
         * <p>
         * Algorithm iterates over set of bits in "level" twice. First pass is to establish
         * minimum number of channel[] element out of those entries where bit in level is set.
         * Additionally, this pass will set channel[] elements to current consumer index where
         * channel[] element is zero.
         * <p>
         * Second pass sets channel[] element to min value found on first pass.
         *
         * @param conf LogWriterConfig
         */
        private void add(LogWriterConfig conf) {
            int mask = conf.getLevel();
            int min = Integer.MAX_VALUE;
            int q = ++ci;

            for (int i = 0, n = channels.length; i < n; i++) {
                if (((mask >> i) & 1) == 1) {
                    int that = channels[i];
                    if (that == 0) {
                        channels[i] = q;
                    }

                    if (that > 0 && that < min) {
                        min = that;
                    }
                }
            }

            if (mask > 1 && min < Integer.MAX_VALUE) {
                for (int i = 0, n = channels.length; i < n; i++) {
                    if (((mask >> i) & 1) == 1) {
                        channels[i] = min;
                    }
                }
            }

            writerConfigs.add(conf);
        }

        private Holder getHolder(int index) {
            return holderMap.get(channels[index]);
        }
    }

    static {
        reserved.add("scope");
        reserved.add("class");
        reserved.add("level");
        Os.init();
    }
}
