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
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.StringSink;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.Properties;

public class LogFactory implements Closeable {

    public static final LogFactory INSTANCE = new LogFactory();
    public static final String DEBUG_TRIGGER = "ebug";
    public static final String DEBUG_TRIGGER_ENV = "QDB_DEBUG";
    public static final String CONFIG_SYSTEM_PROPERTY = "out";

    private static final int DEFAULT_QUEUE_DEPTH = 1024;
    private static final int DEFAULT_MSG_SIZE = 4 * 1024;
    private static final String DEFAULT_CONFIG = "/log-stdout.conf";
    private static final String EMPTY_STR = "";
    private static final CharSequenceHashSet reserved = new CharSequenceHashSet();
    private static final LengthDescendingComparator LDC = new LengthDescendingComparator();
    private final CharSequenceObjHashMap<ScopeConfiguration> scopeConfigMap = new CharSequenceObjHashMap<>();
    private final ObjList<ScopeConfiguration> scopeConfigs = new ObjList<>();
    private final ObjHashSet<LogWriter> jobs = new ObjHashSet<>();
    private final MicrosecondClock clock;
    private final StringSink sink = new StringSink();
    private WorkerPool workerPool;
    private boolean configured = false;
    private int queueDepth = DEFAULT_QUEUE_DEPTH;
    private int recordLength = DEFAULT_MSG_SIZE;
    static boolean envEnabled = true;

    public LogFactory() {
        this(MicrosecondClockImpl.INSTANCE);
    }

    public LogFactory(MicrosecondClock clock) {
        this.clock = clock;
    }

    public static void configureFromProperties(LogFactory factory, Properties properties, WorkerPool workerPool) {

        factory.workerPool = workerPool;
        String writers = getProperty(properties, "writers");

        if (writers == null) {
            factory.configured = true;
            return;
        }

        String s;

        s = getProperty(properties, "queueDepth");
        if (s != null && s.length() > 0) {
            try {
                factory.setQueueDepth(Numbers.parseInt(s));
            } catch (NumericException e) {
                throw new LogError("Invalid value for queueDepth");
            }
        }

        s = getProperty(properties, "recordLength");
        if (s != null && s.length() > 0) {
            try {
                factory.setRecordLength(Numbers.parseInt(s));
            } catch (NumericException e) {
                throw new LogError("Invalid value for recordLength");
            }
        }

        for (String w : writers.split(",")) {
            LogWriterConfig conf = createWriter(properties, w.trim());
            if (conf != null) {
                factory.add(conf);
            }
        }

        factory.bind();
    }

    public static void configureFromSystemProperties(LogFactory factory) {
        configureFromSystemProperties(factory, null);
    }

    public static void configureFromSystemProperties(LogFactory factory, WorkerPool workerPool) {
        String conf = System.getProperty(CONFIG_SYSTEM_PROPERTY);
        if (conf == null) {
            conf = DEFAULT_CONFIG;
        }
        try (InputStream is = LogFactory.class.getResourceAsStream(conf)) {
            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);
                configureFromProperties(factory, properties, workerPool);
            } else {
                File f = new File(conf);
                if (f.canRead()) {
                    try (FileInputStream fis = new FileInputStream(f)) {
                        Properties properties = new Properties();
                        properties.load(fis);
                        configureFromProperties(factory, properties, workerPool);
                    }
                } else {
                    factory.configureDefaultWriter();
                }
            }
        } catch (IOException e) {
            if (!DEFAULT_CONFIG.equals(conf)) {
                throw new LogError("Cannot read " + conf, e);
            } else {
                factory.configureDefaultWriter();
            }
        }
        factory.startThread();
    }

    public static void configureFromSystemProperties(WorkerPool workerPool) {
        configureFromSystemProperties(INSTANCE, workerPool);
    }

    @SuppressWarnings("rawtypes")
    public static Log getLog(Class clazz) {
        return getLog(clazz.getName());
    }

    public static Log getLog(CharSequence key) {
        if (!INSTANCE.configured) {
            configureFromSystemProperties(INSTANCE, null);
        }
        return INSTANCE.create(key);
    }

    public void add(final LogWriterConfig config) {
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

    public void assign(WorkerPool workerPool) {
        for (int i = 0, n = jobs.size(); i < n; i++) {
            workerPool.assign(jobs.get(i));
        }
        if (this.workerPool == null) {
            this.workerPool = workerPool;
        }
    }

    public void bind() {
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
            jobs.get(i).bindProperties();
        }

        if (workerPool != null) {
            assign(workerPool);
        }
    }

    @Override
    public void close() {
        haltThread();
        for (int i = 0, n = jobs.size(); i < n; i++) {
            Misc.free(jobs.get(i));
        }
        for (int i = 0, n = scopeConfigs.size(); i < n; i++) {
            Misc.free(scopeConfigs.getQuick(i));
        }
    }

    public Log create(CharSequence key) {
        if (!configured) {
            throw new LogError("Not configured");
        }

        ScopeConfiguration scopeConfiguration = find(key);
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
                    null
            );
        }
        final Holder inf = scopeConfiguration.getHolder(Numbers.msb(LogLevel.INFO));
        final Holder dbg = scopeConfiguration.getHolder(Numbers.msb(LogLevel.DEBUG));
        final Holder err = scopeConfiguration.getHolder(Numbers.msb(LogLevel.ERROR));
        final Holder adv = scopeConfiguration.getHolder(Numbers.msb(LogLevel.ADVISORY));
        return new Logger(
                clock,
                compressScope(key, sink),
                dbg == null ? null : dbg.ring,
                dbg == null ? null : dbg.lSeq,
                inf == null ? null : inf.ring,
                inf == null ? null : inf.lSeq,
                err == null ? null : err.ring,
                err == null ? null : err.lSeq,
                adv == null ? null : adv.ring,
                adv == null ? null : adv.lSeq
        );
    }

    public ObjHashSet<LogWriter> getJobs() {
        return jobs;
    }

    public int getQueueDepth() {
        return queueDepth;
    }

    private void setQueueDepth(int queueDepth) {
        this.queueDepth = queueDepth;
    }

    public int getRecordLength() {
        return recordLength;
    }

    private void setRecordLength(int recordLength) {
        this.recordLength = recordLength;
    }

    public void haltThread() {
        if (workerPool != null) {
            workerPool.halt();
            workerPool = null;
        }
    }

    public void startThread() {

        if (this.workerPool != null) {
            return;
        }

        this.workerPool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override
            public int[] getWorkerAffinity() {
                return new int[]{-1};
            }

            @Override
            public int getWorkerCount() {
                return 1;
            }

            @Override
            public boolean haltOnError() {
                return false;
            }

            @Override
            public boolean isDaemonPool() {
                return true;
            }
        });
        assign(workerPool);
        workerPool.start(null);
    }

    private static String getProperty(final Properties properties, String key) {
        if (envEnabled) {
            final String envValue = System.getenv("QDB_LOG_" + key.replace('.', '_').toUpperCase());
            if (envValue == null) {
                return properties.getProperty(key);
            }
            return envValue;
        }
        return properties.getProperty(key);
    }

    @SuppressWarnings("rawtypes")
    private static LogWriterConfig createWriter(final Properties properties, String w) {
        final String writer = "w." + w + '.';
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
                                Unsafe.getUnsafe().putObject(w1, Unsafe.getUnsafe().objectFieldOffset(f), getProperty(properties, n));
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

    private static boolean isForcedDebug() {
        return System.getProperty(DEBUG_TRIGGER) != null || System.getenv().containsKey(DEBUG_TRIGGER_ENV);
    }

    /**
     * Converts fully qualified class name into an abbreviated form:
     * com.questdb.mp.Sequence -> c.n.m.Sequence
     *
     * @param key typically class name
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

    private void configureDefaultWriter() {
        int level = LogLevel.INFO | LogLevel.ERROR | LogLevel.ADVISORY;
        if (isForcedDebug()) {
            level = level | LogLevel.DEBUG;
        }
        add(new LogWriterConfig(level, LogConsoleWriter::new));
        bind();
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

    private static class ScopeConfiguration implements Closeable {
        private final int[] channels;
        private final ObjList<LogWriterConfig> writerConfigs = new ObjList<>();
        private final IntObjHashMap<Holder> holderMap = new IntObjHashMap<>();
        private final ObjList<Holder> holderList = new ObjList<>();
        private int ci = 0;

        public ScopeConfiguration(int levels) {
            this.channels = new int[levels];
        }

        public void bind(ObjHashSet<LogWriter> jobs, int queueDepth, int recordLength) {
            // create queues for processed channels
            for (int i = 0, n = channels.length; i < n; i++) {
                int index = channels[i];
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
                // all bits in level mask will point to the same queue
                // so we just get most significant bit number
                // and dereference queue on its index
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
         * Additionally this pass will set channel[] elements to current consumer index where
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

    private static class Holder implements Closeable {
        private final RingQueue<LogRecordSink> ring;
        private final Sequence lSeq;
        private SCSequence wSeq;
        private FanOut fanOut;

        public Holder(int queueDepth, final int recordLength) {
            this.ring = new RingQueue<>(
                    LogRecordSink::new,
                    Numbers.ceilPow2(recordLength),
                    queueDepth,
                    MemoryTag.NATIVE_DEFAULT
            );
            this.lSeq = new MPSequence(queueDepth);
        }

        @Override
        public void close() {
            Misc.free(ring);
        }
    }

    static {
        reserved.add("scope");
        reserved.add("class");
        reserved.add("level");
        Os.init();
    }
}
