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

import com.nfsdb.collections.*;
import com.nfsdb.concurrent.*;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.misc.*;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class LogFactory implements Closeable {

    public static final LogFactory INSTANCE = new LogFactory();

    public static final int DEFAULT_QUEUE_DEPTH = 1024;
    public static final int DEFAULT_MSG_SIZE = 4 * 1024;
    private static final String DEFAULT_CONFIG = "/nfslog.conf";
    private static final String EMPTY_STR = "";
    private static final CharSequenceHashSet reserved = new CharSequenceHashSet();
    private static final LengthDescendingComparator LDC = new LengthDescendingComparator();
    private final CharSequenceObjHashMap<ScopeConfiguration> scopeConfigMap = new CharSequenceObjHashMap<>();
    private final ObjList<ScopeConfiguration> scopeConfigs = new ObjList<>();
    private final ObjHashSet<LogWriter> jobs = new ObjHashSet<>();
    private final CountDownLatch workerHaltLatch = new CountDownLatch(1);
    private Worker worker = null;
    private boolean configured = false;
    private int queueDepth = DEFAULT_QUEUE_DEPTH;
    private int recordLength = DEFAULT_MSG_SIZE;

    public static void configureFromSystemProperties(LogFactory factory) {
        String conf = System.getProperty("nfslog");
        if (conf == null) {
            conf = DEFAULT_CONFIG;
        }
        try (InputStream is = LogFactory.class.getResourceAsStream(conf)) {
            if (is != null) {
                Properties properties = new Properties();
                properties.load(is);
                setup(factory, properties);
            } else {
                File f = new File(conf);
                if (f.canRead()) {
                    try (FileInputStream fis = new FileInputStream(f)) {
                        Properties properties = new Properties();
                        properties.load(fis);
                        setup(factory, properties);
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

    public static Log getLogger(CharSequence key) {
        if (!INSTANCE.configured) {
            configureFromSystemProperties(INSTANCE);
        }
        return INSTANCE.create(key);
    }

    public static void setup(LogFactory factory, Properties properties) {

        String writers = properties.getProperty("writers");

        if (writers == null) {
            factory.configured = true;
            return;
        }

        String s;

        s = properties.getProperty("queueDepth");
        if (s != null && s.length() > 0) {
            try {
                factory.setQueueDepth(Numbers.parseInt(s));
            } catch (NumericException e) {
                throw new LogError("Invalid value for queueDepth");
            }
        }

        s = properties.getProperty("recordLength");
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

    public void add(final LogWriterConfig config) {
        ScopeConfiguration scopeConf = scopeConfigMap.get(config.getScope());
        if (scopeConf == null) {
            scopeConfigMap.put(config.getScope(), scopeConf = new ScopeConfiguration(3));
            scopeConfigs.add(scopeConf);
        }
        scopeConf.add(config);
    }

    public void bind() {
        if (configured) {
            return;
        }

        for (int i = 0, n = scopeConfigs.size(); i < n; i++) {
            ScopeConfiguration conf = scopeConfigs.get(i);
            conf.bind(jobs, queueDepth, recordLength);

        }

        scopeConfigMap.sortKeys(LDC);

        for (int i = 0, n = jobs.size(); i < n; i++) {
            LogWriter w = jobs.get(i);
            w.bindProperties();
        }

        configured = true;
    }

    @Override
    public void close() {
        for (int i = 0, n = jobs.size(); i < n; i++) {
            Misc.free(jobs.get(i));
        }
    }

    public Log create(CharSequence key) {
        if (!configured) {
            throw new LogError("Not configured");
        }

        ScopeConfiguration scopeConfiguration = find(key);
        if (scopeConfiguration == null) {
            return new Log(null, null, null, null, null, null);
        }
        Holder inf = scopeConfiguration.getHolder(Numbers.msb(LogLevel.LOG_LEVEL_INFO));
        Holder dbg = scopeConfiguration.getHolder(Numbers.msb(LogLevel.LOG_LEVEL_DEBUG));
        Holder err = scopeConfiguration.getHolder(Numbers.msb(LogLevel.LOG_LEVEL_ERROR));
        return new Log(
                dbg == null ? null : dbg.ring,
                dbg == null ? null : dbg.lSeq,
                inf == null ? null : inf.ring,
                inf == null ? null : inf.lSeq,
                err == null ? null : err.ring,
                err == null ? null : err.lSeq
        );
    }

    public ObjHashSet<LogWriter> getJobs() {
        return jobs;
    }

    public int getQueueDepth() {
        return queueDepth;
    }

    public void setQueueDepth(int queueDepth) {
        this.queueDepth = queueDepth;
    }

    public int getRecordLength() {
        return recordLength;
    }

    public void setRecordLength(int recordLength) {
        this.recordLength = recordLength;
    }

    public void haltThread() {
        if (worker != null) {
            worker.halt();
            try {
                workerHaltLatch.await();
            } catch (InterruptedException ignore) {
            }
            worker = null;
        }
    }

    public void startThread() {
        this.worker = new Worker(jobs, workerHaltLatch);
        worker.setDaemon(true);
        worker.setName("nfsdb-log-writer");
        worker.start();
    }

    private static LogWriterConfig createWriter(final Properties properties, String w) {
        final String writer = "w." + w + '.';
        final String clazz = properties.getProperty(writer + "class");
        final String levelStr = properties.getProperty(writer + "level");
        final String scope = properties.getProperty(writer + "scope");

        if (clazz == null) {
            return null;
        }

        final Class<?> cl;
        final Constructor constructor;
        try {
            cl = Class.forName(clazz);
            constructor = cl.getDeclaredConstructor(RingQueue.class, Sequence.class, int.class);
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
                        level |= LogLevel.LOG_LEVEL_DEBUG;
                        break;
                    case "INFO":
                        level |= LogLevel.LOG_LEVEL_INFO;
                        break;
                    case "ERROR":
                        level |= LogLevel.LOG_LEVEL_ERROR;
                        break;
                    default:
                        throw new LogError("Unknown level: " + s);
                }
            }
        }

        return new LogWriterConfig(scope == null ? EMPTY_STR : scope, level, new LogWriterFactory() {
            @Override
            public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                try {
                    LogWriter w = (LogWriter) constructor.newInstance(ring, seq, level);

                    for (String n : properties.stringPropertyNames()) {
                        if (n.startsWith(writer)) {
                            String p = n.substring(writer.length());

                            if (reserved.contains(p)) {
                                continue;
                            }

                            try {
                                Field f = cl.getDeclaredField(p);
                                if (f != null && f.getType() == String.class) {
                                    Unsafe.getUnsafe().putObject(w, Unsafe.getUnsafe().objectFieldOffset(f), properties.getProperty(n));
                                }
                            } catch (Exception e) {
                                throw new LogError("Unknown property: " + n, e);
                            }
                        }
                    }
                    return w;
                } catch (Exception e) {
                    throw new LogError("Error creating log writer", e);
                }
            }
        });
    }

    private void configureDefaultWriter() {
        add(new LogWriterConfig(LogLevel.LOG_LEVEL_INFO | LogLevel.LOG_LEVEL_ERROR, new LogWriterFactory() {
            @Override
            public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                return new LogConsoleWriter(ring, seq, level);
            }
        }));
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

    private static class ScopeConfiguration {
        private final int channels[];
        private final ObjList<LogWriterConfig> writerConfigs = new ObjList<>();
        private final IntObjHashMap<Holder> holderMap = new IntObjHashMap<>();
        private int ci = 0;

        public ScopeConfiguration(int levels) {
            this.channels = new int[levels];
        }

        public void bind(ObjHashSet<LogWriter> jobs, int queueDepth, int recordLength) {
            ObjList<Holder> holderList = new ObjList<>();

            // create queues for processed channels
            for (int i = 0, n = channels.length; i < n; i++) {
                int index = Unsafe.arrayGet(channels, i);
                if (index > 0) {
                    Holder h = holderMap.get(index);
                    if (h == null) {
                        holderMap.put(index, h = new Holder(queueDepth, recordLength));
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
                        h.fanOut = new FanOut(h.wSeq, h.wSeq = new SCSequence());
                    } else {
                        h.fanOut.add(h.wSeq = new SCSequence());
                    }
                } else {
                    // we are here first!
                    h.wSeq = new SCSequence();
                }
                // now h.wSeq contains out writer's sequence
                jobs.add(c.getFactory().createLogWriter(h.ring, h.wSeq, c.getLevel()));
            }

            // and the last step is to link dependant sequences
            for (int i = 0, n = holderList.size(); i < n; i++) {
                Holder h = holderList.getQuick(i);
                if (h.fanOut != null) {
                    h.lSeq.followedBy(h.fanOut);
                    h.fanOut.followedBy(h.lSeq);
                } else {
                    h.lSeq.followedBy(h.wSeq);
                    h.wSeq.followedBy(h.lSeq);
                }
            }
        }

        private void add(LogWriterConfig conf) {
            int mask = conf.getLevel();
            int min = Integer.MAX_VALUE;
            int q = ++ci;

            for (int i = 0, n = channels.length; i < n; i++) {
                if (((mask >> i) & 1) == 1) {
                    int that = Unsafe.arrayGet(channels, i);
                    if (that == 0) {
                        Unsafe.arrayPut(channels, i, q);
                    }

                    if (that > 0 && that < min) {
                        min = that;
                    }
                }
            }

            if (mask > 1 && min < Integer.MAX_VALUE) {
                for (int i = 0, n = channels.length; i < n; i++) {
                    if (((mask >> i) & 1) == 1) {
                        Unsafe.arrayPut(channels, i, min);
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

    private static class Holder {
        private RingQueue<LogRecordSink> ring;
        private Sequence wSeq;
        private Sequence lSeq;
        private FanOut fanOut;

        public Holder(int queueDepth, final int recordLength) {
            this.ring = new RingQueue<>(new ObjectFactory<LogRecordSink>() {
                @Override
                public LogRecordSink newInstance() {
                    return new LogRecordSink(recordLength);
                }
            }, queueDepth);
            this.lSeq = new MPSequence(queueDepth);
        }
    }

    static {
        Os.init();
        reserved.add("scope");
        reserved.add("class");
        reserved.add("level");
    }
}
