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

package com.questdb;

import com.questdb.log.LogFactory;
import com.questdb.log.LogFileWriter;
import com.questdb.log.LogLevel;
import com.questdb.log.LogWriterConfig;
import com.questdb.mp.Job;
import com.questdb.net.http.HttpServer;
import com.questdb.net.http.SimpleUrlMatcher;
import com.questdb.net.http.handlers.*;
import com.questdb.parser.typeprobe.TypeProbeCollection;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Misc;
import com.questdb.std.ObjHashSet;
import com.questdb.std.Os;
import com.questdb.std.microtime.MicrosecondClockImpl;
import com.questdb.std.str.CharSink;
import com.questdb.std.time.DateFormatFactory;
import com.questdb.std.time.DateLocaleFactory;
import com.questdb.store.factory.Factory;
import sun.misc.Signal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

class BootstrapMain {

    public static void main(String[] args) throws Exception {
        System.err.printf("QuestDB HTTP Server %s%nCopyright (C) Appsicle 2014-2018, all rights reserved.%n%n", getVersion());
        if (args.length < 1) {
            System.err.println("Root directory name expected");
            return;
        }

        if (Os.type == Os._32Bit) {
            System.err.println("QuestDB requires 64-bit JVM");
            return;
        }

        final CharSequenceObjHashMap<String> optHash = hashArgs(args);

        // expected flags:
        // -d <root dir> = sets root directory
        // -f = forces copy of site to root directory even if site exists
        // -n = disables handling of HUP signal

        String dir = optHash.get("-d");
        extractSite(dir, optHash.get("-f") != null);
        File conf = new File(dir, "conf/questdb.conf");

        if (!conf.exists()) {
            System.err.println("Configuration file does not exist: " + conf);
            return;
        }

        BootstrapEnv env = new BootstrapEnv();
        // main configuration
        env.configuration = new ServerConfiguration(conf);
        configureLoggers(env.configuration);
        env.dateFormatFactory = new DateFormatFactory();
        env.dateLocaleFactory = DateLocaleFactory.INSTANCE;
        env.typeProbeCollection = new TypeProbeCollection(new File(dir, "conf/date.formats").getAbsolutePath(), env.dateFormatFactory, env.dateLocaleFactory);

        // reader/writer factory and cache
        env.factory = new Factory(
                env.configuration.getDbPath().getAbsolutePath(),
                env.configuration.getDbPoolIdleTimeout(),
                env.configuration.getDbReaderPoolSize(),
                env.configuration.getDbPoolIdleCheckInterval()
        );

        // URL matcher configuration
        env.matcher = new SimpleUrlMatcher();
        env.matcher.put("/imp", new ImportHandler(env));
        env.matcher.put("/exec", new QueryHandler(env));
        env.matcher.put("/exp", new CsvHandler(env));
        env.matcher.put("/chk", new ExistenceCheckHandler(env));
        env.matcher.setDefaultHandler(new StaticContentHandler(env));

        // server configuration
        // add all other jobs to server as it will be scheduling workers to do them
        final HttpServer server = new HttpServer(env);
        // monitoring setup
        final FactoryEventLogger factoryEventLogger = new FactoryEventLogger(env.factory, 10000000, 5000, MicrosecondClockImpl.INSTANCE);
        ObjHashSet<Job> jobs = server.getJobs();
        jobs.addAll(LogFactory.INSTANCE.getJobs());
        jobs.add(factoryEventLogger);
        env.factory.exportJobs(jobs);

        // welcome message
        CharSink welcome = Misc.getThreadLocalBuilder();
        if (!server.start()) {
            welcome.put("Could not bind socket ").put(env.configuration.getHttpIP()).put(':').put(env.configuration.getHttpPort());
            welcome.put(". Already running?");
            System.err.println(welcome);
            System.out.println(new Date() + " QuestDB failed to start");
        } else {
            welcome.put("Listening on ").put(env.configuration.getHttpIP()).put(':').put(env.configuration.getHttpPort());
            if (env.configuration.getSslConfig().isSecure()) {
                welcome.put(" [HTTPS]");
            } else {
                welcome.put(" [HTTP plain]");
            }

            System.err.println(welcome);
            System.out.println(new Date() + " QuestDB is running");

            if (Os.type != Os.WINDOWS && optHash.get("-n") == null) {
                // suppress HUP signal
                Signal.handle(new Signal("HUP"), signal -> {
                });
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println(new Date() + " QuestDB is shutting down");
                server.halt();
                factoryEventLogger.close();
                env.factory.close();
            }));
        }
    }

    private static String getVersion() throws IOException {
        Enumeration<URL> resources = BootstrapMain.class.getClassLoader()
                .getResources("META-INF/MANIFEST.MF");
        while (resources.hasMoreElements()) {
            try (InputStream is = resources.nextElement().openStream()) {
                Manifest manifest = new Manifest(is);
                Attributes attributes = manifest.getMainAttributes();
                if ("org.questdb".equals(attributes.getValue("Implementation-Vendor-Id"))) {
                    return manifest.getMainAttributes().getValue("Implementation-Version");
                }
            }
        }
        return "[DEVELOPMENT]";
    }

    private static CharSequenceObjHashMap<String> hashArgs(String[] args) {
        CharSequenceObjHashMap<String> optHash = new CharSequenceObjHashMap<>();
        String flag = null;
        for (int i = 0, n = args.length; i < n; i++) {
            String s = args[i];

            if (s.startsWith("-")) {
                if (flag != null) {
                    optHash.put(flag, "");
                }
                flag = s;
            } else {
                if (flag != null) {
                    optHash.put(flag, s);
                    flag = null;
                } else {
                    System.err.println("Unknown arg: " + s);
                    System.exit(55);
                }
            }
        }

        if (flag != null) {
            optHash.put(flag, "");
        }

        return optHash;
    }

    private static void configureLoggers(final ServerConfiguration configuration) {

        LogFactory.INSTANCE.add(new LogWriterConfig("access", LogLevel.LOG_LEVEL_ALL, (ring1, seq1, level1) -> {
            LogFileWriter w1 = new LogFileWriter(ring1, seq1, level1);
            w1.setLocation(configuration.getAccessLog().getAbsolutePath());
            return w1;
        }));


        LogFactory.INSTANCE.add(new LogWriterConfig(
                System.getProperty(LogFactory.DEBUG_TRIGGER) != null ? LogLevel.LOG_LEVEL_ALL : LogLevel.LOG_LEVEL_ERROR | LogLevel.LOG_LEVEL_INFO,
                (ring, seq, level) -> {
                    LogFileWriter w = new LogFileWriter(ring, seq, level);
                    w.setLocation(configuration.getErrorLog().getAbsolutePath());
                    return w;
                }));
        LogFactory.INSTANCE.bind();
    }

    private static void extractSite(String dir, boolean force) throws URISyntaxException, IOException {
        System.out.println("Preparing content...");
        URL url = HttpServer.class.getResource("/site/");
        String[] components = url.toURI().toString().split("!");
        FileSystem fs = null;
        final Path source;
        final int sourceLen;
        if (components.length > 1) {
            fs = FileSystems.newFileSystem(URI.create(components[0]), new HashMap<>());
            source = fs.getPath(components[1]);
            sourceLen = source.toAbsolutePath().toString().length();
        } else {
            source = Paths.get(url.toURI());
            sourceLen = source.toAbsolutePath().toString().length() + 1;
        }

        try {
            final Path target = Paths.get(dir);
            final EnumSet<FileVisitOption> walkOptions = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
            final CopyOption[] copyOptions = new CopyOption[]{COPY_ATTRIBUTES, REPLACE_EXISTING};

            if (force) {
                File pub = new File(dir, "public");
                if (pub.exists()) {
                    com.questdb.std.Files.delete(pub);
                }
            }

            Files.walkFileTree(source, walkOptions, Integer.MAX_VALUE, new FileVisitor<Path>() {

                private boolean skip = true;

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (skip) {
                        skip = false;
                    } else {
                        try {
                            Files.copy(dir, toDestination(dir), copyOptions);
                            System.out.println("Extracted " + dir);
                        } catch (FileAlreadyExistsException ignore) {
                        } catch (IOException x) {
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.copy(file, toDestination(file), copyOptions);
                    System.out.println("Extracted " + file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                    return FileVisitResult.CONTINUE;
                }

                private Path toDestination(final Path path) {
                    final Path tmp = path.toAbsolutePath();
                    return target.resolve(tmp.toString().substring(sourceLen));
                }
            });
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }
}
