/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2016 Appsicle
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

package com.questdb;

import com.questdb.factory.JournalFactory;
import com.questdb.factory.JournalFactoryPool;
import com.questdb.log.*;
import com.questdb.misc.Os;
import com.questdb.mp.RingQueue;
import com.questdb.mp.Sequence;
import com.questdb.net.http.HttpServer;
import com.questdb.net.http.HttpServerConfiguration;
import com.questdb.net.http.MimeTypes;
import com.questdb.net.http.SimpleUrlMatcher;
import com.questdb.net.http.handlers.ExistenceCheckHandler;
import com.questdb.net.http.handlers.ImportHandler;
import com.questdb.net.http.handlers.QueryHandler;
import com.questdb.net.http.handlers.StaticContentHandler;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

class BootstrapMain {

    @SuppressFBWarnings("PATH_TRAVERSAL_IN")
    public static void main(String[] args) throws Exception {
        System.out.printf("QDB HTTP Server 3.0\nCopyright (C) Appsicle 2014-2016, all rights reserved.\n\n");
        if (args.length < 1) {
            System.out.println("Root directory name expected");
            return;
        }

        if (Os.type == Os._32Bit) {
            System.out.println("QDB requires 64-bit JVM");
            return;
        }

        String dir = args[0];
        String flag = args.length > 1 ? args[1] : null;

        extractSite(dir, "-f".equals(flag));
        File conf = new File(dir, "conf/questdb.conf");

        if (!conf.exists()) {
            System.out.println("Configuration file does not exist: " + conf);
            return;
        }

        final HttpServerConfiguration configuration = new HttpServerConfiguration(conf);
        configureLoggers(configuration);

        final SimpleUrlMatcher matcher = new SimpleUrlMatcher();
        JournalFactory factory = new JournalFactory(configuration.getDbPath().getAbsolutePath());
        matcher.put("/imp", new ImportHandler(factory));
        matcher.put("/js", new QueryHandler(new JournalFactoryPool(factory.getConfiguration(), configuration.getJournalPoolSize())));
        matcher.put("/chk", new ExistenceCheckHandler(factory));
        matcher.setDefaultHandler(new StaticContentHandler(configuration.getHttpPublic(), new MimeTypes(configuration.getMimeTypes())));

        HttpServer server = new HttpServer(configuration, matcher);
        server.start(LogFactory.INSTANCE.getJobs());

        StringBuilder welcome = new StringBuilder();
        welcome.append("Listening on ").append(configuration.getHttpIP()).append(':').append(configuration.getHttpPort());
        if (configuration.getSslConfig().isSecure()) {
            welcome.append(" [HTTPS]");
        } else {
            welcome.append(" [HTTP plain]");
        }

        System.out.println(welcome);
    }

    private static void configureLoggers(final HttpServerConfiguration configuration) {
        LogFactory.INSTANCE.add(new LogWriterConfig("access", LogLevel.LOG_LEVEL_ALL, new LogWriterFactory() {
            @Override
            public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                LogFileWriter w = new LogFileWriter(ring, seq, level);
                w.setLocation(configuration.getAccessLog().getAbsolutePath());
                return w;
            }
        }));

        final int level = System.getProperty(LogFactory.DEBUG_TRIGGER) != null ? LogLevel.LOG_LEVEL_ALL : LogLevel.LOG_LEVEL_ERROR | LogLevel.LOG_LEVEL_INFO;
        LogFactory.INSTANCE.add(new LogWriterConfig(level,
                new LogWriterFactory() {
                    @Override
                    public LogWriter createLogWriter(RingQueue<LogRecordSink> ring, Sequence seq, int level) {
                        LogFileWriter w = new LogFileWriter(ring, seq, level);
                        w.setLocation(configuration.getErrorLog().getAbsolutePath());
                        return w;
                    }
                }));

        LogFactory.INSTANCE.bind();
    }

    private static void extractSite(String dir, boolean force) throws URISyntaxException, IOException {
        URL url = HttpServer.class.getResource("/site/");
        final Path source = Paths.get(url.toURI());
        final Path target = Paths.get(dir);
        final EnumSet<FileVisitOption> walkOptions = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final CopyOption[] copyOptions = new CopyOption[]{COPY_ATTRIBUTES, REPLACE_EXISTING};

        if (force) {
            File pub = new File(dir, "public");
            if (pub.exists()) {
                com.questdb.misc.Files.delete(pub);
            }
        }

        Files.walkFileTree(source, walkOptions, Integer.MAX_VALUE, new FileVisitor<Path>() {

            private boolean skip = true;

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (skip) {
                    skip = false;
                } else {
                    Path newDirectory = target.resolve(source.relativize(dir));
                    try {
                        Files.copy(dir, newDirectory, copyOptions);
                    } catch (FileAlreadyExistsException ignore) {
                    } catch (IOException x) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.copy(file, target.resolve(source.relativize(file)), copyOptions);
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
        });
    }

}
