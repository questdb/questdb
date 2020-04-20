/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

package io.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.line.udp.AbstractLineProtoReceiver;
import io.questdb.cutlass.line.udp.LineProtoReceiver;
import io.questdb.cutlass.line.udp.LinuxMMLineProtoReceiver;
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.*;
import io.questdb.std.time.Dates;
import sun.misc.Signal;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class ServerMain {
    public ServerMain(String[] args) throws Exception {
        System.err.printf("QuestDB server %s%nCopyright (C) 2014-%d, all rights reserved.%n%n", getVersion(), Dates.getYear(System.currentTimeMillis()));
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

        final String rootDirectory = optHash.get("-d");
        extractSite(rootDirectory, optHash.get("-f") != null);
        final Properties properties = new Properties();
        final String configurationFileName = "/server.conf";
        final File configurationFile = new File(new File(rootDirectory, PropServerConfiguration.CONFIG_DIRECTORY), configurationFileName);

        try (InputStream is = new FileInputStream(configurationFile)) {
            properties.load(is);
        }

        final PropServerConfiguration configuration = new PropServerConfiguration(rootDirectory, properties);

        // create database directory
        try (io.questdb.std.str.Path path = new io.questdb.std.str.Path()) {
            path.of(configuration.getCairoConfiguration().getRoot());
            if (!Chars.endsWith(path, io.questdb.std.Files.SEPARATOR)) {
                // this would end trailing path separator
                path.concat("");
            }
            path.$();

            if (io.questdb.std.Files.mkdirs(path, configuration.getCairoConfiguration().getMkDirMode()) != 0) {
                System.err.println("Could not create database root directory: " + path.toString());
                System.exit(30);
            } else {
                System.out.println("Database root is '" + path + "'");
            }
        }
        switch (Os.type) {
            case Os.WINDOWS:
                System.out.println("OS: windows-amd64 " + Vect.getSupportedInstructionSetName());
                break;
            case Os.LINUX_AMD64:
                System.out.println("OS: linux-amd64" + Vect.getSupportedInstructionSetName());
                break;
            case Os.OSX:
                System.out.println("OS: apple-amd64" + Vect.getSupportedInstructionSetName());
                break;
            case Os.LINUX_ARM64:
                System.out.println("OS: linux-arm64" + Vect.getSupportedInstructionSetName());
                break;
            case Os.FREEBSD:
                System.out.println("OS: freebsd-amd64" + Vect.getSupportedInstructionSetName());
                break;
            default:
                System.err.println("Unsupported OS");
                break;
        }

        final WorkerPool workerPool = new WorkerPool(configuration.getWorkerPoolConfiguration());
        final MessageBus messageBus = new MessageBusImpl();

        LogFactory.configureFromSystemProperties(workerPool);
        final Log log = LogFactory.getLog("server-main");
        final CairoEngine cairoEngine = new CairoEngine(configuration.getCairoConfiguration(), messageBus);
        workerPool.assign(cairoEngine.getWriterMaintenanceJob());

        final HttpServer httpServer = HttpServer.create(
                configuration.getHttpServerConfiguration(),
                workerPool,
                log,
                cairoEngine,
                messageBus
        );

        final PGWireServer pgWireServer = PGWireServer.create(
                configuration.getPGWireConfiguration(),
                workerPool,
                log,
                cairoEngine,
                messageBus
        );

        final AbstractLineProtoReceiver lineProtocolReceiver;

        if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
            lineProtocolReceiver = new LinuxMMLineProtoReceiver(
                    configuration.getLineUdpReceiverConfiguration(),
                    cairoEngine,
                    workerPool
            );
        } else {
            lineProtocolReceiver = new LineProtoReceiver(
                    configuration.getLineUdpReceiverConfiguration(),
                    cairoEngine,
                    workerPool
            );
        }

        startQuestDb(workerPool, lineProtocolReceiver, log);

        if (Os.type != Os.WINDOWS && optHash.get("-n") == null) {
            // suppress HUP signal
            Signal.handle(new Signal("HUP"), signal -> {
            });
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println(new Date() + " QuestDB is shutting down");
            shutdownQuestDb(workerPool, cairoEngine, httpServer, pgWireServer, lineProtocolReceiver);
            System.err.println(new Date() + " QuestDB is down");
        }));
    }

    public static void deleteOrException(File file) {
        if (!file.exists()) {
            return;
        }
        deleteDirContentsOrException(file);

        int retryCount = 3;
        boolean deleted = false;
        while (retryCount > 0 && !(deleted = file.delete())) {
            retryCount--;
            Thread.yield();
        }

        if (!deleted) {
            throw new RuntimeException("Cannot delete file " + file);
        }
    }

    public static void main(String[] args) throws Exception {
        new ServerMain(args);
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

    private static void extractSite(String dir, boolean force) throws URISyntaxException, IOException {
        System.out.println("Preparing site content...");
        URL url = ServerMain.class.getResource("/site/");
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
                    delete(pub);
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
                            doCopy(dir);
                        } catch (FileAlreadyExistsException ignore) {
                        } catch (IOException x) {
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    doCopy(file);
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

                private void doCopy(Path dir) throws IOException {
                    Path to = toDestination(dir);
                    Files.copy(dir, to, copyOptions);
                    System.out.println("Extracted " + dir + " -> " + to);
                }

                private Path toDestination(final Path path) {
                    final Path tmp = path.toAbsolutePath();
                    return target.resolve(tmp.toString().substring(sourceLen));
                }
            });
        } finally {
            System.out.println("Site content is ready");
            if (fs != null) {
                fs.close();
            }
        }
    }

    private static void deleteDirContentsOrException(File file) {
        if (!file.exists()) {
            return;
        }
        try {
            if (notSymlink(file)) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (int i = 0; i < files.length; i++) {
                        deleteOrException(files[i]);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot delete dir contents: " + file, e);
        }
    }

    private static boolean notSymlink(File file) throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("File must not be null");
        }
        if (File.separatorChar == '\\') {
            return true;
        }

        File fileInCanonicalDir;
        if (file.getParentFile() == null) {
            fileInCanonicalDir = file;
        } else {
            File canonicalDir = file.getParentFile().getCanonicalFile();
            fileInCanonicalDir = new File(canonicalDir, file.getName());
        }

        return fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
    }

    static void delete(File file) {
        deleteOrException(file);
    }

    private String getVersion() throws IOException {
        Enumeration<URL> resources = ServerMain.class.getClassLoader()
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

    protected void shutdownQuestDb(final WorkerPool workerPool,
                                   final CairoEngine cairoEngine,
                                   final HttpServer httpServer,
                                   final PGWireServer pgWireServer,
                                   final AbstractLineProtoReceiver lineProtocolReceiver
    ) {
        lineProtocolReceiver.halt();
        workerPool.halt();
        Misc.free(pgWireServer);
        Misc.free(httpServer);
        Misc.free(cairoEngine);
        Misc.free(lineProtocolReceiver);
    }

    protected void startQuestDb(
            final WorkerPool workerPool,
            final AbstractLineProtoReceiver lineProtocolReceiver,
            final Log log
    ) {
        workerPool.start(log);
        lineProtocolReceiver.start();
    }
}
