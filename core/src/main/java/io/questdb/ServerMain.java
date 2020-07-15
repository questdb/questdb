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
import io.questdb.cutlass.json.JsonException;
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

import java.io.*;
import java.net.URL;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ServerMain {
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

    protected PropServerConfiguration configuration;
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

        final Log log = LogFactory.getLog("server-main");
        // expected flags:
        // -d <root dir> = sets root directory
        // -f = forces copy of site to root directory even if site exists
        // -n = disables handling of HUP signal

        final String rootDirectory = optHash.get("-d");
        extractSite(rootDirectory, optHash.get("-f") != null, log);
        final Properties properties = new Properties();
        final String configurationFileName = "/server.conf";
        final File configurationFile = new File(new File(rootDirectory, PropServerConfiguration.CONFIG_DIRECTORY), configurationFileName);

        try (InputStream is = new FileInputStream(configurationFile)) {
            properties.load(is);
        }
        readServerConfiguration(rootDirectory, properties);

        // create database directory
        try (io.questdb.std.str.Path path = new io.questdb.std.str.Path()) {
            path.of(configuration.getCairoConfiguration().getRoot());
            if (!Chars.endsWith(path, io.questdb.std.Files.SEPARATOR)) {
                // this would end trailing path separator
                path.concat("");
            }
            path.$();

            if (io.questdb.std.Files.mkdirs(path, configuration.getCairoConfiguration().getMkDirMode()) != 0) {
                log.error().$("could not create database root [dir=").$(path).$(']').$();
                System.exit(30);
            } else {
                log.info().$("database root [dir=").$(path).$(']').$();
            }
        }
        switch (Os.type) {
            case Os.WINDOWS:
                log.info().$("OS: windows-amd64 ").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.LINUX_AMD64:
                log.info().$("OS: linux-amd64 ").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.OSX:
                log.info().$("OS: apple-amd64 ").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.LINUX_ARM64:
                log.info().$("OS: linux-arm64 ").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.FREEBSD:
                log.info().$("OS: freebsd-amd64 ").$(Vect.getSupportedInstructionSetName()).$();
                break;
            default:
                log.error().$("Unsupported OS ").$(Vect.getSupportedInstructionSetName()).$();
                break;
        }

        final WorkerPool workerPool = new WorkerPool(configuration.getWorkerPoolConfiguration());
        final MessageBus messageBus = new MessageBusImpl();

        LogFactory.configureFromSystemProperties(workerPool);
        final CairoEngine cairoEngine = new CairoEngine(configuration.getCairoConfiguration(), messageBus);
        workerPool.assign(cairoEngine.getWriterMaintenanceJob());
        initQuestDb(workerPool, cairoEngine, log);

        final HttpServer httpServer = createHttpServer(workerPool, messageBus, log, cairoEngine);

        final PGWireServer pgWireServer;
        if (null != configuration.getPGWireConfiguration()) {
            pgWireServer = PGWireServer
                    .create(
                configuration.getPGWireConfiguration(),
                workerPool,
                log,
                cairoEngine,
                messageBus
        );
        } else {
            pgWireServer = null;
        }

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

        startQuestDb(workerPool, cairoEngine, lineProtocolReceiver, log);

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

    protected HttpServer createHttpServer(final WorkerPool workerPool, final MessageBus messageBus, final Log log, final CairoEngine cairoEngine) {
        return HttpServer
                .create(
                configuration.getHttpServerConfiguration(),
                workerPool,
                log,
                cairoEngine,
                messageBus
        );
    }

    protected void readServerConfiguration(final String rootDirectory, final Properties properties) throws ServerConfigurationException, JsonException {
        configuration = new PropServerConfiguration(rootDirectory, properties);
    }

    private static CharSequenceObjHashMap<String> hashArgs(String[] args) {
        CharSequenceObjHashMap<String> optHash = new CharSequenceObjHashMap<>();
        String flag = null;
        for (String s : args) {
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

    private static void extractSite(String dir, boolean force, Log log) throws IOException {
        final String publicZip = "/io/questdb/site/public.zip";
        final String publicDest = dir + "/public";
        final byte[] buffer = new byte[1024 * 1024];
        try (final InputStream is = ServerMain.class.getResourceAsStream(publicZip)) {
            if (is != null) {
                try (ZipInputStream zip = new ZipInputStream(is)) {
                    ZipEntry ze;
                    while ((ze = zip.getNextEntry()) != null) {
                        final File dest = new File(publicDest, ze.getName());
                        if (!ze.isDirectory()) {
                            copyInputStream(force, buffer, dest, zip, log);
                        }
                        zip.closeEntry();
                    }
                }
            } else {
                log.error().$("could not find site [resource=").$(publicZip).$(']').$();
            }
        }
        copyConfResource(dir, force, buffer, "conf/date.formats", log);
        copyConfResource(dir, force, buffer, "conf/mime.types", log);
        copyConfResource(dir, force, buffer, "conf/server.conf", log);
    }

    private static void copyConfResource(String dir, boolean force, byte[] buffer, String res, Log log) throws IOException {
        File out = new File(dir, res);
        try (InputStream is = ServerMain.class.getResourceAsStream("/io/questdb/site/" + res)) {
            if (is != null) {
                copyInputStream(force, buffer, out, is, log);
            }
        }
    }

    private static void copyInputStream(boolean force, byte[] buffer, File out, InputStream is, Log log) throws IOException {
        final boolean exists = out.exists();
        if (force || !exists) {
            File dir = out.getParentFile();
            if (!dir.exists() && !dir.mkdirs()) {
                log.error().$("could not create directory [path=").$(dir).$(']').$();
                return;
            }
            try (FileOutputStream fos = new FileOutputStream(out)) {
                int n;
                while ((n = is.read(buffer, 0, buffer.length)) > 0) {
                    fos.write(buffer, 0, n);
                }
            }
            log.info().$("extracted [path=").$(out).$(']').$();
            return;
        }
        log.debug().$("skipped [path=").$(out).$(']').$();
    }

    private static void deleteDirContentsOrException(File file) {
        if (!file.exists()) {
            return;
        }
        try {
            if (notSymlink(file)) {
                File[] files = file.listFiles();
                if (files != null) {
                    for (File f : files) {
                        deleteOrException(f);
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

    private static String getVersion() throws IOException {
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

    protected static void shutdownQuestDb(final WorkerPool workerPool,
                                          final CairoEngine cairoEngine,
                                          final HttpServer httpServer,
                                          final PGWireServer pgWireServer,
                                          final AbstractLineProtoReceiver lineProtocolReceiver
    ) {
        lineProtocolReceiver.halt();
        workerPool.halt();
        if (null != pgWireServer) {
            Misc.free(pgWireServer);
        }
        Misc.free(httpServer);
        Misc.free(cairoEngine);
        Misc.free(lineProtocolReceiver);
    }

    protected void initQuestDb(
            final WorkerPool workerPool,
            final CairoEngine cairoEngine,
            final Log log
    ) {
        // For extension
    }

    protected void startQuestDb(
            final WorkerPool workerPool,
            final CairoEngine cairoEngine,
            final AbstractLineProtoReceiver lineProtocolReceiver,
            final Log log
    ) {
        workerPool.start(log);
        lineProtocolReceiver.start();
    }
}
