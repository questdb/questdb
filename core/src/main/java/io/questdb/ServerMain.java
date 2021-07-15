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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.locks.LockSupport;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.O3CallbackJob;
import io.questdb.cairo.O3CopyJob;
import io.questdb.cairo.O3OpenColumnJob;
import io.questdb.cairo.O3PartitionJob;
import io.questdb.cairo.O3PurgeDiscoveryJob;
import io.questdb.cairo.O3PurgeJob;
import io.questdb.cairo.O3Utils;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.line.tcp.LineTcpServer;
import io.questdb.cutlass.line.udp.LineProtoReceiver;
import io.questdb.cutlass.line.udp.LinuxMMLineProtoReceiver;
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkError;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.Path;
import sun.misc.Signal;

public class ServerMain {
    private static final String VERSION_TXT = "version.txt";

    protected PropServerConfiguration configuration;

    public ServerMain(String[] args) throws Exception {
        //properties fetched from sources other than server.conf
        final BuildInformation buildInformation = fetchBuildInformation();

        System.err.printf(
                "QuestDB server %s%nCopyright (C) 2014-%d, all rights reserved.%n%n",
                buildInformation.getQuestDbVersion(),
                Dates.getYear(System.currentTimeMillis())
        );
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
        extractSite(rootDirectory, log);
        final Properties properties = new Properties();
        final String configurationFileName = "/server.conf";
        final File configurationFile = new File(new File(rootDirectory, PropServerConfiguration.CONFIG_DIRECTORY), configurationFileName);

        try (InputStream is = new FileInputStream(configurationFile)) {
            properties.load(is);
        }

        readServerConfiguration(rootDirectory, properties, log, buildInformation);
        log.info().$("Server config : ").$(configurationFile.getAbsoluteFile()).$();
        log.info().$("Config changes applied:").$();
        log.info().$("  http.enabled : ").$(configuration.getHttpServerConfiguration().isEnabled()).$();
        log.info().$("  tcp.enabled  : ").$(configuration.getLineTcpReceiverConfiguration().isEnabled()).$();
        log.info().$("  pg.enabled   : ").$(configuration.getPGWireConfiguration().isEnabled()).$();

        log.info().$("open database [id=").$(configuration.getCairoConfiguration().getDatabaseIdLo()).$('.').$(configuration.getCairoConfiguration().getDatabaseIdHi()).$(']').$();
        log.info().$("platform [bit=").$(System.getProperty("sun.arch.data.model")).$(']').$();
        switch (Os.type) {
            case Os.WINDOWS:
                log.info().$("OS: windows-amd64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.LINUX_AMD64:
                log.info().$("OS: linux-amd64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.OSX_AMD64:
                log.info().$("OS: apple-amd64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.OSX_ARM64:
                log.info().$("OS: apple-silicon").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.LINUX_ARM64:
                log.info().$("OS: linux-arm64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.FREEBSD:
                log.info().$("OS: freebsd-amd64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            default:
                log.error().$("Unsupported OS").$(Vect.getSupportedInstructionSetName()).$();
                break;
        }

        final WorkerPool workerPool = new WorkerPool(configuration.getWorkerPoolConfiguration());
        final FunctionFactoryCache functionFactoryCache = new FunctionFactoryCache(
                configuration.getCairoConfiguration(),
                ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
        );
        final ObjList<Closeable> instancesToClean = new ObjList<>();

        LogFactory.configureFromSystemProperties(workerPool);
        final CairoEngine cairoEngine = new CairoEngine(configuration.getCairoConfiguration());
        workerPool.assign(cairoEngine.getWriterMaintenanceJob());
        instancesToClean.add(cairoEngine);

        // The TelemetryJob is always needed (even when telemetry is off) because it is responsible for
        // updating the telemetry_config table.
        final TelemetryJob telemetryJob = new TelemetryJob(cairoEngine, functionFactoryCache);
        instancesToClean.add(telemetryJob);

        if (configuration.getCairoConfiguration().getTelemetryConfiguration().getEnabled()) {
            workerPool.assign(telemetryJob);
        }

        workerPool.assignCleaner(Path.CLEANER);
        workerPool.assign(new O3CallbackJob(cairoEngine.getMessageBus()));
        workerPool.assign(new O3PartitionJob(cairoEngine.getMessageBus()));
        workerPool.assign(new O3OpenColumnJob(cairoEngine.getMessageBus()));
        workerPool.assign(new O3CopyJob(cairoEngine.getMessageBus()));
        workerPool.assign(new O3PurgeDiscoveryJob(cairoEngine.getMessageBus(), workerPool.getWorkerCount()));
        workerPool.assign(new O3PurgeJob(cairoEngine.getMessageBus()));
        O3Utils.initBuf(workerPool.getWorkerCount() + 1);

        Metrics metrics;
        if (configuration.getMetricsConfiguration().isEnabled()) {
            metrics = Metrics.enabled();
        } else {
            metrics = Metrics.disabled();
        }

        try {
            initQuestDb(workerPool, cairoEngine, log);

            instancesToClean.add(createHttpServer(workerPool, log, cairoEngine, functionFactoryCache, metrics));
            instancesToClean.add(createMinHttpServer(workerPool, log, cairoEngine, functionFactoryCache, metrics));

            if (configuration.getPGWireConfiguration().isEnabled()) {
                instancesToClean.add(PGWireServer.create(
                        configuration.getPGWireConfiguration(),
                        workerPool,
                        log,
                        cairoEngine,
                        functionFactoryCache,
                        metrics
                ));
            }

            if (configuration.getLineUdpReceiverConfiguration().isEnabled()) {
                if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
                    instancesToClean.add(new LinuxMMLineProtoReceiver(
                            configuration.getLineUdpReceiverConfiguration(),
                            cairoEngine,
                            workerPool
                    ));
                } else {
                    instancesToClean.add(new LineProtoReceiver(
                            configuration.getLineUdpReceiverConfiguration(),
                            cairoEngine,
                            workerPool
                    ));
                }
            }

            instancesToClean.add(LineTcpServer.create(
                    configuration.getLineTcpReceiverConfiguration(),
                    workerPool,
                    log,
                    cairoEngine
            ));

            startQuestDb(workerPool, cairoEngine, log);
            if (configuration.getHttpServerConfiguration().isEnabled()) {
                logWebConsoleUrls(log, configuration);
            }

            System.gc();

            if (Os.type != Os.WINDOWS && optHash.get("-n") == null) {
                // suppress HUP signal
                Signal.handle(new Signal("HUP"), signal -> {
                });
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.err.println(new Date() + " QuestDB is shutting down");
                shutdownQuestDb(workerPool, instancesToClean);
                System.err.println(new Date() + " QuestDB is down");
            }));
        } catch (NetworkError e) {
            log.error().$((Sinkable) e).$();
            LockSupport.parkNanos(10000000L);
            System.exit(55);
        }
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

    private static void logWebConsoleUrls(Log log, PropServerConfiguration configuration) throws SocketException {
        final LogRecord record = log.info().$("web console URL(s):").$('\n').$('\n');
        final int httpBindIP = configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindIPv4Address();
        final int httpBindPort = configuration.getHttpServerConfiguration().getDispatcherConfiguration().getBindPort();
        if (httpBindIP == 0) {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            for (NetworkInterface networkInterface : Collections.list(nets)) {
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                for (InetAddress inetAddress : Collections.list(inetAddresses)) {
                    if (inetAddress instanceof Inet4Address) {
                        record.$('\t').$("http:/").$(inetAddress).$(':').$(httpBindPort).$('\n');
                    }
                }
            }
            record.$('\n').$();
        } else {
            record.$('\t').$("http://").$ip(httpBindIP).$(':').$(httpBindPort).$('\n').$();
        }
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


    private static long getPublicVersion(String publicDir) throws IOException {
        File f = new File(publicDir, VERSION_TXT);
        if (f.exists()) {
            try (FileInputStream fis = new FileInputStream(f)) {
                byte[] buf = new byte[128];
                int len = fis.read(buf);
                return Long.parseLong(new String(buf, 0, len));
            }
        }
        return Long.MIN_VALUE;
    }

    private static void setPublicVersion(String publicDir, long version) throws IOException {
        File f = new File(publicDir, VERSION_TXT);
        try (FileOutputStream fos = new FileOutputStream(f)) {
            byte[] buf = Long.toString(version).getBytes();
            fos.write(buf, 0, buf.length);
        }
    }

    private static void extractSite(String dir, Log log) throws IOException {
        final String publicZip = "/io/questdb/site/public.zip";
        final String publicDir = dir + "/public";
        final byte[] buffer = new byte[1024 * 1024];
        URL resource = ServerMain.class.getResource(publicZip);
        long thisVersion = Long.MIN_VALUE;
        if (resource == null) {
            log.error().$("did not find Web Console build at '").$(publicZip).$("'. Proceeding without Web Console checks").$();
        } else {
            thisVersion = resource.openConnection().getLastModified();
        }
        final long oldVersion = getPublicVersion(publicDir);
        if (thisVersion > oldVersion) {
            try (final InputStream is = ServerMain.class.getResourceAsStream(publicZip)) {
                if (is != null) {
                    try (ZipInputStream zip = new ZipInputStream(is)) {
                        ZipEntry ze;
                        while ((ze = zip.getNextEntry()) != null) {
                            final File dest = new File(publicDir, ze.getName());
                            if (!ze.isDirectory()) {
                                copyInputStream(true, buffer, dest, zip, log);
                            }
                            zip.closeEntry();
                        }
                    }
                } else {
                    log.error().$("could not find site [resource=").$(publicZip).$(']').$();
                }
            }
            setPublicVersion(publicDir, thisVersion);
            copyConfResource(dir, false, buffer, "conf/date.formats", log);
            copyConfResource(dir, true, buffer, "conf/mime.types", log);
            copyConfResource(dir, false, buffer, "conf/server.conf", log);
        } else {
            log.info().$("web console is up to date").$();
        }
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

    protected static void shutdownQuestDb(final WorkerPool workerPool, final ObjList<? extends Closeable> instancesToClean) {
        workerPool.halt();
        Misc.freeObjList(instancesToClean);
    }

    private static CharSequence getQuestDbVersion(final Attributes manifestAttributes) {
        final CharSequence version = manifestAttributes.getValue("Implementation-Version");
        return version != null ? version : "[DEVELOPMENT]";
    }

    private static CharSequence getJdkVersion(final Attributes manifestAttributes) {
        final CharSequence version = manifestAttributes.getValue("Build-Jdk");
        return version != null ? version : "Unknown Version";
    }

    private static CharSequence getCommitHash(final Attributes manifestAttributes) {
        final CharSequence version = manifestAttributes.getValue("Build-Commit-Hash");
        return version != null ? version : "Unknown Version";
    }

    private static BuildInformation fetchBuildInformation() throws IOException {
        final Attributes manifestAttributes = getManifestAttributes();

        return new BuildInformationHolder(
                getQuestDbVersion(manifestAttributes),
                getJdkVersion(manifestAttributes),
                getCommitHash(manifestAttributes)
        );
    }

    private static Attributes getManifestAttributes() throws IOException {
        final Enumeration<URL> resources = ServerMain.class.getClassLoader()
                .getResources("META-INF/MANIFEST.MF");
        while (resources.hasMoreElements()) {
            try (InputStream is = resources.nextElement().openStream()) {
                final Manifest manifest = new Manifest(is);
                final Attributes attributes = manifest.getMainAttributes();
                if ("org.questdb".equals(attributes.getValue("Implementation-Vendor-Id"))) {
                    return manifest.getMainAttributes();
                }
            }
        }

        return new Attributes();
    }

    protected HttpServer createHttpServer(
            final WorkerPool workerPool,
            final Log log,
            final CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            Metrics metrics) {
        return HttpServer.create(
                configuration.getHttpServerConfiguration(),
                workerPool,
                log,
                cairoEngine,
                functionFactoryCache,
                metrics
        );
    }

    protected HttpServer createMinHttpServer(
            final WorkerPool workerPool,
            final Log log,
            final CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            Metrics metrics) {
        return HttpServer.createMin(
                configuration.getHttpMinServerConfiguration(),
                workerPool,
                log,
                cairoEngine,
                functionFactoryCache,
                metrics
        );
    }

    protected void initQuestDb(
            final WorkerPool workerPool,
            final CairoEngine cairoEngine,
            final Log log
    ) {
        // For extension
    }

    protected void readServerConfiguration(final String rootDirectory,
                                           final Properties properties,
                                           Log log,
                                           final BuildInformation buildInformation) throws ServerConfigurationException, JsonException {
        configuration = new PropServerConfiguration(rootDirectory, properties, System.getenv(), log, buildInformation);
    }

    protected void startQuestDb(
            final WorkerPool workerPool,
            final CairoEngine cairoEngine,
            final Log log
    ) {
        workerPool.start(log);
    }
}