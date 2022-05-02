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

package io.questdb;

import io.questdb.cairo.*;
import io.questdb.cutlass.http.HttpServer;
import io.questdb.cutlass.json.JsonException;
import io.questdb.cutlass.line.tcp.LineTcpReceiver;
import io.questdb.cutlass.line.udp.LineUdpReceiver;
import io.questdb.cutlass.line.udp.LinuxMMLineUdpReceiver;
import io.questdb.cutlass.pgwire.PGWireServer;
import io.questdb.griffin.DatabaseSnapshotAgent;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryCache;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkError;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.Path;
import sun.misc.Signal;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ServerMain {
    private static final String VERSION_TXT = "version.txt";
    private static final String PUBLIC_ZIP = "/io/questdb/site/public.zip";

    protected PropServerConfiguration configuration;

    public ServerMain(String[] args) throws Exception {
        //properties fetched from sources other than server.conf
        final BuildInformation buildInformation = BuildInformationHolder.INSTANCE;

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
        // expected flags:
        // -d <root dir> = sets root directory
        // -f = forces copy of site to root directory even if site exists
        // -n = disables handling of HUP signal

        final String rootDirectory = optHash.get("-d");

        LogFactory.configureFromSystemProperties(LogFactory.INSTANCE, null, rootDirectory);
        final Log log = LogFactory.getLog("server-main");

        log.advisoryW().$("QuestDB server ")
                .$(buildInformation.getQuestDbVersion())
                .$(". Copyright (C) 2014-").$(Dates.getYear(System.currentTimeMillis()))
                .$(", all rights reserved.")
                .$();
        extractSite(buildInformation, rootDirectory, log);
        final Properties properties = new Properties();
        final String configurationFileName = "/server.conf";
        final File configurationFile = new File(new File(rootDirectory, PropServerConfiguration.CONFIG_DIRECTORY), configurationFileName);

        try (InputStream is = new FileInputStream(configurationFile)) {
            properties.load(is);
        }

        log.advisoryW().$("Server config: ").$(configurationFile.getAbsoluteFile()).$();

        try {
            readServerConfiguration(rootDirectory, properties, log, buildInformation);
        } catch (ServerConfigurationException sce) {
            log.errorW().$(sce.getMessage()).$();
            throw sce;
        }

        final CairoConfiguration cairoConfiguration = configuration.getCairoConfiguration();

        final boolean httpEnabled = configuration.getHttpServerConfiguration().isEnabled();
        final boolean httpReadOnly = configuration.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext();
        final String httpReadOnlyHint = httpEnabled && httpReadOnly ? " [read-only]" : "";
        final boolean pgEnabled = configuration.getPGWireConfiguration().isEnabled();
        final boolean pgReadOnly = configuration.getPGWireConfiguration().readOnlySecurityContext();
        final String pgReadOnlyHint = pgEnabled && pgReadOnly ? " [read-only]" : "";

        log.advisoryW().$("Config changes applied:").$();
        log.advisoryW().$("  http.enabled : ").$(httpEnabled).$(httpReadOnlyHint).$();
        log.advisoryW().$("  tcp.enabled  : ").$(configuration.getLineTcpReceiverConfiguration().isEnabled()).$();
        log.advisoryW().$("  pg.enabled   : ").$(pgEnabled).$(pgReadOnlyHint).$();

        log.advisoryW().$("open database [id=").$(cairoConfiguration.getDatabaseIdLo()).$('.').$(cairoConfiguration.getDatabaseIdHi()).$(']').$();
        log.advisoryW().$("platform [bit=").$(System.getProperty("sun.arch.data.model")).$(']').$();
        switch (Os.type) {
            case Os.WINDOWS:
                log.advisoryW().$("OS/Arch: windows/amd64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.LINUX_AMD64:
                log.advisoryW().$("OS/Arch: linux/amd64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.OSX_AMD64:
                log.advisoryW().$("OS/Arch: apple/amd64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.OSX_ARM64:
                log.advisoryW().$("OS/Arch: apple/apple-silicon").$();
                break;
            case Os.LINUX_ARM64:
                log.advisoryW().$("OS/Arch: linux/arm64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            case Os.FREEBSD:
                log.advisoryW().$("OS: freebsd/amd64").$(Vect.getSupportedInstructionSetName()).$();
                break;
            default:
                log.criticalW().$("Unsupported OS").$(Vect.getSupportedInstructionSetName()).$();
                break;
        }
        log.advisoryW().$("available CPUs: ").$(Runtime.getRuntime().availableProcessors()).$();
        log.advisoryW().$("db root: ").$(cairoConfiguration.getRoot()).$();
        log.advisoryW().$("backup root: ").$(cairoConfiguration.getBackupRoot()).$();
        try (Path path = new Path()) {
            verifyFileSystem("db", cairoConfiguration.getRoot(), path, log);
            verifyFileSystem("backup", cairoConfiguration.getBackupRoot(), path, log);
            verifyFileOpts(cairoConfiguration, path);
        }

        if (JitUtil.isJitSupported()) {
            final int jitMode = configuration.getCairoConfiguration().getSqlJitMode();
            switch (jitMode) {
                case SqlJitMode.JIT_MODE_ENABLED:
                    log.advisoryW().$("SQL JIT compiler mode: on").$();
                    break;
                case SqlJitMode.JIT_MODE_FORCE_SCALAR:
                    log.advisoryW().$("SQL JIT compiler mode: scalar").$();
                    break;
                case SqlJitMode.JIT_MODE_DISABLED:
                    log.advisoryW().$("SQL JIT compiler mode: off").$();
                    break;
                default:
                    log.errorW().$("Unknown SQL JIT compiler mode: ").$(jitMode).$();
                    break;
            }
        }

        Metrics metrics;
        if (configuration.getMetricsConfiguration().isEnabled()) {
            metrics = Metrics.enabled();
        } else {
            metrics = Metrics.disabled();
        }

        final WorkerPool workerPool = new WorkerPool(configuration.getWorkerPoolConfiguration(), metrics);
        final FunctionFactoryCache functionFactoryCache = new FunctionFactoryCache(
                configuration.getCairoConfiguration(),
                ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader())
        );
        final ObjList<Closeable> instancesToClean = new ObjList<>();

        LogFactory.configureFromSystemProperties(workerPool);
        final CairoEngine cairoEngine = new CairoEngine(configuration.getCairoConfiguration(), metrics);
        workerPool.assign(cairoEngine.getEngineMaintenanceJob());
        instancesToClean.add(cairoEngine);

        final DatabaseSnapshotAgent snapshotAgent = new DatabaseSnapshotAgent(cairoEngine);
        instancesToClean.add(snapshotAgent);

        if (!configuration.getCairoConfiguration().getTelemetryConfiguration().getDisableCompletely()) {
            final TelemetryJob telemetryJob = new TelemetryJob(cairoEngine, functionFactoryCache);
            instancesToClean.add(telemetryJob);

            if (configuration.getCairoConfiguration().getTelemetryConfiguration().getEnabled()) {
                workerPool.assign(telemetryJob);
            }
        }

        workerPool.assignCleaner(Path.CLEANER);
        O3Utils.setupWorkerPool(
                workerPool,
                cairoEngine.getMessageBus(),
                configuration.getCairoConfiguration().getCircuitBreakerConfiguration()
        );

        try {
            initQuestDb(workerPool, cairoEngine, log);

            instancesToClean.add(createHttpServer(workerPool, log, cairoEngine, functionFactoryCache, snapshotAgent, metrics));
            instancesToClean.add(createMinHttpServer(workerPool, log, cairoEngine, functionFactoryCache, snapshotAgent, metrics));

            if (configuration.getPGWireConfiguration().isEnabled()) {
                instancesToClean.add(PGWireServer.create(
                        configuration.getPGWireConfiguration(),
                        workerPool,
                        log,
                        cairoEngine,
                        functionFactoryCache,
                        snapshotAgent,
                        metrics
                ));
            }

            if (configuration.getLineUdpReceiverConfiguration().isEnabled()) {
                if (Os.type == Os.LINUX_AMD64 || Os.type == Os.LINUX_ARM64) {
                    instancesToClean.add(new LinuxMMLineUdpReceiver(
                            configuration.getLineUdpReceiverConfiguration(),
                            cairoEngine,
                            workerPool
                    ));
                } else {
                    instancesToClean.add(new LineUdpReceiver(
                            configuration.getLineUdpReceiverConfiguration(),
                            cairoEngine,
                            workerPool
                    ));
                }
            }

            instancesToClean.add(LineTcpReceiver.create(
                    configuration.getLineTcpReceiverConfiguration(),
                    workerPool,
                    log,
                    cairoEngine,
                    metrics
            ));

            startQuestDb(workerPool, cairoEngine, log);
            if (configuration.getHttpServerConfiguration().isEnabled()) {
                logWebConsoleUrls(log, configuration);
            }

            System.gc();

            log.advisoryW().$("enjoy").$();

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
            log.errorW().$((Sinkable) e).$();
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
        try {
            new ServerMain(args);
        } catch (ServerConfigurationException sce) {
            System.err.println(sce.getMessage());
            System.exit(1);
        }
    }

    static void verifyFileOpts(CairoConfiguration cairoConfiguration, Path path) {
        final FilesFacade ff = cairoConfiguration.getFilesFacade();

        path.of(cairoConfiguration.getRoot()).concat("_verify_").put(cairoConfiguration.getRandom().nextPositiveInt()).put(".d");
        long fd = ff.openRW(path.$(), cairoConfiguration.getWriterFileOpenOpts());

        try {
            if (fd > -1) {
                long mem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    TableUtils.writeLongOrFail(
                            ff,
                            fd,
                            0,
                            123456789L,
                            mem,
                            path
                    );
                } finally {
                    Unsafe.free(mem, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        } finally {
            ff.close(fd);
        }
        ff.remove(path);
    }

    private static void logWebConsoleUrls(Log log, PropServerConfiguration configuration) throws SocketException {
        final LogRecord record = log.infoW().$("web console URL(s):").$('\n').$('\n');
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

    private static String getPublicVersion(String publicDir) throws IOException {
        File f = new File(publicDir, VERSION_TXT);
        if (f.exists()) {
            try (FileInputStream fis = new FileInputStream(f)) {
                byte[] buf = new byte[128];
                int len = fis.read(buf);
                return new String(buf, 0, len);
            }
        }
        return null;
    }

    private static void setPublicVersion(String publicDir, String version) throws IOException {
        File f = new File(publicDir, VERSION_TXT);
        try (FileOutputStream fos = new FileOutputStream(f)) {
            byte[] buf = version.getBytes();
            fos.write(buf, 0, buf.length);
        }
    }

    //made package level for testing only
    static void extractSite(BuildInformation buildInformation, String dir, Log log) throws IOException {
        final String publicZip = "/io/questdb/site/public.zip";
        final String publicDir = dir + "/public";
        final byte[] buffer = new byte[1024 * 1024];
        URL resource = ServerMain.class.getResource(publicZip);
        long thisVersion = Long.MIN_VALUE;
        if (resource == null) {
            log.errorW().$("did not find Web Console build at '").$(publicZip).$("'. Proceeding without Web Console checks").$();
        } else {
            thisVersion = resource.openConnection().getLastModified();
        }

        boolean extracted = false;
        final String oldVersionStr = getPublicVersion(publicDir);
        final CharSequence dbVersion = buildInformation.getQuestDbVersion();
        if (oldVersionStr == null) {
            if (thisVersion != 0) {
                extractSite0(dir, log, publicDir, buffer, Long.toString(thisVersion));
            } else {
                extractSite0(dir, log, publicDir, buffer, Chars.toString(dbVersion));
            }
            extracted = true;
        } else {
            // This is a hack to deal with RT package problem
            // in this package "thisVersion" is always 0, and we need to fall back
            // to the database version.
            if (thisVersion == 0) {
                if (!Chars.equals(oldVersionStr, dbVersion)) {
                    extractSite0(dir, log, publicDir, buffer, Chars.toString(dbVersion));
                    extracted = true;
                }
            } else {
                // it is possible that old version is the database version
                // which means user might have switched from RT distribution to no-JVM on the same data dir
                // in this case we might fail to parse the version string
                try {
                    final long oldVersion = Numbers.parseLong(oldVersionStr);
                    if (thisVersion > oldVersion) {
                        extractSite0(dir, log, publicDir, buffer, Long.toString(thisVersion));
                        extracted = true;
                    }
                } catch (NumericException e) {
                    extractSite0(dir, log, publicDir, buffer, Long.toString(thisVersion));
                    extracted = true;
                }
            }
        }

        if (!extracted) {
            log.infoW().$("web console is up to date").$();
        }
    }

    private static void extractSite0(String dir, Log log, String publicDir, byte[] buffer, String thisVersion) throws IOException {
        try (final InputStream is = ServerMain.class.getResourceAsStream(PUBLIC_ZIP)) {
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
                log.errorW().$("could not find site [resource=").$(PUBLIC_ZIP).$(']').$();
            }
        }
        setPublicVersion(publicDir, thisVersion);
        copyConfResource(dir, false, buffer, "conf/date.formats", log);
        copyConfResource(dir, true, buffer, "conf/mime.types", log);
        copyConfResource(dir, false, buffer, "conf/server.conf", log);
        copyConfResource(dir, false, buffer, "conf/log.conf", log);
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
                log.errorW().$("could not create directory [path=").$(dir).$(']').$();
                return;
            }
            try (FileOutputStream fos = new FileOutputStream(out)) {
                int n;
                while ((n = is.read(buffer, 0, buffer.length)) > 0) {
                    fos.write(buffer, 0, n);
                }
            }
            log.infoW().$("extracted [path=").$(out).$(']').$();
            return;
        }
        log.debugW().$("skipped [path=").$(out).$(']').$();
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

    private static void verifyFileSystem(String kind, CharSequence dir, Path path, Log log) {
        if (dir != null) {
            path.of(dir).$();
            // path will contain file system name
            long fsStatus = Files.getFileSystemStatus(path);
            path.seekZ();
            LogRecord rec = log.advisoryW().$(kind).$(" file system magic: 0x");
            if (fsStatus < 0) {
                rec.$hex(-fsStatus).$(" [").$(path).$("] SUPPORTED").$();
            } else {
                rec.$hex(fsStatus).$(" [").$(path).$("] EXPERIMENTAL").$();
                log.advisoryW().$("\n\n\n\t\t\t*** SYSTEM IS USING UNSUPPORTED FILE SYSTEM AND COULD BE UNSTABLE ***\n\n").$();
            }
        }
    }

    protected HttpServer createHttpServer(
            final WorkerPool workerPool,
            final Log log,
            final CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics) {
        return HttpServer.create(
                configuration.getHttpServerConfiguration(),
                workerPool,
                log,
                cairoEngine,
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
    }

    protected HttpServer createMinHttpServer(
            final WorkerPool workerPool,
            final Log log,
            final CairoEngine cairoEngine,
            FunctionFactoryCache functionFactoryCache,
            DatabaseSnapshotAgent snapshotAgent,
            Metrics metrics) {
        if (!metrics.isEnabled()) {
            log.advisoryW().$("Min health server is starting. Health check endpoint will not consider unhandled errors when metrics are disabled.").$();
        }
        return HttpServer.createMin(
                configuration.getHttpMinServerConfiguration(),
                workerPool,
                log,
                cairoEngine,
                functionFactoryCache,
                snapshotAgent,
                metrics
        );
    }

    @SuppressWarnings("unused")
    protected void initQuestDb(
            final WorkerPool workerPool,
            final CairoEngine cairoEngine,
            final Log log
    ) {
        // For extension
    }

    protected void readServerConfiguration(
            final String rootDirectory,
            final Properties properties,
            Log log,
            final BuildInformation buildInformation
    ) throws ServerConfigurationException, JsonException {
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
