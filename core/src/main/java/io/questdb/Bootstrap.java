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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogRecord;
import io.questdb.network.IODispatcherConfiguration;
import io.questdb.std.*;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.NativeLPSZ;
import io.questdb.std.str.Path;
import org.jetbrains.annotations.NotNull;
import sun.misc.Signal;

import java.io.*;
import java.net.*;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Bootstrap {

    public static final String SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION = "--use-default-log-factory-configuration";
    private static final String BANNER =
            "     ___                  _   ____  ____\n" +
                    "    / _ \\ _   _  ___  ___| |_|  _ \\| __ )\n" +
                    "   | | | | | | |/ _ \\/ __| __| | | |  _ \\\n" +
                    "   | |_| | |_| |  __/\\__ \\ |_| |_| | |_) |\n" +
                    "    \\__\\_\\\\__,_|\\___||___/\\__|____/|____/\n\n";

    private static final BuildInformation buildInformation = BuildInformationHolder.INSTANCE;
    private static final String LOG_NAME = "server-main";
    private static final String CONFIG_FILE = "/server.conf";
    private static final String PUBLIC_VERSION_TXT = "version.txt";
    private static final String PUBLIC_ZIP = "/io/questdb/site/public.zip";
    private final String rootDirectory;
    private final PropServerConfiguration config;
    private final Metrics metrics;
    private final Log log;
    private final String banner;

    public Bootstrap(String... args) {
        this(BANNER, args);
    }

    public Bootstrap(String banner, String... args) {
        if (args.length < 2) {
            throw new BootstrapException("Root directory name expected (-d <root-path>)");
        }
        this.banner = banner;

        // non /server.conf properties
        final CharSequenceObjHashMap<String> argsMap = processArgs(args);
        rootDirectory = argsMap.get("-d");
        if (Chars.isBlank(rootDirectory)) {
            throw new BootstrapException("Root directory name expected (-d <root-path>)");
        }
        final File rootPath = new File(rootDirectory);
        if (!rootPath.exists()) {
            throw new BootstrapException("Root directory does not exist: " + rootDirectory);
        }
        if (argsMap.get("-n") == null && Os.type != Os.WINDOWS) {
            Signal.handle(new Signal("HUP"), signal -> { /* suppress HUP signal */ });
        }

        // setup logger
        if (argsMap.get(SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION) == null) {
            LogFactory.configureFromSystemProperties(new LogFactory(), rootDirectory);
        }
        log = LogFactory.getLog(LOG_NAME);

        // report copyright and architecture
        log.advisoryW().$("QuestDB server ").$(buildInformation.getQuestDbVersion()).$(". Copyright (C) 2014-").$(Dates.getYear(System.currentTimeMillis())).$(", all rights reserved.").$();
        String archName;
        boolean isOsSupported = true;
        switch (Os.type) {
            case Os.WINDOWS:
                archName = "OS/Arch windows/amd64";
                break;
            case Os.LINUX_AMD64:
                archName = "OS/Arch linux/amd64";
                break;
            case Os.OSX_AMD64:
                archName = "OS/Arch apple/amd64";
                break;
            case Os.OSX_ARM64:
                archName = "OS/Arch apple/apple-silicon";
                break;
            case Os.LINUX_ARM64:
                archName = "OS/Arch linux/arm64";
                break;
            case Os.FREEBSD:
                archName = "OS/ARCH freebsd/amd64";
                break;
            default:
                isOsSupported = false;
                archName = "Unsupported OS";
                break;
        }
        StringBuilder sb = new StringBuilder(Vect.getSupportedInstructionSetName());
        sb.setLength(sb.length() - 1); // remove ending ']'
        sb.append(", ").append(System.getProperty("sun.arch.data.model")).append(" bits");
        sb.append(", ").append(Runtime.getRuntime().availableProcessors()).append(" processors");
        if (isOsSupported) {
            log.advisoryW().$(archName).$(sb).I$();
        } else {
            log.criticalW().$(archName).$(sb).I$();
        }

        try {
            // site
            extractSite();

            // /server.conf properties
            final Properties properties = loadProperties(rootPath);
            config = new PropServerConfiguration(rootDirectory, properties, System.getenv(), log, buildInformation);
            reportValidateConfig();
            reportCrashFiles(config.getCairoConfiguration(), log);
        } catch (Throwable e) {
            log.errorW().$(e).$();
            throw new BootstrapException(e);
        }

        // metrics
        if (config.getMetricsConfiguration().isEnabled()) {
            metrics = Metrics.enabled();
        } else {
            metrics = Metrics.disabled();
            log.advisoryW().$("Metrics are disabled, health check endpoint will not consider unhandled errors").$();
        }
    }

    public String getBanner() {
        return banner;
    }

    public PropServerConfiguration getConfiguration() {
        return config;
    }

    public Log getLog() {
        return log;
    }

    public Metrics getMetrics() {
        return metrics;
    }

    static void logWebConsoleUrls(PropServerConfiguration config, Log log, String banner) {
        if (config.getHttpServerConfiguration().isEnabled()) {
            final LogRecord r = log.infoW().$('\n')
                    .$(banner)
                    .$("Web Console URL(s):").$("\n\n");

            final IODispatcherConfiguration httpConf = config.getHttpServerConfiguration().getDispatcherConfiguration();
            final int bindIP = httpConf.getBindIPv4Address();
            final int bindPort = httpConf.getBindPort();
            if (bindIP == 0) {
                try {
                    for (Enumeration<NetworkInterface> ni = NetworkInterface.getNetworkInterfaces(); ni.hasMoreElements(); ) {
                        for (Enumeration<InetAddress> addr = ni.nextElement().getInetAddresses(); addr.hasMoreElements(); ) {
                            InetAddress inetAddress = addr.nextElement();
                            if (inetAddress instanceof Inet4Address) {
                                r.$('\t').$("http://").$(inetAddress.getHostAddress()).$(':').$(bindPort).$('\n');
                            }
                        }
                    }
                } catch (SocketException se) {
                    throw new Bootstrap.BootstrapException("Cannot access network interfaces");
                }
                r.$('\n').$();
            } else {
                r.$('\t').$("http://").$ip(bindIP).$(':').$(bindPort).$('\n').$();
            }
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
                log.errorW().$("could not create directory [path=").$(dir).I$();
                return;
            }
            try (FileOutputStream fos = new FileOutputStream(out)) {
                int n;
                while ((n = is.read(buffer, 0, buffer.length)) > 0) {
                    fos.write(buffer, 0, n);
                }
            }
            log.infoW().$("extracted [path=").$(out).I$();
            return;
        }
        log.debugW().$("skipped [path=").$(out).I$();
    }

    private static String getPublicVersion(String publicDir) throws IOException {
        File f = new File(publicDir, PUBLIC_VERSION_TXT);
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
        File f = new File(publicDir, PUBLIC_VERSION_TXT);
        File publicFolder = f.getParentFile();
        if (!publicFolder.exists()) {
            if (!publicFolder.mkdirs()) {
                throw new BootstrapException("Cannot create folder: " + publicFolder);
            }
        }
        try (FileOutputStream fos = new FileOutputStream(f)) {
            byte[] buf = version.getBytes();
            fos.write(buf, 0, buf.length);
        }
    }

    static void reportCrashFiles(CairoConfiguration cairoConfiguration, Log log) {
        final CharSequence dbRoot = cairoConfiguration.getRoot();
        final FilesFacade ff = cairoConfiguration.getFilesFacade();
        final int maxFiles = cairoConfiguration.getMaxCrashFiles();
        NativeLPSZ name = new NativeLPSZ();
        try (
                Path path = new Path().of(dbRoot).slash$();
                Path other = new Path().of(dbRoot).slash$()
        ) {
            int plen = path.length();
            AtomicInteger counter = new AtomicInteger(0);
            FilesFacadeImpl.INSTANCE.iterateDir(path, (pUtf8NameZ, type) -> {
                if (Files.notDots(pUtf8NameZ)) {
                    name.of(pUtf8NameZ);
                    if (Chars.startsWith(name, cairoConfiguration.getOGCrashFilePrefix()) && type == Files.DT_FILE) {
                        path.trimTo(plen).concat(pUtf8NameZ).$();
                        boolean shouldRename = false;
                        do {
                            other.trimTo(plen).concat(cairoConfiguration.getArchivedCrashFilePrefix()).put(counter.getAndIncrement()).put(".log").$();
                            if (!ff.exists(other)) {
                                shouldRename = counter.get() <= maxFiles;
                                break;
                            }
                        } while (counter.get() < maxFiles);
                        if (shouldRename && ff.rename(path, other) == 0) {
                            log.criticalW().$("found crash file [path=").$(other).I$();
                        } else {
                            log.criticalW().
                                    $("could not rename crash file [path=").$(path)
                                    .$(", errno=").$(ff.errno())
                                    .$(", index=").$(counter.get())
                                    .$(", max=").$(maxFiles)
                                    .I$();
                        }
                    }
                }
            });
        }
    }

    private static void verifyFileOpts(Path path, CairoConfiguration cairoConfiguration) {
        final FilesFacade ff = cairoConfiguration.getFilesFacade();
        path.of(cairoConfiguration.getRoot()).concat("_verify_").put(cairoConfiguration.getRandom().nextPositiveInt()).put(".d").$();
        long fd = ff.openRW(path, cairoConfiguration.getWriterFileOpenOpts());
        try {
            if (fd > -1) {
                long mem = Unsafe.malloc(Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                try {
                    TableUtils.writeLongOrFail(ff, fd, 0, 123456789L, mem, path);
                } finally {
                    Unsafe.free(mem, Long.BYTES, MemoryTag.NATIVE_DEFAULT);
                }
            }
        } finally {
            ff.close(fd);
        }
        ff.remove(path);
    }

    static CharSequenceObjHashMap<String> processArgs(String... args) {
        final int n = args.length;
        if (n == 0) {
            throw new BootstrapException("Arguments expected, non provided");
        }
        CharSequenceObjHashMap<String> optHash = new CharSequenceObjHashMap<>();
        for (int i = 0; i < n; i++) {
            String arg = args[i];
            if (arg.length() > 1 && arg.charAt(0) == '-') {
                if (i + 1 < n) {
                    String nextArg = args[i + 1];
                    if (nextArg.length() > 1 && nextArg.charAt(0) == '-') {
                        optHash.put(arg, "");
                    } else {
                        optHash.put(arg, nextArg);
                        i++;
                    }
                } else {
                    optHash.put(arg, "");
                }
            } else {
                optHash.put("$" + i, arg);
            }
        }
        return optHash;
    }

    void extractSite() throws IOException {
        URL resource = ServerMain.class.getResource(PUBLIC_ZIP);
        long thisVersion = Long.MIN_VALUE;
        if (resource == null) {
            log.infoW().$("Web Console build [").$(PUBLIC_ZIP).$("] not found").$();
        } else {
            thisVersion = resource.openConnection().getLastModified();
        }

        final String publicDir = rootDirectory + Files.SEPARATOR + "public";
        final byte[] buffer = new byte[1024 * 1024];

        boolean extracted = false;
        final String oldVersionStr = getPublicVersion(publicDir);
        final CharSequence dbVersion = buildInformation.getQuestDbVersion();
        if (oldVersionStr == null) {
            if (thisVersion != 0) {
                extractSite0(publicDir, buffer, Long.toString(thisVersion));
            } else {
                extractSite0(publicDir, buffer, Chars.toString(dbVersion));
            }
            extracted = true;
        } else {
            // This is a hack to deal with RT package problem
            // in this package "thisVersion" is always 0, and we need to fall back
            // to the database version.
            if (thisVersion == 0) {
                if (!Chars.equals(oldVersionStr, dbVersion)) {
                    extractSite0(publicDir, buffer, Chars.toString(dbVersion));
                    extracted = true;
                }
            } else {
                // it is possible that old version is the database version
                // which means user might have switched from RT distribution to no-JVM on the same data dir
                // in this case we might fail to parse the version string
                try {
                    final long oldVersion = Numbers.parseLong(oldVersionStr);
                    if (thisVersion > oldVersion) {
                        extractSite0(publicDir, buffer, Long.toString(thisVersion));
                        extracted = true;
                    }
                } catch (NumericException e) {
                    extractSite0(publicDir, buffer, Long.toString(thisVersion));
                    extracted = true;
                }
            }
        }
        if (!extracted) {
            log.infoW().$("Web Console is up to date").$();
        }
    }

    private void extractSite0(String publicDir, byte[] buffer, String thisVersion) throws IOException {
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
        copyConfResource(rootDirectory, false, buffer, "conf/date.formats", log);
        try {
            copyConfResource(rootDirectory, true, buffer, "conf/mime.types", log);
        } catch (IOException exception) {
            // conf can be read-only, this is not critical
            if (exception.getMessage() == null || (!exception.getMessage().contains("Read-only file system") && !exception.getMessage().contains("Permission denied"))) {
                throw exception;
            }
        }
        copyConfResource(rootDirectory, false, buffer, "conf/server.conf", log);
        copyConfResource(rootDirectory, false, buffer, "conf/log.conf", log);
    }

    @NotNull
    private Properties loadProperties(File rootPath) throws IOException {
        final Properties properties = new Properties();
        java.nio.file.Path configFile = Paths.get(rootPath.getAbsolutePath(), PropServerConfiguration.CONFIG_DIRECTORY, CONFIG_FILE);
        log.advisoryW().$("Server config: ").$(configFile).$();

        try (InputStream is = java.nio.file.Files.newInputStream(configFile)) {
            properties.load(is);
        }
        return properties;
    }

    private void reportValidateConfig() {
        final boolean httpEnabled = config.getHttpServerConfiguration().isEnabled();
        final boolean httpReadOnly = config.getHttpServerConfiguration().getHttpContextConfiguration().readOnlySecurityContext();
        final String httpReadOnlyHint = httpEnabled && httpReadOnly ? " [read-only]" : "";
        final boolean pgEnabled = config.getPGWireConfiguration().isEnabled();
        final boolean pgReadOnly = config.getPGWireConfiguration().readOnlySecurityContext();
        final String pgReadOnlyHint = pgEnabled && pgReadOnly ? " [read-only]" : "";
        final CairoConfiguration cairoConfig = config.getCairoConfiguration();
        log.advisoryW().$("Config:").$();
        log.advisoryW().$(" - http.enabled : ").$(httpEnabled).$(httpReadOnlyHint).$();
        log.advisoryW().$(" - tcp.enabled  : ").$(config.getLineTcpReceiverConfiguration().isEnabled()).$();
        log.advisoryW().$(" - pg.enabled   : ").$(pgEnabled).$(pgReadOnlyHint).$();
        log.advisoryW().$(" - open database [id=").$(cairoConfig.getDatabaseIdLo()).$('.').$(cairoConfig.getDatabaseIdHi()).I$();
        try (Path path = new Path()) {
            verifyFileSystem(path, cairoConfig.getRoot(), "db");
            verifyFileSystem(path, cairoConfig.getBackupRoot(), "backup");
            verifyFileSystem(path, cairoConfig.getSnapshotRoot(), "snapshot");
            verifyFileSystem(path, cairoConfig.getSqlCopyInputRoot(), "sql copy input");
            verifyFileSystem(path, cairoConfig.getSqlCopyInputWorkRoot(), "sql copy input worker");
            verifyFileOpts(path, cairoConfig);
        }
        if (JitUtil.isJitSupported()) {
            final int jitMode = cairoConfig.getSqlJitMode();
            switch (jitMode) {
                case SqlJitMode.JIT_MODE_ENABLED:
                    log.advisoryW().$(" - SQL JIT compiler mode: on").$();
                    break;
                case SqlJitMode.JIT_MODE_FORCE_SCALAR:
                    log.advisoryW().$(" - SQL JIT compiler mode: scalar").$();
                    break;
                case SqlJitMode.JIT_MODE_DISABLED:
                    log.advisoryW().$(" - SQL JIT compiler mode: off").$();
                    break;
                default:
                    log.errorW().$(" - Unknown SQL JIT compiler mode: ").$(jitMode).$();
                    break;
            }
        }
    }

    private void verifyFileSystem(Path path, CharSequence rootDir, String kind) {
        if (rootDir == null) {
            log.advisoryW().$(" - ").$(kind).$(" root: NOT SET").$();
            return;
        }
        path.of(rootDir).$();
        // path will contain file system name
        long fsStatus = Files.getFileSystemStatus(path);
        path.seekZ();
        LogRecord rec = log.advisoryW().$(" - ").$(kind).$(" root: [path=").$(rootDir).$(", magic=0x");
        if (fsStatus < 0) {
            rec.$hex(-fsStatus).$("] -> SUPPORTED").$();
        } else {
            rec.$hex(fsStatus).$("] -> UNSUPPORTED (SYSTEM COULD BE UNSTABLE)").$();
        }
    }

    public static class BootstrapException extends RuntimeException {
        public BootstrapException(String message) {
            super(message);
        }

        public BootstrapException(Throwable thr) {
            super(thr);
        }
    }

    static {
        if (Os.type == Os._32Bit) {
            throw new Error("QuestDB requires 64-bit JVM");
        }
    }
}
