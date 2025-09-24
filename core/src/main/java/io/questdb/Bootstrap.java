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

package io.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.SqlJitMode;
import io.questdb.cairo.TableUtils;
import io.questdb.cutlass.http.HttpFullFatServerConfiguration;
import io.questdb.jit.JitUtil;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.log.LogLevel;
import io.questdb.log.LogRecord;
import io.questdb.network.Net;
import io.questdb.std.CharSequenceObjHashMap;
import io.questdb.std.Chars;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.datetime.MicrosecondClock;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.datetime.millitime.Dates;
import io.questdb.std.str.DirectUtf8StringZ;
import io.questdb.std.str.Path;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import sun.misc.Signal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static io.questdb.griffin.engine.functions.str.SizePrettyFunctionFactory.toSizePretty;

public class Bootstrap {

    public static final String CONFIG_FILE = "/server.conf";
    public static final String CONTAINERIZED_SYSTEM_PROPERTY = "containerized";
    public static final String SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION = "--use-default-log-factory-configuration";
    private static final String LOG_NAME = "server-main";
    private static final String PUBLIC_VERSION_TXT = "version.txt";
    private static final String PUBLIC_ZIP = "/io/questdb/site/public.zip";
    private final String banner;
    private final BuildInformation buildInformation;
    private final ServerConfiguration config;
    private final Log log;
    private final MicrosecondClock microsecondClock;
    private final String rootDirectory;

    public Bootstrap(String... args) {
        this(new PropBootstrapConfiguration(), args);
    }

    public Bootstrap(BootstrapConfiguration bootstrapConfiguration, String... args) {
        if (args.length < 2) {
            throw new BootstrapException("Root directory name expected (-d <root-path>)");
        }
        Os.init();
        banner = bootstrapConfiguration.getBanner();
        microsecondClock = bootstrapConfiguration.getMicrosecondClock();
        buildInformation = new BuildInformationHolder(bootstrapConfiguration.getClass());

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

        // before we set up the logger, we need to copy the conf file
        byte[] buffer = new byte[1024 * 1024];
        try {
            copyLogConfResource(buffer);
        } catch (IOException e) {
            throw new BootstrapException("Could not extract log configuration file");
        }

        // setup logger
        // note: this call must be made before any Log init.
        if (argsMap.get(SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION) == null) {
            LogFactory.configureRootDir(rootDirectory);
        }
        log = LogFactory.getLog(LOG_NAME);

        try {
            copyResource(rootDirectory, false, buffer, "import/readme.txt", log);
            copyResource(rootDirectory, false, buffer, "import/trades.parquet", log);
        } catch (IOException e) {
            throw new BootstrapException("Could not create the default import directory");
        }

        // report copyright and architecture
        log.advisoryW()
                .$(buildInformation.getSwName()).$(' ').$(buildInformation.getSwVersion())
                .$(". Copyright (C) 2014-").$(Dates.getYear(System.currentTimeMillis()))
                .$(", all rights reserved.").$();
        boolean isOsSupported = true;
        switch (Os.type) {
            case Os.WINDOWS:
            case Os.DARWIN:
            case Os.LINUX:
            case Os.FREEBSD:
                break;
            default:
                isOsSupported = false;
                break;
        }
        StringBuilder sb = new StringBuilder(Vect.getSupportedInstructionSetName());
        sb.setLength(sb.length() - 1); // remove ending ']'
        sb.append(", ").append(System.getProperty("sun.arch.data.model")).append(" bits");
        sb.append(", ").append(Runtime.getRuntime().availableProcessors()).append(" processors");
        if (isOsSupported) {
            log.advisoryW().$(Os.name).$('-').$(Os.archName).$(sb).I$();
        } else {
            log.critical().$("!!UNSUPPORTED!!").$(System.getProperty("os.name")).$('-').$(Os.archName).$(sb).I$();
        }

        verifyFileLimits();
        try {
            if (bootstrapConfiguration.useSite()) {
                // site
                extractSite();
            }

            final ServerConfiguration configuration = bootstrapConfiguration.getServerConfiguration(this);
            if (configuration == null) {
                // /server.conf properties
                final Properties properties = loadProperties();
                final FilesFacade ffOverride = bootstrapConfiguration.getFilesFacade();
                if (ffOverride == null) {
                    config = new DynamicPropServerConfiguration(
                            rootDirectory,
                            properties,
                            bootstrapConfiguration.getEnv(),
                            log,
                            buildInformation
                    );
                } else {
                    config = new DynamicPropServerConfiguration(
                            rootDirectory,
                            properties,
                            bootstrapConfiguration.getEnv(),
                            log,
                            buildInformation,
                            ffOverride,
                            MicrosecondClockImpl.INSTANCE,
                            (configuration1, engine, freeOnExit) -> DefaultFactoryProvider.INSTANCE,
                            true
                    );
                }
            } else {
                config = configuration;
            }

            Files.FS_CACHE_ENABLED = config.getCairoConfiguration().getFileDescriptorCacheEnabled();
            LogLevel.init(config.getCairoConfiguration());
            if (LogLevel.TIMESTAMP_TIMEZONE != null) {
                log.infoW().$("changing logger timezone [from=`UTC`, to=`").$(LogLevel.TIMESTAMP_TIMEZONE).$('`').I$();
            }
            reportValidateConfig();
            reportCrashFiles(config.getCairoConfiguration(), log);
        } catch (BootstrapException e) {
            throw e;
        } catch (ServerConfigurationException e) {
            throw new BootstrapException(e);
        } catch (Throwable e) {
            log.errorW().$(e).$();
            throw new BootstrapException(e);
        }
        if (!config.getMetricsConfiguration().isEnabled()) {
            log.advisoryW().$("Metrics are disabled, health check endpoint will not consider unhandled errors").$();
        }
        Unsafe.setRssMemLimit(config.getMemoryConfiguration().getResolvedRamUsageLimitBytes());
    }

    public static String[] getServerMainArgs(CharSequence root) {
        return new String[]{
                "-d",
                Chars.toString(root),
                SWITCH_USE_DEFAULT_LOG_FACTORY_CONFIGURATION
        };
    }

    public static CharSequenceObjHashMap<String> processArgs(String... args) {
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

    public static void reportCrashFiles(CairoConfiguration cairoConfiguration, Log log) {
        final CharSequence dbRoot = cairoConfiguration.getDbRoot();
        final FilesFacade ff = cairoConfiguration.getFilesFacade();
        final int maxFiles = cairoConfiguration.getMaxCrashFiles();
        DirectUtf8StringZ name = new DirectUtf8StringZ();
        try (Path path = new Path(); Path other = new Path()) {
            path.of(dbRoot).slash();
            other.of(dbRoot).slash();

            int plen = path.size();
            AtomicInteger counter = new AtomicInteger(0);
            FilesFacadeImpl.INSTANCE.iterateDir(path.$(), (pUtf8NameZ, type) -> {
                if (Files.notDots(pUtf8NameZ)) {
                    name.of(pUtf8NameZ);
                    if (Utf8s.startsWithAscii(name, cairoConfiguration.getOGCrashFilePrefix()) && type == Files.DT_FILE) {
                        path.trimTo(plen).concat(pUtf8NameZ).$();
                        boolean shouldRename = false;
                        do {
                            other.trimTo(plen).concat(cairoConfiguration.getArchivedCrashFilePrefix()).put(counter.getAndIncrement()).put(".log").$();
                            if (!ff.exists(other.$())) {
                                shouldRename = counter.get() <= maxFiles;
                                break;
                            }
                        } while (counter.get() < maxFiles);
                        if (shouldRename && ff.rename(path.$(), other.$()) == 0) {
                            log.critical().$("found crash file [path=").$(other).I$();
                        } else {
                            log.critical().$("could not rename crash file [path=").$(path).$(", errno=").$(ff.errno()).$(", index=").$(counter.get()).$(", max=").$(maxFiles).I$();
                        }
                    }
                }
            });
        }
    }

    public void extractSite() throws IOException {
        final byte[] buffer = new byte[1024 * 1024];
        final URL resource = ServerMain.class.getResource(PUBLIC_ZIP);
        if (resource == null) {
            log.infoW().$("Web Console build [").$(PUBLIC_ZIP).$("] not found").$();
            extractConfDir(buffer);
        } else {
            long thisVersion = resource.openConnection().getLastModified();
            final String publicDir = rootDirectory + Files.SEPARATOR + "public";

            boolean extracted = false;
            final String oldSwVersion = getPublicVersion(publicDir);
            final CharSequence newSwVersion = buildInformation.getSwVersion();
            if (oldSwVersion == null) {
                if (thisVersion != 0) {
                    extractSite0(publicDir, buffer, Long.toString(thisVersion));
                } else {
                    extractSite0(publicDir, buffer, Chars.toString(newSwVersion));
                }
                extracted = true;
            } else {
                // This is a hack to deal with RT package problem
                // in this package "thisVersion" is always 0, and we need to fall back
                // to the database version.
                if (thisVersion == 0) {
                    if (!Chars.equals(oldSwVersion, newSwVersion)) {
                        extractSite0(publicDir, buffer, Chars.toString(newSwVersion));
                        extracted = true;
                    }
                } else {
                    // it is possible that old version is the database version
                    // which means user might have switched from RT distribution to no-JVM on the same data dir
                    // in this case we might fail to parse the version string
                    try {
                        final long oldVersion = Numbers.parseLong(oldSwVersion);
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
    }

    public BuildInformation getBuildInformation() {
        return buildInformation;
    }

    public ServerConfiguration getConfiguration() {
        return config;
    }

    public Log getLog() {
        return log;
    }

    public MicrosecondClock getMicrosecondClock() {
        return microsecondClock;
    }

    public String getRootDirectory() {
        return rootDirectory;
    }

    @NotNull
    public Properties loadProperties() throws IOException {
        final Properties properties = new Properties();
        java.nio.file.Path configFile = Paths.get(rootDirectory, PropServerConfiguration.CONFIG_DIRECTORY, CONFIG_FILE);
        log.advisoryW().$("Server config: ").$(configFile).$();
        if (!java.nio.file.Files.exists(configFile)) {
            throw new BootstrapException("Server configuration file does not exist! " + configFile, true);
        }
        if (!java.nio.file.Files.isReadable(configFile)) {
            throw new BootstrapException("Server configuration file exists, but is not readable! Check file permissions. " + configFile, true);
        }
        try (InputStream is = java.nio.file.Files.newInputStream(configFile)) {
            properties.load(is);
        }
        return properties;
    }

    public CairoEngine newCairoEngine() {
        return new CairoEngine(getConfiguration().getCairoConfiguration());
    }

    private static void copyInputStream(boolean force, byte[] buffer, File out, InputStream is, Log log) throws IOException {
        final boolean exists = out.exists();
        if (force || !exists) {
            File dir = out.getParentFile();
            if (!dir.exists() && !dir.mkdirs()) {
                if (log != null) {
                    log.errorW().$("could not create directory [path=").$(dir).I$();
                }
                return;
            }
            try (FileOutputStream fos = new FileOutputStream(out)) {
                int n;
                while ((n = is.read(buffer, 0, buffer.length)) > 0) {
                    fos.write(buffer, 0, n);
                }
            }
            if (log != null) {
                log.infoW().$("extracted [path=").$(out).I$();
            }
            return;
        }
        if (log != null) {
            log.debugW().$("skipped [path=").$(out).I$();
        }
    }

    private static void copyResource(String dir, boolean force, byte[] buffer, String res, String dest, Log log) throws IOException {
        File out = new File(dir, dest);
        try (InputStream is = ServerMain.class.getResourceAsStream("/io/questdb/site/" + res)) {
            if (is != null) {
                copyInputStream(force, buffer, out, is, log);
            }
        }
    }

    private static void copyResource(String dir, boolean force, byte[] buffer, String res, Log log) throws IOException {
        copyResource(dir, force, buffer, res, res, log);
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

    private static void padToNextCol(StringBuilder sb, int headerWidth) {
        int colWidth = 32;
        // Insert at least one space between columns
        sb.append("  ");
        for (int i = headerWidth + 2; i < colWidth; i++) {
            sb.append(' ');
        }
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

    private static void verifyFileOpts(Path path, CairoConfiguration cairoConfiguration) {
        final FilesFacade ff = cairoConfiguration.getFilesFacade();
        path.of(cairoConfiguration.getDbRoot()).concat("_verify_").put(cairoConfiguration.getRandom().nextPositiveInt()).put(".d").$();
        long fd = ff.openRW(path.$(), cairoConfiguration.getWriterFileOpenOpts());
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
        ff.remove(path.$());
    }

    private void copyLogConfResource(byte[] buffer) throws IOException {
        if (Chars.equalsIgnoreCaseNc("false", System.getProperty(CONTAINERIZED_SYSTEM_PROPERTY))) {
            copyResource(rootDirectory, false, buffer, "conf/non_containerized_log.conf", "conf/log.conf", null);
        } else {
            copyResource(rootDirectory, false, buffer, "conf/log.conf", null);
        }
    }

    private void createHelloFile(String helloMsg) {
        final File helloFile = new File(rootDirectory, "hello.txt");
        final File growingFile = new File(rootDirectory, helloFile.getName() + ".tmp");
        try (Writer w = new FileWriter(growingFile)) {
            w.write(helloMsg);
        } catch (IOException e) {
            log.infoW().$("Failed to create ").$(growingFile.getAbsolutePath()).$();
        }
        if (!growingFile.renameTo(helloFile)) {
            log.infoW().$("Failed to rename ").$(growingFile.getAbsolutePath()).$(" to ").$(helloFile.getName()).$();
        }
        helloFile.deleteOnExit();
    }

    private void extractConfDir(byte[] buffer) throws IOException {
        copyResource(rootDirectory, false, buffer, "conf/date.formats", log);
        try {
            copyResource(rootDirectory, true, buffer, "conf/mime.types", log);
        } catch (IOException exception) {
            // conf can be read-only, this is not critical
            if (exception.getMessage() == null || (!exception.getMessage().contains("Read-only file system") && !exception.getMessage().contains("Permission denied"))) {
                throw exception;
            }
        }
        copyResource(rootDirectory, false, buffer, "conf/server.conf", log);
        copyLogConfResource(buffer);
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
        extractConfDir(buffer);
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
        boolean enabled = config.getLineTcpReceiverConfiguration().isEnabled();
        log.advisoryW().$(" - tcp.enabled  : ").$(enabled).$();
        log.advisoryW().$(" - pg.enabled   : ").$(pgEnabled).$(pgReadOnlyHint).$();
        if (cairoConfig != null) {
            log.advisoryW().$(" - attach partition suffix: ").$(cairoConfig.getAttachPartitionSuffix()).$();
            log.advisoryW().$(" - open database [").$uuid(cairoConfig.getDatabaseIdLo(), cairoConfig.getDatabaseIdHi()).I$();
            if (cairoConfig.isReadOnlyInstance()) {
                log.advisoryW().$(" - THIS IS READ ONLY INSTANCE").$();
            }
            try (Path path = new Path()) {
                verifyFileSystem(path, cairoConfig.getDbRoot(), "db", true, true);
                verifyFileSystem(path, cairoConfig.getBackupRoot(), "backup", false, false);
                verifyFileSystem(path, cairoConfig.getCheckpointRoot(), TableUtils.CHECKPOINT_DIRECTORY, true, false);
                verifyFileSystem(path, cairoConfig.getLegacyCheckpointRoot(), TableUtils.LEGACY_CHECKPOINT_DIRECTORY, false, false);
                verifyFileSystem(path, cairoConfig.getSqlCopyInputRoot(), "sql copy input", false, false);
                verifyFileSystem(path, cairoConfig.getSqlCopyInputWorkRoot(), "sql copy input worker", true, false);
                verifyFileOpts(path, cairoConfig);
                cairoConfig.getVolumeDefinitions().forEach((alias, volumePath) -> verifyFileSystem(path, volumePath, "create table allowed volume [" + alias + ']', true, false));
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

        MemoryConfiguration ramConfig = config.getMemoryConfiguration();
        long ramUsageLimitBytes = ramConfig.getRamUsageLimitBytes();
        long ramUsageLimitPercent = ramConfig.getRamUsageLimitPercent();
        long effectiveRamUsageLimit = ramConfig.getResolvedRamUsageLimitBytes();
        log.advisoryW().$(" - configured ram.usage.limit.bytes: ")
                .$(ramUsageLimitBytes != 0 ? toSizePretty(ramUsageLimitBytes) : "0 (no limit)").$();
        log.advisoryW().$(" - configured ram.usage.limit.percent: ")
                .$(ramUsageLimitPercent != 0 ? ramUsageLimitPercent : "0 (no limit)").$();
        log.advisoryW().$(" - system RAM: ").$(toSizePretty(ramConfig.getTotalSystemMemory())).$();
        log.advisoryW().$(" - resolved RAM usage limit: ")
                .$(effectiveRamUsageLimit != 0 ? toSizePretty(effectiveRamUsageLimit) : "0 (no limit)").$();
    }

    private void verifyFileLimits() {
        boolean insufficientLimits = false;

        final long fileLimit = Files.getFileLimit();
        if (fileLimit < 0) {
            log.error().$("could not read fs.file-max [errno=").$(Os.errno()).I$();
        }
        if (fileLimit > 0) {
            if (fileLimit <= Files.DEFAULT_FILE_LIMIT) {
                insufficientLimits = true;
                log.advisoryW().$("fs.file-max limit is too low [limit=").$(fileLimit).$("] (SYSTEM COULD BE UNSTABLE)").$();
            } else {
                log.advisoryW().$("fs.file-max checked [limit=").$(fileLimit).I$();
            }
        }

        final long mapCountLimit = Files.getMapCountLimit();
        if (mapCountLimit < 0) {
            log.error().$("could not read vm.max_map_count [errno=").$(Os.errno()).I$();
        }
        if (mapCountLimit > 0) {
            if (mapCountLimit <= Files.DEFAULT_MAP_COUNT_LIMIT) {
                insufficientLimits = true;
                log.advisoryW().$("vm.max_map_count limit is too low [limit=").$(mapCountLimit).$("] (SYSTEM COULD BE UNSTABLE)").$();
            } else {
                log.advisoryW().$("vm.max_map_count checked [limit=").$(mapCountLimit).I$();
            }
        }

        if (insufficientLimits) {
            log.advisoryW().$("make sure to increase fs.file-max and vm.max_map_count limits:\n" +
                    "https://questdb.io/docs/deployment/capacity-planning/#os-configuration").$();
        }
    }

    private void verifyFileSystem(Path path, CharSequence rootDir, String kind, boolean failOnNfs, boolean logUnstable) {
        if (rootDir == null) {
            log.advisoryW().$(" - ").$(kind).$(" root: NOT SET").$();
            return;
        }
        path.of(rootDir);

        // path will contain file system name
        if (Files.exists(path.$())) {
            final long fsStatus = Files.getFileSystemStatus(path.$());
            path.seekZ();
            LogRecord rec = log.advisoryW().$(" - ").$(kind).$(" root: [path=").$(rootDir).$(", magic=0x");
            if (fsStatus < 0 || (fsStatus == 0 && Os.type == Os.DARWIN && Os.arch == Os.ARCH_AARCH64)) {
                rec.$hex(-fsStatus).$(", fs=").$(path).$("] -> SUPPORTED").$();
            } else {
                rec.$hex(fsStatus).$(", fs=").$(path);
                if (logUnstable) {
                    rec.$("] -> UNSUPPORTED (SYSTEM COULD BE UNSTABLE)").$();
                } else {
                    rec.$("] -> UNSUPPORTED").$();
                }
            }

            if (failOnNfs && fsStatus == Files.NFS_MAGIC) {
                throw new BootstrapException("Error: Unsupported Filesystem Detected. " + Misc.EOL
                        + "QuestDB cannot start because the '" + rootDirectory + "' is located on an NFS filesystem, "
                        + "which is not supported. Please relocate your '" + kind + " root' to a supported filesystem to continue. " + Misc.EOL
                        + "For a list of supported filesystems and further guidance, please visit: https://questdb.io/docs/deployment/capacity-planning/#supported-filesystems "
                        + "[path=" + rootDir + ", kind=" + kind + ", fs=NFS]", true);
            }
        } else {
            log.info().$(" - ").$(kind).$(" root: [path=").$(rootDir).$("] -> NOT FOUND").$();
        }
    }

    void logBannerAndEndpoints(String schema) {
        final boolean ilpEnabled = config.getHttpServerConfiguration().getLineHttpProcessorConfiguration().isEnabled();
        final String indent = "    ";
        final StringBuilder sb = new StringBuilder();
        sb.append('\n').append(banner);
        if (config.getHttpServerConfiguration().isEnabled()) {
            sb.append(indent);
            String col1Header = "Web Console URL";
            sb.append(col1Header);
            if (ilpEnabled) {
                padToNextCol(sb, col1Header.length());
                sb.append("ILP Client Connection String");
            }
            sb.append("\n\n");
            final HttpFullFatServerConfiguration httpConf = config.getHttpServerConfiguration();
            final int bindIP = httpConf.getBindIPv4Address();
            final int bindPort = httpConf.getBindPort();
            final String contextPathWebConsole = httpConf.getContextPathWebConsole();
            if (bindIP == 0) {
                try {
                    for (Enumeration<NetworkInterface> ni = NetworkInterface.getNetworkInterfaces(); ni.hasMoreElements(); ) {
                        for (Enumeration<InetAddress> addr = ni.nextElement().getInetAddresses(); addr.hasMoreElements(); ) {
                            InetAddress inetAddress = addr.nextElement();
                            if (inetAddress instanceof Inet4Address) {
                                String leftCol = schema + "://" + inetAddress.getHostAddress() + ':' + bindPort + contextPathWebConsole;
                                sb.append(indent).append(leftCol);
                                if (ilpEnabled) {
                                    padToNextCol(sb, leftCol.length());
                                    sb.append(schema).append("::addr=").append(inetAddress.getHostAddress()).append(':').append(bindPort).append(';');
                                }
                                sb.append('\n');
                            }
                        }
                    }
                } catch (SocketException se) {
                    throw new Bootstrap.BootstrapException("Cannot access network interfaces");
                }
                sb.append('\n');
            } else {
                sb.append('\t').append(schema);
                final Utf8StringSink sink = new Utf8StringSink();
                Net.appendIP4(sink, bindIP);
                sb.append(sink).append(':').append(bindPort).append('\n');
            }
            if (!ilpEnabled) {
                sb.append("InfluxDB Line Protocol is disabled for HTTP. Enable in server.conf: line.http.enabled=true\n");
            }
        } else {
            sb.append("HTTP server is disabled. Enable in server.conf: http.enabled=true\n");
        }
        sb.append("QuestDB configuration files are in ");
        try {
            sb.append(new File(rootDirectory, "conf").getCanonicalPath());
        } catch (IOException e) {
            sb.append(new File(rootDirectory, "conf").getAbsolutePath());
        }
        sb.append("\n\n");
        final String helloMsg = sb.toString();
        log.infoW().$(helloMsg).$();
        createHelloFile(helloMsg);
    }

    public static class BootstrapException extends RuntimeException {
        private boolean silentStacktrace = false;

        public BootstrapException(String message) {
            this(message, false);
        }

        public BootstrapException(String message, boolean silentStacktrace) {
            super(message);
            this.silentStacktrace = silentStacktrace;
        }

        public BootstrapException(Throwable thr) {
            super(thr);
        }

        public boolean isSilentStacktrace() {
            return silentStacktrace;
        }
    }

    static {
        if (Os.type == Os._32Bit) {
            throw new Error("QuestDB requires 64-bit JVM");
        }
    }
}
