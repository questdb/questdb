/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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
import io.questdb.cairo.wal.WalUtils;
import io.questdb.recovery.BoundedTxnReader;
import io.questdb.recovery.ConsoleRenderer;
import io.questdb.recovery.RecoverySession;
import io.questdb.recovery.TableDiscoveryService;
import io.questdb.recovery.TxnStateService;
import io.questdb.std.FilesFacade;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.str.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class RecoveryMain {
    private static final int EXIT_BAD_ARGS = 2;
    private static final int EXIT_LOCKED = 3;
    private static final int EXIT_IO_ERROR = 4;

    public static void main(String[] args) {
        int code = run(args, System.in, System.out, System.err, FilesFacadeImpl.INSTANCE);
        if (code != 0) {
            System.exit(code);
        }
    }

    public static int run(String[] args, InputStream in, PrintStream out, PrintStream err, FilesFacade ff) {
        final CliConfig cliConfig = parseArgs(args, err);
        if (cliConfig == null) {
            printUsage(err);
            return EXIT_BAD_ARGS;
        }

        if (cliConfig.help) {
            printUsage(out);
            return 0;
        }

        final String dbRoot = resolveDbRoot(cliConfig.installRoot, cliConfig.explicitDbRoot, err);
        if (!ff.exists(Path.getThreadLocal(dbRoot).$())) {
            err.println("db root does not exist: " + dbRoot);
            return EXIT_IO_ERROR;
        }

        long lockFd = -1;
        try {
            final LockResult lockResult = tryAcquireSafetyLock(ff, dbRoot, out);
            if (lockResult.activeLockHeld) {
                err.println("database appears to be running (active lock holder detected): " + lockResult.lockPath);
                err.println("stop the running database and retry recovery mode");
                return EXIT_LOCKED;
            }
            lockFd = lockResult.lockFd;

            RecoverySession session = new RecoverySession(
                    dbRoot,
                    new TableDiscoveryService(ff),
                    new TxnStateService(new BoundedTxnReader(ff)),
                    new ConsoleRenderer()
            );
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                return session.run(reader, out, err);
            }
        } catch (IOException e) {
            err.println("recovery mode failed: " + e.getMessage());
            return EXIT_IO_ERROR;
        } finally {
            if (lockFd > -1) {
                ff.close(lockFd);
            }
        }
    }

    private static String resolveDbRoot(String installRoot, String explicitDbRoot, PrintStream err) {
        if (explicitDbRoot != null) {
            return explicitDbRoot;
        }

        final java.nio.file.Path base = Paths.get(installRoot);
        final java.nio.file.Path confPath = base.resolve("conf").resolve("server.conf");
        final java.nio.file.Path fallback = base.resolve("db");

        if (!Files.exists(confPath)) {
            return fallback.toString();
        }

        final Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(confPath)) {
            properties.load(in);
        } catch (IOException e) {
            err.println("warning: cannot read server.conf, falling back to " + fallback + " [" + e.getMessage() + ']');
            return fallback.toString();
        }

        final String cairoRoot = properties.getProperty("cairo.root");
        if (cairoRoot == null || cairoRoot.trim().isEmpty()) {
            return fallback.toString();
        }

        final java.nio.file.Path configured = Paths.get(cairoRoot.trim());
        return configured.isAbsolute() ? configured.toString() : base.resolve(configured).normalize().toString();
    }

    private static LockResult tryAcquireSafetyLock(FilesFacade ff, CharSequence dbRoot, PrintStream out) {
        try (Path lockPath = new Path()) {
            lockPath.of(dbRoot).concat(WalUtils.TABLE_REGISTRY_NAME_FILE).put(".lock");
            boolean existedBefore = ff.exists(lockPath.$());

            final long fd = ff.openRWNoCache(lockPath.$(), CairoConfiguration.O_NONE);
            if (fd < 0) {
                out.println("warning: lock unavailable, continuing without lock [path=" + lockPath + ", errno=" + ff.errno() + ']');
                return new LockResult(lockPath.toString(), -1, false);
            }

            if (ff.lock(fd) != 0) {
                ff.close(fd);
                if (existedBefore) {
                    return new LockResult(lockPath.toString(), -1, true);
                }
                out.println("warning: lock file could not be locked, continuing [path=" + lockPath + ", errno=" + ff.errno() + ']');
                return new LockResult(lockPath.toString(), -1, false);
            }
            return new LockResult(lockPath.toString(), fd, false);
        }
    }

    private static void printUsage(PrintStream out) {
        out.println("usage: RecoveryMain -d <installRoot> [--db <dbRoot>] [--help]");
    }

    private static CliConfig parseArgs(String[] args, PrintStream err) {
        String installRoot = null;
        String explicitDbRoot = null;
        boolean help = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--help":
                case "-h":
                    help = true;
                    break;
                case "-d":
                    if (i + 1 >= args.length) {
                        err.println("expected value after -d");
                        return null;
                    }
                    installRoot = args[++i];
                    break;
                case "--db":
                    if (i + 1 >= args.length) {
                        err.println("expected value after --db");
                        return null;
                    }
                    explicitDbRoot = args[++i];
                    break;
                default:
                    err.println("unexpected argument: " + arg);
                    return null;
            }
        }

        if (!help && installRoot == null) {
            err.println("-d <installRoot> is required");
            return null;
        }

        return new CliConfig(help, installRoot, explicitDbRoot);
    }

    private static class CliConfig {
        private final String explicitDbRoot;
        private final boolean help;
        private final String installRoot;

        private CliConfig(boolean help, String installRoot, String explicitDbRoot) {
            this.help = help;
            this.installRoot = installRoot;
            this.explicitDbRoot = explicitDbRoot;
        }
    }

    private static class LockResult {
        private final boolean activeLockHeld;
        private final long lockFd;
        private final String lockPath;

        private LockResult(String lockPath, long lockFd, boolean activeLockHeld) {
            this.lockPath = lockPath;
            this.lockFd = lockFd;
            this.activeLockHeld = activeLockHeld;
        }
    }
}
