/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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

import com.questdb.cairo.CairoEngine;
import com.questdb.cutlass.http.HttpRequestProcessor;
import com.questdb.cutlass.http.HttpRequestProcessorFactory;
import com.questdb.cutlass.http.HttpServer;
import com.questdb.cutlass.http.HttpServerConfiguration;
import com.questdb.cutlass.http.processors.JsonQueryProcessor;
import com.questdb.cutlass.http.processors.StaticContentProcessor;
import com.questdb.cutlass.http.processors.TableStatusCheckProcessor;
import com.questdb.cutlass.http.processors.TextImportProcessor;
import com.questdb.std.CharSequenceObjHashMap;
import com.questdb.std.Os;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class ServerMain {
    public ServerMain(String[] args) throws Exception {
        System.err.printf("QuestDB server %s%nCopyright (C) Appsicle 2014-2019, all rights reserved.%n%n", getVersion());
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

        // todo: load path to data directory from configuration
        final PropServerConfiguration configuration = new PropServerConfiguration(rootDirectory, properties);
        final CairoEngine cairoEngine = new CairoEngine(configuration.getCairoConfiguration());

        final HttpServer httpServer = new HttpServer(configuration.getHttpServerConfiguration());
        httpServer.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/exec";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new JsonQueryProcessor(configuration.getHttpServerConfiguration().getJsonQueryProcessorConfiguration(), cairoEngine);
            }
        });

        httpServer.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/imp";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TextImportProcessor(configuration.getHttpServerConfiguration().getTextImportProcessorConfiguration(), cairoEngine);
            }
        });

        httpServer.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return "/chk";
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new TableStatusCheckProcessor(cairoEngine);
            }
        });

        httpServer.bind(new HttpRequestProcessorFactory() {
            @Override
            public String getUrl() {
                return HttpServerConfiguration.DEFAULT_PROCESSOR_URL;
            }

            @Override
            public HttpRequestProcessor newInstance() {
                return new StaticContentProcessor(configuration.getHttpServerConfiguration().getStaticContentProcessorConfiguration());
            }
        });

        httpServer.start();
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
                    com.questdb.store.Files.delete(pub);
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
            System.out.println("Site content is ready");
            if (fs != null) {
                fs.close();
            }
        }
    }

    private String getVersion() throws IOException {
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
}
