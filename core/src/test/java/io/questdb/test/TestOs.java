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

package io.questdb.test;

import io.questdb.std.Os;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.TimeUnit;

public class TestOs {
    public static void init() {
    }

    private static FileTime getLastModifiedTime(Path absoluteDevReleasePath) {
        try {
            return Files.getLastModifiedTime(absoluteDevReleasePath);
        } catch (IOException e) {
            return FileTime.from(0, TimeUnit.MILLISECONDS);
        }
    }

    // This is copy of main Os class to load test jni library
    static {
        String outputLibExt;
        if (Os.type == Os.LINUX || Os.type == Os.FREEBSD) {
            outputLibExt = ".so";
        } else if (Os.type == Os.DARWIN) {
            outputLibExt = ".dylib";
        } else if (Os.type == Os.WINDOWS) {
            outputLibExt = ".dll";
        } else {
            throw new Error("Unsupported OS: " + Os.name);
        }

        final String rustLibName;
        if (Os.type == Os.WINDOWS) {
            rustLibName = "qdbsqllogictest" + outputLibExt;
        } else {
            rustLibName = "libqdbsqllogictest" + outputLibExt;
        }

        URL resource = TestOs.class.getResource("/io/questdb/bin/" + Os.name + '-' + Os.archName + '/' + rustLibName);
        if (resource != null) {
            String absolutePathPreCompiled;
            try {
                absolutePathPreCompiled = resource.toURI().getPath();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            if (Os.isWindows() && absolutePathPreCompiled.charAt(0) == '/') {
                // Remove forward /
                absolutePathPreCompiled = absolutePathPreCompiled.substring(1);
            }
            String sourcesPath = absolutePathPreCompiled.substring(0, absolutePathPreCompiled.indexOf("/target/"));
            Path absoluteDevReleasePath = Paths.get(sourcesPath + "/rust/qdb-sqllogictest/target/release/" + rustLibName).toAbsolutePath();
            Path absoluteDevDebugPath = Paths.get(sourcesPath + "/rust/qdb-sqllogictest/target/debug/" + rustLibName).toAbsolutePath();
            Path absolutePrdPath = Paths.get(absolutePathPreCompiled);

            FileTime tsDevRel = getLastModifiedTime(absoluteDevReleasePath);
            FileTime tsDevDeb = getLastModifiedTime(absoluteDevDebugPath);
            FileTime tsPrd = getLastModifiedTime(absolutePrdPath);

            Path rustLibPath;
            if (tsDevRel.compareTo(tsPrd) > 0 && tsDevRel.compareTo(tsDevDeb) > 0) {
                System.err.println("Loading DEV release sqllogictest library: " + absoluteDevReleasePath);
                rustLibPath = absoluteDevReleasePath;
            } else if (tsDevDeb.compareTo(tsPrd) > 0) {
                System.err.println("Loading DEV debug sqllogictest library: " + absoluteDevDebugPath);
                rustLibPath = absoluteDevDebugPath;
            } else {
                rustLibPath = absolutePrdPath;
            }

            try (InputStream is = Files.newInputStream(rustLibPath)) {
                Os.loadLib(rustLibPath.toString(), is);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
