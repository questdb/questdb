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

package io.questdb.std;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class LibcDetector {
    public enum LibcType {
        UNKNOWN, GLIBC, MUSL
    }

    public static LibcType detectLibc() {
        // Try multiple methods in order of reliability

        // Method 1: Check /proc/self/maps
        LibcType result = detectFromMaps();
        if (result != LibcType.UNKNOWN) {
            return result;
        }

        // Method 2: Use ldd command
        result = detectViaLdd();
        if (result != LibcType.UNKNOWN) {
            return result;
        }

        // Method 3: Use getconf command
        result = detectViaGetconf();
        if (result != LibcType.UNKNOWN) {
            return result;
        }

        // Method 4: Check system properties for hints
        result = detectFromSystemProperties();
        return result;
    }

    private static LibcType detectFromMaps() {
        try {
            Path mapsPath = Paths.get("/proc/self/maps");
            if (!Files.exists(mapsPath)) {
                return LibcType.UNKNOWN;
            }

            List<String> lines = Files.readAllLines(mapsPath);
            for (String line : lines) {
                if (line.contains("libc.so.6")) {
                    return LibcType.GLIBC;
                } else if (line.contains("libc.musl") || line.contains("ld-musl")) {
                    return LibcType.MUSL;
                }
            }
        } catch (IOException e) {
            // Ignore
        }
        return LibcType.UNKNOWN;
    }

    private static LibcType detectViaLdd() {
        try {
            ProcessBuilder pb = new ProcessBuilder("ldd", "--version");
            Process process = pb.start();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream())
            );

            String line;
            while ((line = reader.readLine()) != null) {
                String lowerLine = line.toLowerCase();
                if (lowerLine.contains("glibc") || lowerLine.contains("gnu libc")) {
                    return LibcType.GLIBC;
                } else if (lowerLine.contains("musl")) {
                    return LibcType.MUSL;
                }
            }

            process.waitFor();
        } catch (Exception e) {
            // Ignore
        }
        return LibcType.UNKNOWN;
    }

    private static LibcType detectViaGetconf() {
        try {
            ProcessBuilder pb = new ProcessBuilder("getconf", "GNU_LIBC_VERSION");
            Process process = pb.start();

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream())
            );

            String line = reader.readLine();
            if (line != null && line.toLowerCase().startsWith("glibc")) {
                return LibcType.GLIBC;
            }

            process.waitFor();
        } catch (Exception e) {
            // Might indicate musl (getconf GNU_LIBC_VERSION fails on musl)
        }
        return LibcType.UNKNOWN;
    }

    private static LibcType detectFromSystemProperties() {
        String osName = System.getProperty("os.name").toLowerCase();
        String osVersion = System.getProperty("os.version");

        // Some educated guesses based on common distributions
        if (osName.contains("alpine")) {
            return LibcType.MUSL;
        }

        // Most other Linux distributions use glibc by default
        if (osName.contains("linux")) {
            return LibcType.GLIBC;
        }

        return LibcType.UNKNOWN;
    }

    public static void main(String[] args) {
        LibcType libc = detectLibc();
        System.out.println("Detected libc: " + libc);

        // Also print some diagnostic info
        System.out.println("OS Name: " + System.getProperty("os.name"));
        System.out.println("OS Version: " + System.getProperty("os.version"));
        System.out.println("OS Arch: " + System.getProperty("os.arch"));
    }
}