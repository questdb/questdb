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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

public class CompileAllRustBinaries {
    public static void main(String[] args) {
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        // Print the current working directory to stderr
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> CWD >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(System.getProperty("user.dir"));
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> CWD >>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        // Print all system properties to stderr
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> PROPS >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.getProperties().forEach((k, v) -> System.err.println(k + ": " + v));
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> PROPS >>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        // Print all args to stderr
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> ARGS >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        for (String arg : args) {
            System.err.println(arg);
        }
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> ARGS >>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        Path path = Paths.get(args[0]);
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> PATH >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(path);
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> PATH >>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        gatherAllRustBinaries(path);

        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> AAAA >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
    }

    private static void gatherAllRustBinaries(Path path) {
        // Create a "hello.txt" file with contents "world" as a child of the given path
        Path hello = path.resolve("hello.txt");
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> HELLO >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        System.err.println(hello);
        System.err.println(">>>>>>>>>>>>>>>>>>>>>>>> HELLO >>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        try {
            Files.writeString(hello, "world\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
