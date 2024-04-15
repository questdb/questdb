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
import io.questdb.jar.jni.JarJniLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Qdb {
    @SuppressWarnings("EmptyMethod")
    public static void init() {
    }

    // Java_io_questdb_std_Qdb_libInit
    private static native void libInit();

    private static boolean loadQdbSkip() {
        try {
            final Properties props = new Properties();
            final String path = "/io/questdb/rust/qdb.properties";
            try (InputStream is = Qdb.class.getResourceAsStream(path)) {
                if (is == null) {
                    throw new RuntimeException("missing resource: " + path);
                }
                props.load(is);
                return Boolean.parseBoolean(props.getProperty("qdb.skip"));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static {
        final boolean qdbSkip = loadQdbSkip();
        if (!qdbSkip) {
            // Rust lib built via rust-maven-plugin.
            // If this fails to load, ensure you've run `mvn compile` (or `mvn package`) from the command line.
            // Integration with IntelliJ requires setting up an Ant script that runs the Maven goal.
            JarJniLoader.loadLib(
                    Qdb.class,
                    "/io/questdb/rust/",
                    "qdb"
            );

            libInit();
        }
    }
}
