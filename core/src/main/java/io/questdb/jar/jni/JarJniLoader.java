/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.jar.jni;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public interface JarJniLoader {

    static <T> void loadLib(Class<T> cls, String jarPathPrefix, LibInfo lib) {
        final String sep = jarPathPrefix.endsWith("/") ? "" : "/";
        final String pathInJar = jarPathPrefix + sep + lib.getPath();
        final InputStream is = cls.getResourceAsStream(pathInJar);
        if (is == null) {
            throw new LoadException("Internal error: cannot find " + pathInJar + ", broken package?");
        }

        try {
            File tempLib = null;
            try {
                final int dot = pathInJar.indexOf('.');
                tempLib = File.createTempFile(pathInJar.substring(0, dot), pathInJar.substring(dot));
                // copy to tempLib
                try (FileOutputStream out = new FileOutputStream(tempLib)) {
                    byte[] buf = new byte[4096];
                    while (true) {
                        int read = is.read(buf);
                        if (read == -1) {
                            break;
                        }
                        out.write(buf, 0, read);
                    }
                } finally {
                    tempLib.deleteOnExit();
                }
                System.load(tempLib.getAbsolutePath());
            } catch (IOException e) {
                throw new LoadException("Internal error: cannot unpack " + tempLib, e);
            }
        } finally {
            try {
                is.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    static <T> void loadLib(Class<T> cls, String jarPathPrefix, String libName) {
        loadLib(cls, jarPathPrefix, new LibInfo(libName));
    }
}
