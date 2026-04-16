/*+*****************************************************************************
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

package io.questdb.std.afm;

import io.questdb.std.Os;
import io.questdb.std.ex.FatalError;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

public final class AppleFoundationModel {
    private static final String LIB_NAME = "libquestdb-afm.dylib";
    private static volatile boolean isLoaded;

    private AppleFoundationModel() {
    }

    public static String generate(String prompt) {
        ensureLoaded();
        return generate0(prompt);
    }

    public static void generateStream(String prompt, Consumer<String> onToken) {
        ensureLoaded();
        final long handle = streamStart0(prompt);
        try {
            String token;
            while ((token = streamNext0(handle)) != null) {
                onToken.accept(token);
            }
        } finally {
            streamClose0(handle);
        }
    }

    public static void main(String[] args) {
        String prompt = args.length > 0 ? String.join(" ", args) : "Say hi in one sentence.";
        generateStream(prompt, t -> {
            System.out.print(t);
            System.out.flush();
        });
        System.out.println();
    }

    private static void ensureLoaded() {
        if (isLoaded) {
            return;
        }
        synchronized (AppleFoundationModel.class) {
            if (isLoaded) {
                return;
            }
            if (Os.type != Os.DARWIN) {
                throw new UnsupportedOperationException(
                        "Apple Foundation Models are only available on macOS");
            }
            // Try dev path first (CMake output), fall back to packaged resource.
            String devPath = "/io/questdb/bin-local/" + LIB_NAME;
            InputStream is = AppleFoundationModel.class.getResourceAsStream(devPath);
            if (is == null) {
                String prdPath = "/io/questdb/bin/" + Os.name + '-' + Os.archName + '/' + LIB_NAME;
                is = AppleFoundationModel.class.getResourceAsStream(prdPath);
                if (is == null) {
                    throw new FatalError("Cannot find " + LIB_NAME + " on classpath (tried "
                            + devPath + " and " + prdPath + ")");
                }
            }
            extractAndLoad(is);
            isLoaded = true;
        }
    }

    private static void extractAndLoad(InputStream is) {
        File tempLib = null;
        try {
            tempLib = File.createTempFile("libquestdb-afm", ".dylib");
            tempLib.deleteOnExit();
            try (FileOutputStream out = new FileOutputStream(tempLib)) {
                byte[] buf = new byte[4096];
                int read;
                while ((read = is.read(buf)) != -1) {
                    out.write(buf, 0, read);
                }
            }
            System.load(tempLib.getAbsolutePath());
        } catch (IOException e) {
            throw new FatalError("Cannot unpack " + tempLib, e);
        } finally {
            try {
                is.close();
            } catch (IOException ignore) {
            }
        }
    }

    private static native String generate0(String prompt);

    private static native void streamClose0(long handle);

    private static native String streamNext0(long handle);

    private static native long streamStart0(String prompt);
}
