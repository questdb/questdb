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

package io.questdb.test.cutlass.http;

import io.questdb.Bootstrap;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class WebConsoleTypesTest {
    private static final Log LOG = LogFactory.getLog(WebConsoleTypesTest.class);

    @Test
    public void testUnsignedTypesArePresentInCreateTableUiBundle() throws Exception {
        try (InputStream input = Bootstrap.class.getResourceAsStream("/io/questdb/site/public.zip")) {
            if (input == null) {
                LOG.info().$("Skipping: public.zip not available (build with -P build-web-console)").$();
            }
            Assume.assumeNotNull(input);

            boolean foundJsBundle = false;
            boolean hasUInt16 = false;
            boolean hasUInt32 = false;
            boolean hasUInt64 = false;

            try (ZipInputStream zip = new ZipInputStream(input, StandardCharsets.UTF_8)) {
                ZipEntry entry;
                while ((entry = zip.getNextEntry()) != null) {
                    if (!entry.isDirectory() && entry.getName().endsWith(".js")) {
                        foundJsBundle = true;
                        final String js = readAll(zip);
                        hasUInt16 |= js.contains("UINT16");
                        hasUInt32 |= js.contains("UINT32");
                        hasUInt64 |= js.contains("UINT64");
                    }
                    zip.closeEntry();
                }
            }

            Assert.assertTrue("No JavaScript bundle found in public.zip", foundJsBundle);
            Assert.assertTrue("Web console bundle does not include UINT16", hasUInt16);
            Assert.assertTrue("Web console bundle does not include UINT32", hasUInt32);
            Assert.assertTrue("Web console bundle does not include UINT64", hasUInt64);
        }
    }

    private static String readAll(ZipInputStream input) throws IOException {
        final byte[] buffer = new byte[8192];
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        int read;
        while ((read = input.read(buffer)) > -1) {
            out.write(buffer, 0, read);
        }
        return out.toString(StandardCharsets.UTF_8);
    }
}
