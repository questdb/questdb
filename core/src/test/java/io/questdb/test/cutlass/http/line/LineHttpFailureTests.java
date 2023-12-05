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

package io.questdb.test.cutlass.http.line;

import io.questdb.Bootstrap;
import io.questdb.DefaultBootstrapConfiguration;
import io.questdb.std.Files;
import io.questdb.std.FilesFacade;
import io.questdb.std.str.LPSZ;
import io.questdb.std.str.Utf8s;
import io.questdb.test.AbstractBootstrapTest;
import io.questdb.test.TestServerMain;
import io.questdb.test.std.TestFilesFacadeImpl;
import io.questdb.test.tools.TestUtils;
import org.influxdb.InfluxDB;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static io.questdb.test.cutlass.http.line.IlpHttpUtils.*;

public class LineHttpFailureTests extends AbstractBootstrapTest {
    @Before
    public void setUp() {
        super.setUp();
        TestUtils.unchecked(() -> createDummyConfiguration());
        dbPath.parent().$();
    }

    @Test
    public void testAppendErrors() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final FilesFacade filesFacade = new TestFilesFacadeImpl() {
                private final AtomicInteger attempt = new AtomicInteger();

                @Override
                public int openRW(LPSZ name, long opts) {
                    if (Utf8s.endsWithAscii(name, Files.SEPARATOR + "x.d") && attempt.getAndIncrement() == 0) {
                        return -1;
                    }
                    return super.openRW(name, opts);
                }
            };

            final Bootstrap bootstrap = new Bootstrap(new DefaultBootstrapConfiguration() {
                @Override
                public FilesFacade getFilesFacade() {
                    return filesFacade;
                }
            }, TestUtils.getServerMainArgs(root));

            try (final TestServerMain serverMain = new TestServerMain(bootstrap)) {
                serverMain.start();

                final List<String> points = new ArrayList<>();
                try (final InfluxDB influxDB = IlpHttpUtils.getConnection(serverMain)) {
                    assertRequestOk(influxDB, points, "m1,tag1=value1 f1=1i,y=12i");

                    assertRequestErrorContains(influxDB, points, "m1,tag1=value1 f1=1i,x=12i",
                            "failed to parse line protocol: errors encountered on line(s):write error: m1, line: 2",
                            ", errorId:",
                            ", errno: 2",
                            ", error: could not open read-write"
                    );
                }
            }
        });
    }
}
