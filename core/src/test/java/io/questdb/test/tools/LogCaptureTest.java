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

package io.questdb.test.tools;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.test.AbstractCairoTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LogCaptureTest extends AbstractCairoTest {
    private static Log LOG = LogFactory.getLog(LogCaptureTest.class);
    private static final LogCapture capture = new LogCapture();

    @Before
    @Override
    public void setUp() {
        LogFactory.enableGuaranteedLogging(LogCaptureTest.class);
        super.setUp();
        capture.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        capture.stop();
        super.tearDown();
        LogFactory.disableGuaranteedLogging(LogCaptureTest.class);
    }

    @Test
    public void testWaitForBoundedOverloadReturnsWhenLinePresent() {
        final String marker = "log-capture-witness-present-" + System.nanoTime();
        LOG.info().$(marker).$();
        capture.waitFor(marker, 5_000);
    }

    @Test
    public void testWaitForBoundedOverloadThrowsOnAbsentLine() {
        final String marker = "log-capture-witness-absent-" + System.nanoTime();
        AssertionError error = Assert.assertThrows(AssertionError.class, () -> capture.waitFor(marker, 50));
        Assert.assertTrue(
                "expected the AssertionError message to name the awaited string, got: " + error.getMessage(),
                error.getMessage().contains(marker)
        );
    }
}
