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

package io.questdb.test.log;

import io.questdb.log.Log;
import io.questdb.log.LogError;
import io.questdb.log.LogFactory;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.ParanoiaState.LOG_PARANOIA_MODE;
import static io.questdb.ParanoiaState.LOG_PARANOIA_MODE_NONE;

public class LogParanoiaTest {
    private static final Log LOG = LogFactory.getLog(LogParanoiaTest.class);

    @Test
    public void testBrokenUtf8RaisesError() {
        final char lead2 = (char) 0b110_001_01;
        final char cont = (char) 0b10_11_0101;
        final char a = 'a';
        final String broken = "" + lead2 + cont + cont + cont + a + a;
        final String good = "" + lead2 + cont + a + a;
        final int iterCount = 10;

        for (int i = 0; i < iterCount; i++) {
            LOG.info().$("broken follows").$();
            try {
                LOG.info().$(broken).$();
                Assert.fail();
            } catch (LogError e) {
                // expected
            }
            LOG.info().$("good follows").$();
            LOG.info().$(good).$();
            LOG.info().$("end").$();
        }
    }

    @Test
    public void testLeakedRecordRaisesError() {
        final int iterCount = 10;
        LOG.info().$("initial record").$();
        for (int i = 0; i < iterCount; i++) {
            LOG.info().$("leaked record");
            try {
                LOG.info().$("subsequent record").$();
                Assert.fail();
            } catch (LogError e) {
                // expected
            }
            LOG.info().$("good record").$();
        }
    }

    // ParanoiaState.LOG_PARANOIA_MODE is set during class init and checks its stack
    // trace to find callers from a test framework. If the class gets initialized from
    // a background thread, it will fail to detect the test framework, and this test
    // will fail.
    @Test
    public void testLogParanoiaModeEnabled() {
        Assert.assertNotEquals(LOG_PARANOIA_MODE_NONE, LOG_PARANOIA_MODE);
    }
}
