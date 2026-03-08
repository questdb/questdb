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
import io.questdb.PropertyKey;
import io.questdb.ServerMain;
import io.questdb.cutlass.http.HttpSessionStore;
import io.questdb.std.Rnd;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.test.tools.TestUtils.unchecked;

public class HttpCookieConcurrentTest extends BaseHttpCookieConcurrentTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        unchecked(() -> createDummyConfiguration(
                PropertyKey.HTTP_USER.getPropertyPath() + "=" + ADMIN_USER,
                PropertyKey.HTTP_PASSWORD.getPropertyPath() + "=" + ADMIN_PWD)
        );
    }

    @Override
    protected int getNumOfSessions(HttpSessionStore sessionStore) {
        return sessionStore.size(ADMIN_USER);
    }

    @Override
    protected String getPassword() {
        return ADMIN_PWD;
    }

    @Override
    protected ServerMain getServerMain(AtomicLong currentMicros) {
        final Bootstrap bootstrap = HttpCookieTest.getBootstrapWithMockClock(currentMicros);
        return new ServerMain(bootstrap);
    }

    @Override
    protected String getUserName(Rnd rnd) {
        return ADMIN_USER;
    }

    @Override
    protected void initTest(Rnd rnd) {
    }
}
