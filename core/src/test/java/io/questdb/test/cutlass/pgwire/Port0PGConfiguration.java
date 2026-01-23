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

package io.questdb.test.cutlass.pgwire;

import io.questdb.cutlass.pgwire.DefaultPGConfiguration;
import io.questdb.std.Rnd;


public class Port0PGConfiguration extends DefaultPGConfiguration {
    private static final String DEBUG_PGWIRE_PORT = "QDB_DEBUG_PGWIRE_PORT";
    int connectionLimit;

    public Port0PGConfiguration() {
        this(-1);
    }

    public Port0PGConfiguration(int connectionLimit) {
        this.connectionLimit = connectionLimit;
    }

    public static int getPGWirePort() {
        final String debugPort = System.getenv().get(DEBUG_PGWIRE_PORT);
        // When QDB_DEBUG_PGWIRE_PORT is not set, bind to ANY port.
        return debugPort != null ? Integer.parseInt(debugPort) : 0;
    }

    @Override
    public int getBindPort() {
        return getPGWirePort();
    }

    @Override
    public int getLimit() {
        if (connectionLimit > -1) {
            return connectionLimit;
        }
        return super.getLimit();
    }

    @Override
    public Rnd getRandom() {
        return new Rnd();
    }
}
