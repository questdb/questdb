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

import io.questdb.cutlass.pgwire.PGConfiguration;
import io.questdb.cutlass.pgwire.PGServer;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.network.NetworkFacade;
import io.questdb.test.cutlass.NetUtils;
import org.jetbrains.annotations.Nullable;

import java.util.function.IntConsumer;

/**
 * Plays PostgreSQL wire hex scripts against a PG server: lines starting with {@code >} are
 * sent as the client, lines starting with {@code <} are the bytes the server must answer
 * ({@link NetUtils#playScript}). Used by {@code PGJobContextTest} and the conformance kit.
 * <p>
 * The caller runs a script under {@code assertMemoryLeak}, which {@code AbstractCairoTest}
 * keeps protected to its subclasses.
 */
public final class PGHexScripts {
    private static final Log LOG = LogFactory.getLog(PGHexScripts.class);

    private PGHexScripts() {
    }

    /**
     * Starts a PG server of {@code test} with {@code configuration} and a fixed client id and
     * secret, plays {@code script} against it and stops it.
     */
    public static void playScript(
            BasePGTest test,
            NetworkFacade clientNf,
            String script,
            PGConfiguration configuration,
            @Nullable IntConsumer afterReceive
    ) throws Exception {

        /*
            You can use Wireshark to capture and decode. You can also see executed statements in the logs.
            From a Wireshark capture you can right-click on a packet and follow conversation:

            ...n....user.xyz.database.qdb.client_encoding.UTF8.DateStyle.ISO.TimeZone.Europe/London.extra_float_digits.2..R........p....oh.R........S....TimeZone.GMT.S....application_name.QuestDB.S....server_version.11.3.S....integer_datetimes.on.S....client_encoding.UTF8.Z....IP...".SET extra_float_digits = 3...B............E...	.....S....1....2....C....SET.Z....IP...7.SET application_name = 'PostgreSQL JDBC Driver'...B............E...	.....S....1....2....C....SET.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...*.select 1,2,3 from long_sequence(1)...B............D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IP...-S_1.select 1,2,3 from long_sequence(1)...B.....S_1.......D....P.E...	.....S....1....2....T...B..1...................2...................3...................D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IB.....S_1.......E...	.....S....2....D..........1....2....3C...
            SELECT 1.Z....IP...&.select 1 from long_sequence(2)...B............D....P.E...	.....S....1....2....T......1...................D..........1D..........1C...
            SELECT 2.Z....IX....
        */

        try (
                PGServer server = test.createPGServer(configuration, true);
                WorkerPool workerPool = server.getWorkerPool()
        ) {
            workerPool.start(LOG);
            NetUtils.playScript(clientNf, script, "127.0.0.1", server.getPort(), afterReceive);
        }
    }
}
