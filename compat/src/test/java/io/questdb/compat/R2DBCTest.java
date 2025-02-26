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

package io.questdb.compat;

import io.questdb.ServerMain;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import org.junit.AfterClass;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;

public class R2DBCTest extends AbstractTest {

    @AfterClass
    public static void cleanup() {
        // Shutdown Project Reactor business
        Schedulers.shutdownNow();
    }

    @Test
    public void testSmoke() {
        assertWithR2RDBC(conn -> {
            String name = "QuestDB";
            String verb = "Rocks";

            Mono<String> result = executeQuery(conn, c ->
                    c.createStatement("SELECT concat($1, ' ', $2)")
                            .bind(0, name)
                            .bind(1, verb));

            StepVerifier.create(result)
                    .expectNext("QuestDB Rocks")
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));
        });
    }

    @FunctionalInterface
    private interface SQLStatement {
        Statement prepare(Connection connection);
    }

    private static Mono<String> executeQuery(Publisher<? extends Connection> connectionPublisher, SQLStatement statementPrep) {
        return Mono.from(connectionPublisher)
                .flatMap(conn -> Mono.from(statementPrep.prepare(conn).execute())
                        .flatMap(result -> Mono.from(result.map((row, metadata) ->
                                row.get(0, String.class))))
                        .doFinally(signalType -> conn.close()));
    }

    @FunctionalInterface
    private interface AsyncConnectionAwareRunnable {
        void run(Publisher<? extends Connection> connection);
    }

    private void assertWithR2RDBC(AsyncConnectionAwareRunnable runnable) {
        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            ConnectionFactory connectionFactory = new PostgresqlConnectionFactory(
                    PostgresqlConnectionConfiguration.builder()
                            .host("localhost")
                            .port(serverMain.getPgWireServerPort())
                            .database("qdb")
                            .username("admin")
                            .password("quest")
                            .build()
            );
            runnable.run(connectionFactory.create());
        }
    }
}
