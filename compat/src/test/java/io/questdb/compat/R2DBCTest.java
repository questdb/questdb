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

package io.questdb.compat;

import io.questdb.ServerMain;
import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;

public class R2DBCTest extends AbstractTest {

    @AfterClass
    public static void cleanup() {
        // Shutdown Project Reactor business
        Schedulers.shutdownNow();
    }

    @Test
    public void testCachedAsyncFilterAfterTableTruncate() {
        assertWithR2DBC(conn -> {
            String createTableSQL = "CREATE TABLE tab (" +
                    "timestamp TIMESTAMP, " +
                    "status SYMBOL " +
                    ") timestamp(timestamp) partition by day";

            Mono<Void> createTableMono = Mono.from(conn)
                    .flatMap(connection -> Mono.from(connection.createStatement(createTableSQL).execute())
                            .then());

            StepVerifier.create(createTableMono)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

            Mono<Long> insertFlux = Mono.from(conn)
                    .flatMapMany(connection -> {
                        String insertSQL = "INSERT INTO tab (" +
                                "timestamp, " +
                                "status" +
                                ") VALUES (" +
                                "$1, $2" +
                                ")";

                        return Flux.range(0, 10)
                                .flatMap(i -> {
                                    Statement statement = connection.createStatement(insertSQL);
                                    statement.bind(0, Instant.now().plusSeconds(i));
                                    statement.bind(1, "NONE");
                                    return Mono.from(statement.execute());
                                }, 10); // Concurrency of 10
                    })
                    .flatMap(Result::getRowsUpdated)
                    .count();

            StepVerifier.create(insertFlux)
                    .expectNext(10L)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

            // async filter which stops before consuming all page frames
            final String query = "SELECT * FROM tab WHERE status != 'PURGED' LIMIT 8";

            // all rows are matching the filter, but LIMIT makes us stop before consuming all page frames
            assertEventually(() -> {
                long tableSize = Mono.from(conn)
                        .flatMap(connection -> Mono.from(connection.createStatement(query).execute())
                                .flatMapMany(result -> result.map((row, metadata) -> 1))
                                .count()
                        )
                        .blockOptional()
                        .orElse(-1L);
                Assert.assertEquals(8, tableSize);
            });

            // now empty the backing table
            Mono<Long> truncateTableMono = Mono.from(conn)
                    .flatMap(connection -> Mono.from(connection.createStatement("TRUNCATE TABLE tab").execute())
                            .flatMapMany(result -> result.map((row, metadata) -> 1))
                            .count());

            StepVerifier.create(truncateTableMono)
                    .expectNext(0L)
                    .expectComplete()
                    .verify(Duration.ofSeconds(10));

            // verify the filter still works and return correct results, even it's been cached by PGWire
            assertEventually(() -> {
                // all rows are matching the filter, but LIMIT makes us stop before consuming all page frames
                long tableSize = Mono.from(conn)
                        .flatMap(connection -> Mono.from(connection.createStatement(query).execute())
                                .flatMapMany(result -> result.map((row, metadata) -> 1))
                                .count()
                        )
                        .blockOptional()
                        .orElse(-1L);
                Assert.assertEquals(0, tableSize);
            });
        });
    }


    @Test
    public void testSmoke() {
        assertWithR2DBC(conn -> {
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

    private static Mono<String> executeQuery(Publisher<? extends Connection> connectionPublisher, SQLStatement statementPrep) {
        return Mono.from(connectionPublisher)
                .flatMap(conn -> Mono.from(statementPrep.prepare(conn).execute())
                        .flatMap(result -> Mono.from(result.map((row, metadata) ->
                                row.get(0, String.class))))
                        .doFinally(signalType -> conn.close()));
    }

    private void assertWithR2DBC(AsyncConnectionAwareRunnable runnable) {
        try (final ServerMain serverMain = ServerMain.create(root)) {
            serverMain.start();
            ConnectionFactory connectionFactory = new PostgresqlConnectionFactory(
                    PostgresqlConnectionConfiguration.builder()
                            .host("127.0.0.1")
                            .port(serverMain.getPgWireServerPort())
                            .database("qdb")
                            .username("admin")
                            .password("quest")
                            .build()
            );
            runnable.run(connectionFactory.create());
        }
    }

    @FunctionalInterface
    private interface AsyncConnectionAwareRunnable {
        void run(Publisher<? extends Connection> connection);
    }

    @FunctionalInterface
    private interface SQLStatement {
        Statement prepare(Connection connection);
    }
}
