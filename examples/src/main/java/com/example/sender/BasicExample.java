package com.example.sender;

import io.questdb.client.Sender;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class BasicExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.builder(Sender.Transport.TCP).address("localhost:9009").build()) {
            sender.table("inventors")
                    .symbol("born", "Austrian Empire")
                    .timestampColumn("birthday", Instant.parse("1856-07-10T00:00:00.00Z"))
                    .longColumn("id", 0)
                    .stringColumn("name", "Nicola Tesla")
                    .at(System.nanoTime(), ChronoUnit.NANOS);
            sender.table("inventors")
                    .symbol("born", "USA")
                    .timestampColumn("birthday", Instant.parse("1847-02-11T00:00:00.00Z"))
                    .longColumn("id", 1)
                    .stringColumn("name", "Thomas Alva Edison")
                    .at(System.nanoTime(), ChronoUnit.NANOS);
        }
    }
}