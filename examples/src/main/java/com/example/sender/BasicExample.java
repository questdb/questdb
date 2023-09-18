package com.example.sender;

import io.questdb.client.Sender;

import java.time.Instant;

public class BasicExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.builder().address("localhost:9009").build()) {
            sender.table("inventors")
                    .symbol("born", "Austrian Empire")
                    .timestampColumn("birthdate", Instant.parse("1856-07-10").toEpochMilli() * 1000) // epoch in microseconds
                    .longColumn("id", 0)
                    .stringColumn("name", "Nicola Tesla")
                    .at(System.nanoTime()); // epoch in nanoseconds
            sender.table("inventors")
                    .symbol("born", "USA")
                    .timestampColumn("birthdate", Instant.parse("1847-02-11").toEpochMilli() * 1000)
                    .longColumn("id", 1)
                    .stringColumn("name", "Thomas Alva Edison")
                    .atNow();
        }
    }
}