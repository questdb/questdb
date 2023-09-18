package com.example.sender;

import io.questdb.client.Sender;

import java.time.Instant;

public class BasicExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.builder().address("localhost:9009").build()) {
            Instant bday = Instant.parse("1856-07-10T00:00:00.00Z");
            sender.table("inventors")
                    .symbol("born", "Austrian Empire")
                    .timestampColumn("birthday", bday.toEpochMilli() * 1000) // epoch in micros
                    .longColumn("id", 0)
                    .stringColumn("name", "Nicola Tesla")
                    .at(System.nanoTime()); // epoch in nanos
            bday = Instant.parse("1847-02-11T00:00:00.00Z");
            sender.table("inventors")
                    .symbol("born", "USA")
                    .timestampColumn("birthday", bday.toEpochMilli() * 1000)
                    .longColumn("id", 1)
                    .stringColumn("name", "Thomas Alva Edison")
                    .atNow();
        }
    }
}