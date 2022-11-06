package com.example.sender;

import io.questdb.client.Sender;

public class BasicExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.builder().address("localhost:9009").build()) {
            sender.table("inventors")
                    .symbol("born", "Austrian Empire")
                    .longColumn("id", 0)
                    .stringColumn("name", "Nicola Tesla")
                    .atNow();
            sender.table("inventors")
                    .symbol("born", "USA")
                    .longColumn("id", 1)
                    .stringColumn("name", "Thomas Alva Edison")
                    .atNow();
        }
    }
}