package com.example.sender;

import io.questdb.client.Sender;

public class AuthExample {
    public static void main(String[] args) {
        // Replace:
        // 1. "localhost:9000" with a host and port of your QuestDB server
        // 2. "testUser1" with KID portion from your JSON Web Key
        // 3. token with the D portion of your JSON Web Key
        try (Sender sender = Sender.builder()
                .address("localhost:9009")
                .enableAuth("testUser1").authToken("GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0")
                .build()) {
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
