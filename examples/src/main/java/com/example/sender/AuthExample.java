package com.example.sender;

import io.questdb.client.Sender;

public class AuthExample {
    public static void main(String[] args) {
        // Replace:
        // 1. "localhost:9000" with a host and port of your QuestDB server
        // 2. "testUser1" with KID portion from your JSON Web Key
        // 3. token with the D portion of your JSON Web Key
        try (Sender sender = Sender.fromConfig("tcp::addr=localhost:9009;user=testUser1;token=GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0;")) {
            sender.table("trades")
                    .symbol("symbol", "ETH-USD")
                    .symbol("side", "sell")
                    .doubleColumn("price", 2615.54)
                    .doubleColumn("amount", 0.00044)
                    .atNow();
            sender.table("trades")
                    .symbol("symbol", "TC-USD")
                    .symbol("side", "sell")
                    .doubleColumn("price", 39269.98)
                    .doubleColumn("amount", 0.001)
                    .atNow();
        }
    }
}
