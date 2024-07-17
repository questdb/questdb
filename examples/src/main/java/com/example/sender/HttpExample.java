package com.example.sender;

import io.questdb.client.Sender;

public class HttpExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.fromConfig("http::addr=localhost:9000;")) {
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
