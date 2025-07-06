package com.example.sender;

import io.questdb.client.Sender;

public class AuthTlsExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.fromConfig("tcps::addr=clever-black-363-c1213c97.ilp.b04c.questdb.net:32074;user=admin;token=GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0;")) {
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