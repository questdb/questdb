package com.example.sender;

import io.questdb.client.Sender;

public class AuthTlsExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.fromConfig("tcps::addr=clever-black-363-c1213c97.ilp.b04c.questdb.net:32074;user=admin;token=GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0;")) {
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