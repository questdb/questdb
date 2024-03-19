package com.example.sender;

import io.questdb.client.Sender;

public class AuthTlsExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.fromConfig("tcps::addr=clever-black-363-c1213c97.ilp.b04c.questdb.net:32074;user=admin;token=GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0;")) {
            sender.table("weather_sensor")
                    .symbol("id", "toronto1")
                    .doubleColumn("temperature", 23.5)
                    .doubleColumn("humidity", 0.49)
                    .atNow();
            sender.table("weather_sensor")
                    .symbol("id", "dubai2")
                    .doubleColumn("temperature", 41.2)
                    .doubleColumn("humidity", 0.34)
                    .atNow();
        }
    }
}