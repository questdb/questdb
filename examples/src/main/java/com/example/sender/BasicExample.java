package com.example.sender;

import io.questdb.client.Sender;

public class BasicExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.fromConfig("tcp::addr=localhost:9009;")) {
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