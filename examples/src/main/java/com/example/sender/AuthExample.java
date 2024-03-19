package com.example.sender;

import io.questdb.client.Sender;

public class AuthExample {
    public static void main(String[] args) {
        // Replace:
        // 1. "localhost:9000" with a host and port of your QuestDB server
        // 2. "testUser1" with KID portion from your JSON Web Key
        // 3. token with the D portion of your JSON Web Key
        try (Sender sender = Sender.fromConfig("tcp::addr=localhost:9009;user=testUser1;token=GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0;")) {
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
