package com.example.sender;

import io.questdb.client.Sender;

public class AuthExample {
    public static void main(String[] args) {
        try (Sender sender = Sender.builder()
                .address("localhost:9009")
                .enableAuth("testUser1").authToken("GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0")
                .build()) {

            // Insert record for Nikola Tesla
            sender.table("inventors")
                    .symbol("born", "Austrian Empire")
                    .longColumn("id", 0)
                    .stringColumn("name", "Nicola Tesla")
                    .atNow();

            // Insert record for Thomas Edison
            sender.table("inventors")
                    .symbol("born", "USA")
                    .longColumn("id", 1)
                    .stringColumn("name", "Thomas Alva Edison")
                    .atNow();
        } catch (Exception e) {
            System.out.println("An error occurred while sending data: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
