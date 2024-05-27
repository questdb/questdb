package com.example.sender;

import io.questdb.client.Sender;
import java.io.IOException;

public class AuthTlsExample {

    public static void main(String[] args) {
        String config = "tcps::addr=clever-black-363-c1213c97.ilp.b04c.questdb.net:32074;user=admin;token=GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0;";
        
        try (Sender sender = Sender.fromConfig(config)) {
            insertRecord(sender, "weather_sensor", "toronto1", 23.5, 0.49);
            insertRecord(sender, "weather_sensor", "dubai2", 41.2, 0.34);
        } catch (IOException | InterruptedException e) {
            System.err.println("Failed to send data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void insertRecord(Sender sender, String tableName, String idSymbol, double temperature, double humidity) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(" ")
        .append(".symbol(\"").append(idSymbol).append("\")")
        .append(".doubleColumn(\"temperature\", ").append(temperature).append(")")
        .append(".doubleColumn(\"humidity\", ").append(humidity).append(")")
        .append(".atNow();");
        
        sender.execute(sb.toString());
    }
}
