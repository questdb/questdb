package com.example.sender;

import io.questdb.client.Sender;
import io.questdb.client.SenderException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AuthExample {
    public static void main(String[] args) {
        Properties config = loadConfig("config.properties");
        
        String address = config.getProperty("questdb.address", "localhost:9009");
        String user = config.getProperty("questdb.user", "testUser1");
        String token = config.getProperty("questdb.token", "GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0");
        
        try (Sender sender = Sender.fromConfig(String.format("tcp::addr=%s;user=%s;token=%s;", address, user, token))) {
            sendWeatherData(sender, "toronto1", 23.5, 0.49);
            sendWeatherData(sender, "dubai2", 41.2, 0.34);
        } catch (SenderException e) {
            System.err.println("Failed to send data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Properties loadConfig(String fileName) {
        Properties config = new Properties();
        try (InputStream input = AuthExample.class.getClassLoader().getResourceAsStream(fileName)) {
            if (input == null) {
                System.out.println("Sorry, unable to find " + fileName);
                return config;
            }
            config.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return config;
    }

    private static void sendWeatherData(Sender sender, String id, double temperature, double humidity) {
        sender.table("weather_sensor")
                .symbol("id", id)
                .doubleColumn("temperature", temperature)
                .doubleColumn("humidity", humidity)
                .atNow();
    }
}
