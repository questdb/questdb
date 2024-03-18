package com.example.sender;

import io.questdb.client.Sender;

public class HttpAdvancedExample {
    public static void main(String[] args) {
        // https::addr=localhost:9000 - Sends data over HTTP transport to the server at localhost:9000 with TLS enabled.
        // tls_verify=unsafe_off - Disables SSL verification. Do not use this in production!
        // retry_timeout=20000 - Sets timeout for retries to 20 seconds. This means recoverable errors will be retried for 20 seconds.
        // auto_flush_rows=100000 - Sets maximum number of rows to be sent in a single request to 100,000.
        // auto_flush_interval=5000 - Sets maximum interval between requests to 5 seconds - to avoid long lags in low-throughput scenarios.
        try (Sender sender = Sender.fromConfig("https::addr=localhost:9000;tls_verify=unsafe_off;retry_timeout=20000;auto_flush_rows=100000;auto_flush_interval=5000;")) {
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
