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
