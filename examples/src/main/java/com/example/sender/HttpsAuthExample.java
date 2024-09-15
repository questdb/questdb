import io.questdb.client.Sender;
import io.questdb.client.SenderException;

public class AuthTlsExample {
    public static void main(String[] args) {
        try {
            // Ensure the connection string is correct and matches the expected format
            String config = "tcps::addr=clever-black-363-c1213c97.ilp.b04c.questdb.net:32074;user=admin;token=GwBXoGG5c6NoUTLXnzMxw_uNiVa8PKobzx5EiuylMW0;";

            try (Sender sender = Sender.fromConfig(config)) {
                // Sending data to QuestDB
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

                System.out.println("Data sent successfully.");
            }
        } catch (SenderException e) {
            // Handle Sender-specific exceptions
            System.err.println("Error sending data to QuestDB: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            // Handle other potential exceptions
            System.err.println("An unexpected error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
