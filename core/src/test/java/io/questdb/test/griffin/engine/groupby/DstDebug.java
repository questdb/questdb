package io.questdb.test.griffin.engine.groupby;
import io.questdb.test.AbstractCairoTest;
import org.junit.Test;
public class DstDebug extends AbstractCairoTest {
    @Test
    public void testDebug() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE x AS (" +
                "SELECT rnd_double(0)*100 a, rnd_symbol(5,4,4,1) b, " +
                "timestamp_sequence(cast('2021-10-31T00:22:00.000000Z' as timestamp), 3_400_000_000) k " +
                "FROM long_sequence(40)) TIMESTAMP(k) PARTITION BY NONE");

            printSql("SELECT count() s, k FROM x SAMPLE BY 30m ALIGN TO CALENDAR TIME ZONE 'Europe/Riga' WITH OFFSET '00:40'");
            System.out.println("=== NO FILL ===");
            System.out.println(sink);

            printSql("SELECT count() s, k FROM x SAMPLE BY 30m FILL(9999) ALIGN TO CALENDAR TIME ZONE 'Europe/Riga' WITH OFFSET '00:40'");
            System.out.println("=== FILL(9999) ===");
            System.out.println(sink);
        });
    }
}
