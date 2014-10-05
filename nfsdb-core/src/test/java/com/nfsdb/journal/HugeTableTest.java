package com.nfsdb.journal;

import com.nfsdb.journal.exceptions.JournalConfigurationException;
import com.nfsdb.journal.exceptions.JournalRuntimeException;
import com.nfsdb.journal.factory.configuration.JournalConfigurationBuilder;
import com.nfsdb.journal.logging.Logger;
import com.nfsdb.journal.test.tools.JournalTestFactory;
import com.nfsdb.journal.test.tools.Rnd;
import com.nfsdb.journal.utils.Files;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class HugeTableTest {

    @ClassRule
    public static final JournalTestFactory factory;

    static {
        try {
            factory = new JournalTestFactory(
                    new JournalConfigurationBuilder() {{
                        $(Name.class).recordCountHint(15000000).txCountHint(1)
                                .$sym("name").valueCountHint(15000000).index().noCache()
                        ;
                    }}.build(Files.makeTempDir())
            );
        } catch (JournalConfigurationException e) {
            throw new JournalRuntimeException(e);
        }
    }

    private static final Logger LOGGER = Logger.getLogger(PerformanceTest.class);

    @Test
    public void testLargeSymbolTable() throws Exception {

        JournalWriter<Name> w = factory.writer(Name.class, "name");

        Name name = new Name();
        Rnd rnd = new Rnd();

        long t = 0;
        for (int i = -500000; i < 2000000; i++) {
            if (i == 0) {
                t = System.nanoTime();
            }
            name.name = rnd.randomString(10);
            w.append(name);
        }
        w.commit();

        LOGGER.info("Appended 2M symbols in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t) + "ms");
    }

    public static class Name {
        String name;
    }
}
