package org.nfsdb.examples.append;

import com.nfsdb.journal.JournalWriter;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.utils.Files;
import org.nfsdb.examples.model.Price;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class SimplestAppend {
    /**
     * Appends 1 million rows.
     *
     * @param args factory directory
     * @throws com.nfsdb.journal.exceptions.JournalException
     */
    public static void main(String[] args) throws JournalException {
        try (JournalFactory factory = new JournalFactory("c:\\temp\\nfsdb")) {
            // delete existing price journal
            Files.delete(new File(factory.getConfiguration().getJournalBase(), "price"));
            final int count = 1000000;

            try (JournalWriter<Price> writer = factory.writer(Price.class)) {
                long tZero = System.nanoTime();
                Price p = new Price();

                for (int i = 0; i < count; i++) {
                    p.setTimestamp(tZero + i);
                    p.setSym(String.valueOf(i % 20));
                    p.setPrice(i * 1.04598 + i);
                    writer.append(p);
                }

                // commit is necessary
                writer.commit();
                System.out.println("Persisted " + count + " objects in " +
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - tZero) + "ms.");
            }
        }
    }
}
