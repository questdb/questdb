package org.nfsdb.examples.query;

import com.nfsdb.journal.Journal;
import com.nfsdb.journal.exceptions.JournalException;
import com.nfsdb.journal.factory.JournalFactory;
import com.nfsdb.journal.query.api.QueryAllBuilder;
import org.nfsdb.examples.model.Price;

import java.util.concurrent.TimeUnit;

/**
 * Created by Alexander on 01/06/2014.
 */
public class SimplestSymbolQuery {
    public static void main(String[] args) throws JournalException {
        try (JournalFactory factory = new JournalFactory("c:\\temp\\nfsdb")) {
            try (Journal<Price> journal = factory.reader(Price.class)) {
                long tZero = System.nanoTime();
                int count = 0;
                QueryAllBuilder<Price> builder = journal.query().all().withSymValues("sym", "17");

                for (Price p : builder.asResultSet().bufferedIterator()) {
                    assert p != null;
                    count++;
                }

                System.out.println("Read " + count + " objects in " +
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - tZero) + "ms.");
            }
        }
    }
}
