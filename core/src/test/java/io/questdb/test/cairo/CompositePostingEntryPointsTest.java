package io.questdb.test.cairo;

import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * EVERY SQL route to a POSTING index on a composite table must be refused, so the cell-blind
 * {@code sealPostingIndexForPartition} gate cannot be reached from SQL at all.
 * <p>
 * This exists because closing one route is not closing the class. {@code ADD COLUMN} and
 * {@code ALTER COLUMN TYPE} were gated first; probing then showed {@code CREATE} and
 * {@code ALTER COLUMN ... ADD INDEX} were still wide open, and either would have left the same
 * "successful DDL, table bricks on the next merge commit" defect reachable by a slightly different
 * statement.
 * <p>
 * NOT a claim that the seal is unreachable outright: {@code TableWriterAPI#addColumn} takes an index
 * type directly and does not pass through any of these gates -- which is exactly why
 * {@code CompositeFuzzRunner} filters POSTING adds out of its generated operations, and why the
 * writer-side seal gate stays as the non-SQL backstop.
 */
public class CompositePostingEntryPointsTest extends AbstractCairoTest {

    @Test
    public void testCreateWithPostingIndexIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("CREATE TABLE c1 (ts TIMESTAMP, exch SYMBOL INDEX TYPE POSTING, px DOUBLE) "
                        + "TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
                Assert.fail("CREATE with a POSTING index must be refused on a composite table");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(),
                        "composite partitioning does not yet support a POSTING index");
            }
        });
    }

    @Test
    public void testAlterColumnAddIndexPostingIsRefused() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE c2 (ts TIMESTAMP, exch SYMBOL, sym SYMBOL, px DOUBLE) TIMESTAMP(ts) "
                    + "PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c2 VALUES ('2023-01-01T01:00:00.000000Z','BTC','A',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH','B',2.0)");
            drainWalQueue();
            try {
                execute("ALTER TABLE c2 ALTER COLUMN sym ADD INDEX TYPE POSTING");
                Assert.fail("ALTER COLUMN ADD INDEX TYPE POSTING must be refused on a composite table");
            } catch (Exception e) {
                TestUtils.assertContains(e.getMessage(),
                        "composite partitioning does not yet support a POSTING index");
            }
            Assert.assertFalse("a refused ALTER must leave the table live",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c2")));
        });
    }

    @Test
    public void testDefaultBitmapIndexStillAccepted() throws Exception {
        assertMemoryLeak(() -> {
            // default symbol index type: could a config default make a composite table posting-indexed
            // without anyone naming POSTING at all?
            execute("CREATE TABLE c3 (ts TIMESTAMP, exch SYMBOL INDEX, px DOUBLE) "
                    + "TIMESTAMP(ts) PARTITION BY DAY, exch WAL");
            execute("INSERT INTO c3 VALUES ('2023-01-01T01:00:00.000000Z','BTC',1.0),"
                    + "('2023-01-01T02:00:00.000000Z','ETH',2.0)");
            drainWalQueue();
            // POSITIVE CONTROL. The default symbol index type is BITMAP, which composite DOES support,
            // so this must still be ACCEPTED -- otherwise the three refusals above could be passing
            // because indexed SYMBOL columns were refused wholesale on composite tables.
            Assert.assertFalse("a BITMAP-indexed composite table must not suspend",
                    engine.getTableSequencerAPI().isSuspended(engine.verifyTableName("c3")));
        });
    }
}
