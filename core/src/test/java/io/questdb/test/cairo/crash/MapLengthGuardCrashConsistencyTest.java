package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableUtils;
import io.questdb.cairo.idx.BitmapIndexUtils;
import io.questdb.griffin.SqlException;
import io.questdb.std.str.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * SP1 Task A3: validates that opening a bitmap index value file shorter than
 * its header-claimed size throws a clean CairoException (not SIGBUS/InternalError).
 * <p>
 * Site 1a: AbstractBitmapIndexReader.of() — the path guarded by the fix.
 */
public class MapLengthGuardCrashConsistencyTest extends AbstractCrashConsistencyTest {

    @Test
    public void testBitmapValueFileTooShortThrowsCleanException() throws Exception {
        runWithCrashFacade(() -> {
            // Create a table with a symbol column that has an index
            execute("create table m (ts timestamp, s symbol index, x int) timestamp(ts) partition by none");

            // Insert enough rows to produce a non-trivial value file
            for (int i = 0; i < 50; i++) {
                execute("insert into m values (" + (i * 1_000_000L) + ", 'sym" + (i % 5) + "', " + i + ")");
            }

            // Release everything so the index files are flushed and closed
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // Locate the bitmap value file (.v) for column 's'
            TableToken tt = engine.verifyTableName("m");
            try (Path vPath = new Path()) {
                vPath.of(engine.getConfiguration().getDbRoot())
                        .concat(tt)
                        .concat(TableUtils.DEFAULT_PARTITION_NAME);
                BitmapIndexUtils.valueFileName(vPath, "s", TableUtils.COLUMN_NAME_TXN_NONE);

                // Verify the file exists before truncating
                Assert.assertTrue("bitmap value file must exist: " + vPath,
                        configuration.getFilesFacade().exists(vPath.$()));

                // Truncate the value file to a tiny size — far below what the header claims
                long fd = configuration.getFilesFacade().openRW(vPath.$(), CairoConfiguration.O_NONE);
                Assert.assertTrue("could not open value file for truncation: " + vPath, fd > -1);
                try {
                    boolean truncated = configuration.getFilesFacade().truncate(fd, 16L);
                    Assert.assertTrue("truncate must succeed", truncated);
                } finally {
                    configuration.getFilesFacade().close(fd);
                }
            }

            // Release again so the reader will reopen from the truncated file
            engine.releaseAllReaders();
            engine.releaseAllWriters();

            // Run an index-using query that forces the bitmap reader to map the .v file.
            // Post-fix: expect a clean CairoException with "bitmap index value file too short".
            // Pre-fix: expect SIGBUS -> InternalError, or silent mis-read.
            boolean caughtClean = false;
            String exceptionMessage = null;
            try {
                printSql("select * from m where s = 'sym1'");
            } catch (CairoException e) {
                caughtClean = true;
                exceptionMessage = e.getMessage();
            } catch (SqlException e) {
                // CairoException may be wrapped in SqlException; unwrap and inspect
                Throwable cause = e.getCause();
                if (cause instanceof CairoException) {
                    caughtClean = true;
                    exceptionMessage = cause.getMessage();
                } else {
                    // SqlException with a non-Cairo cause: propagate with context
                    throw new AssertionError("Got unexpected SqlException: " + e.getMessage(), e);
                }
            } catch (InternalError e) {
                // Pre-fix SIGBUS path — causes the test to fail with a clear message
                Assert.fail("Got InternalError (SIGBUS) instead of clean CairoException — fix not in place: " + e.getMessage());
            }

            Assert.assertTrue(
                    "Expected CairoException about bitmap value file too short, but got none. message=" + exceptionMessage,
                    caughtClean
            );
            Assert.assertTrue(
                    "CairoException message must contain 'bitmap index value file too short', got: " + exceptionMessage,
                    exceptionMessage != null && exceptionMessage.contains("bitmap index value file too short")
            );
        });
    }
}
