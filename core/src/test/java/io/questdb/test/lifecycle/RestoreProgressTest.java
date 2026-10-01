package io.questdb.test.lifecycle;

import io.questdb.lifecycle.ProgressEvent;
import io.questdb.lifecycle.RestoreProgress;
import org.junit.Assert;
import org.junit.Test;

public class RestoreProgressTest {

    @Test
    public void testExhaustiveSwitch() {
        Assert.assertEquals("noop", describe(new ProgressEvent.NoOpProgress()));
        Assert.assertEquals("restore-3-7", describe(new RestoreProgress(3, 7, 0L, 0L)));
    }

    @Test
    public void testImplementsProgressEvent() {
        ProgressEvent pe = new RestoreProgress(0, 0, 0L, 0L);
        Assert.assertTrue(pe instanceof RestoreProgress);
    }

    @Test
    public void testRecordFields() {
        RestoreProgress rp = new RestoreProgress(3, 7, 100L, 1000L);
        Assert.assertEquals(3, rp.tablesDone());
        Assert.assertEquals(7, rp.tablesTotal());
        Assert.assertEquals(100L, rp.bytesDone());
        Assert.assertEquals(1000L, rp.bytesTotal());
    }

    @Test
    public void testZeroValues() {
        RestoreProgress rp = new RestoreProgress(0, 0, 0L, 0L);
        Assert.assertEquals(0, rp.tablesDone());
        Assert.assertEquals(0, rp.tablesTotal());
        Assert.assertEquals(0L, rp.bytesDone());
        Assert.assertEquals(0L, rp.bytesTotal());
    }

    private static String describe(ProgressEvent event) {
        return switch (event) {
            case ProgressEvent.NoOpProgress np -> "noop";
            case ProgressEvent.TestOnly to -> "test";
            case RestoreProgress rp -> "restore-" + rp.tablesDone() + "-" + rp.tablesTotal();
        };
    }
}
