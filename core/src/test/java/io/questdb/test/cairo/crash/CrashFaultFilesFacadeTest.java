package io.questdb.test.cairo.crash;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import io.questdb.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

public class CrashFaultFilesFacadeTest extends AbstractTest {

    @Test
    public void testCrashTruncatesToLastFsyncedSize() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("a.d")) {
            long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            Assert.assertTrue(fd > -1);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 32, (byte) 1);
                Assert.assertEquals(16, ff.write(fd, buf, 16, 0));
                ff.fsync(fd);
                Assert.assertEquals(16, ff.write(fd, buf, 16, 16));
                Assert.assertEquals(32, ff.length(fd));
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            ff.crash(dir);
            Assert.assertEquals(16, ff.length(path.$()));
        }
    }

    @Test
    public void testArmCrashThrowsAfterNthDurabilityOp() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot3").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("c.d")) {
            long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            ff.armCrashAt(2); // crash on the 2nd durability op
            try {
                ff.fsync(fd); // op 1
                try {
                    ff.fsync(fd); // op 2 -> throws
                    Assert.fail("expected CrashSimulationError");
                } catch (CrashSimulationError expected) {
                    Assert.assertEquals(2, ff.durabilityOpCount());
                }
            } finally {
                ff.close(fd);
            }
        }
    }

    @Test
    public void testBaselineKeepsPriorDataAndTornTailZeroesRange() throws Exception {
        final CrashFaultFilesFacade ff = new CrashFaultFilesFacade();
        final String dir = temp.newFolder("crashroot2").getAbsolutePath();
        try (Path path = new Path().of(dir).concat("b.d")) {
            long fd = ff.openRW(path.$(), CairoConfiguration.O_NONE);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(buf, 64, (byte) 7);
                Assert.assertEquals(64, ff.write(fd, buf, 64, 0)); // 64 bytes, never fsynced
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
                ff.close(fd);
            }
            ff.markDurableBaseline(dir);   // treat the 64 bytes as durable even though never fsynced
            ff.tornTail(path.$(), 60, 4);  // zero bytes [60,64)
            ff.crash(dir);
            Assert.assertEquals("baseline size preserved", 64, ff.length(path.$()));
            long rd = ff.openRO(path.$());
            long rb = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                Assert.assertEquals(64, ff.read(rd, rb, 64, 0));
                Assert.assertEquals((byte) 7, Unsafe.getUnsafe().getByte(rb + 59));
                Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(rb + 60));
                Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(rb + 63));
            } finally {
                Unsafe.free(rb, 64, MemoryTag.NATIVE_DEFAULT);
                ff.close(rd);
            }
        }
    }
}
