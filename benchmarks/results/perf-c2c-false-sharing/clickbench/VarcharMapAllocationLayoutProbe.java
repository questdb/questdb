import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.UnorderedVarcharMap;
import io.questdb.std.bytes.DirectByteSink;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class VarcharMapAllocationLayoutProbe {
    private static final int MAP_COUNT = 4352;
    private static final int WORKER_COUNT = 15;

    private record Allocation(long address, int map, int worker) {
    }

    public static void main(String[] args) throws Exception {
        Field keySinkField = UnorderedVarcharMap.class.getDeclaredField("keySink");
        keySinkField.setAccessible(true);
        Field implField = DirectByteSink.class.getDeclaredField("impl");
        implField.setAccessible(true);

        ArrayColumnTypes valueTypes = new ArrayColumnTypes().add(ColumnType.LONG);
        List<UnorderedVarcharMap> maps = new ArrayList<>();
        for (int i = 0; i < MAP_COUNT; i++) {
            maps.add(new UnorderedVarcharMap(
                    valueTypes,
                    32,
                    0.7,
                    64,
                    128 * 1024L,
                    128 * 1024L,
                    true,
                    false
            ));
        }

        long[] controls = new long[MAP_COUNT];
        long[] buffers = new long[MAP_COUNT];
        CountDownLatch ready = new CountDownLatch(WORKER_COUNT);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(WORKER_COUNT);
        for (int worker = 0; worker < WORKER_COUNT; worker++) {
            int workerId = worker;
            new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    for (int mapIndex = workerId; mapIndex < MAP_COUNT; mapIndex += WORKER_COUNT) {
                        UnorderedVarcharMap map = maps.get(mapIndex);
                        map.reopen();
                        DirectByteSink sink = (DirectByteSink) keySinkField.get(map);
                        controls[mapIndex] = implField.getLong(sink);
                        buffers[mapIndex] = sink.ptr();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }, "layout-worker-" + worker).start();
        }
        ready.await();
        start.countDown();
        done.await();

        Map<Long, List<Allocation>> allocationsByLine = new HashMap<>();
        for (int mapIndex = 0; mapIndex < MAP_COUNT; mapIndex++) {
            int worker = mapIndex % WORKER_COUNT;
            add(allocationsByLine, new Allocation(controls[mapIndex], mapIndex, worker));
            add(allocationsByLine, new Allocation(buffers[mapIndex], mapIndex, worker));
        }

        long multiMapLines = allocationsByLine.values().stream()
                .filter(VarcharMapAllocationLayoutProbe::hasMultipleMaps)
                .count();
        long crossWorkerLines = allocationsByLine.values().stream()
                .filter(VarcharMapAllocationLayoutProbe::hasMultipleWorkers)
                .count();
        System.out.printf(
                "maps=%d workers=%d multiMapLines=%d crossWorkerLines=%d%n",
                MAP_COUNT,
                WORKER_COUNT,
                multiMapLines,
                crossWorkerLines
        );

        for (UnorderedVarcharMap map : maps) {
            map.close();
        }
    }

    private static void add(Map<Long, List<Allocation>> byLine, Allocation allocation) {
        byLine.computeIfAbsent(allocation.address & ~63L, ignored -> new ArrayList<>()).add(allocation);
    }

    private static boolean hasMultipleMaps(List<Allocation> allocations) {
        Set<Integer> maps = new HashSet<>();
        for (Allocation allocation : allocations) {
            maps.add(allocation.map);
        }
        return maps.size() > 1;
    }

    private static boolean hasMultipleWorkers(List<Allocation> allocations) {
        Set<Integer> workers = new HashSet<>();
        for (Allocation allocation : allocations) {
            workers.add(allocation.worker);
        }
        return workers.size() > 1;
    }
}
