import java.nio.file.Path;
import java.util.*;
import jdk.jfr.consumer.*;
public class AllocationSummary {
    public static void main(String[] args) throws Exception {
        Map<String, long[]> counts = new TreeMap<>();
        try (RecordingFile recording = new RecordingFile(Path.of(args[0]))) {
            while (recording.hasMoreEvents()) {
                RecordedEvent event = recording.readEvent();
                if (!event.getEventType().getName().equals("jdk.ObjectAllocationOutsideTLAB") || event.getStackTrace() == null) continue;
                List<RecordedFrame> frames = event.getStackTrace().getFrames();
                boolean relevant = frames.stream().anyMatch(f -> f.getMethod().getType().getName().endsWith("IntHashJoinBuildTest") && f.getMethod().getName().startsWith("lambda$") && f.getLineNumber() >= 696 && f.getLineNumber() <= 715);
                if (!relevant) continue;
                StringBuilder key = new StringBuilder(event.getClass("objectClass").getName());
                key.append("\n");
                for (RecordedFrame frame: frames) {
                    key.append("  ").append(frame.getMethod().getType().getName()).append(".").append(frame.getMethod().getName()).append(":").append(frame.getLineNumber()).append("\n");
                    if (frame.getMethod().getType().getName().endsWith("IntHashJoinBuildTest")) break;
                }
                long[] counter = counts.computeIfAbsent(key.toString(), k -> new long[2]);
                counter[0]++; counter[1] += event.getLong("allocationSize");
            }
        }
        for (var entry: counts.entrySet()) System.out.println(Arrays.toString(entry.getValue()) + " " + entry.getKey());
    }
}
