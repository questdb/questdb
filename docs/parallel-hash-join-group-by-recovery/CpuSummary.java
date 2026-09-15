import jdk.jfr.consumer.*;
import java.nio.file.*;
import java.util.*;
public class CpuSummary {
    public static void main(String[] args) throws Exception {
        for (String file : args) {
            Map<String, Integer> leaves = new HashMap<>(), paths = new HashMap<>();
            int all = 0, probe = 0;
            try (var recording = new RecordingFile(Path.of(file))) {
                while (recording.hasMoreEvents()) {
                    var event = recording.readEvent();
                    if (!event.getEventType().getName().equals("jdk.ExecutionSample")) continue;
                    all++;
                    var stack = event.getStackTrace();
                    if (stack == null) continue;
                    var frames = stack.getFrames();
                    if (frames.stream().noneMatch(f -> f.getMethod().getType().getName().equals("io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory"))) continue;
                    probe++;
                    var top = frames.getFirst();
                    leaves.merge(top.getMethod().getType().getName()+"."+top.getMethod().getName()+":"+top.getLineNumber(), 1, Integer::sum);
                    StringBuilder path = new StringBuilder();
                    for (int i=0; i<Math.min(8,frames.size()); i++) {
                        var f=frames.get(i);
                        path.append(f.getMethod().getType().getName()).append('.').append(f.getMethod().getName()).append(':').append(f.getLineNumber()).append(" <- ");
                    }
                    paths.merge(path.toString(), 1, Integer::sum);
                }
            }
            System.out.println(file+" all="+all+" reducer="+probe);
            System.out.println("Leaf methods");
            leaves.entrySet().stream().sorted(Map.Entry.<String,Integer>comparingByValue().reversed()).limit(30).forEach(System.out::println);
            System.out.println("Call paths");
            paths.entrySet().stream().sorted(Map.Entry.<String,Integer>comparingByValue().reversed()).limit(25).forEach(System.out::println);
        }
    }
}
