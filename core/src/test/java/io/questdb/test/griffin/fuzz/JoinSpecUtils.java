package io.questdb.test.griffin.fuzz;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.Rnd;

public final class JoinSpecUtils {
    private JoinSpecUtils() {
    }

    public static JoinKeySelection selectJoinKeys(RecordMetadata left, RecordMetadata right, Rnd rnd) {
        int leftId = findColumn(left, "id");
        int rightId = findColumn(right, "id");
        if (leftId != -1 && rightId != -1 && isCompatible(left.getColumnType(leftId), right.getColumnType(rightId))) {
            IntList leftKeys = new IntList();
            IntList rightKeys = new IntList();
            leftKeys.add(leftId);
            rightKeys.add(rightId);
            return new JoinKeySelection(leftKeys, rightKeys);
        }

        IntList leftKeys = new IntList();
        IntList rightKeys = new IntList();
        IntList candidateLeft = new IntList();
        IntList candidateRight = new IntList();
        for (int i = 0, ln = left.getColumnCount(); i < ln; i++) {
            int leftType = left.getColumnType(i);
            for (int j = 0, rn = right.getColumnCount(); j < rn; j++) {
                if (isCompatible(leftType, right.getColumnType(j))) {
                    candidateLeft.add(i);
                    candidateRight.add(j);
                }
            }
        }
        if (candidateLeft.size() == 0) {
            throw new IllegalArgumentException("no compatible join keys");
        }
        int pick = rnd.nextInt(candidateLeft.size());
        leftKeys.add(candidateLeft.getQuick(pick));
        rightKeys.add(candidateRight.getQuick(pick));
        return new JoinKeySelection(leftKeys, rightKeys);
    }

    public static Function createJoinFilter(RecordMetadata left, JoinKeySelection keys) {
        if (keys.leftKeys.size() == 0) {
            return null;
        }
        int leftIndex = keys.leftKeys.getQuick(0);
        int rightIndex = keys.rightKeys.getQuick(0);
        int rightOffset = left.getColumnCount() + rightIndex;
        return new TestJoinEqualityFunction(leftIndex, rightOffset);
    }

    private static boolean isCompatible(int leftType, int rightType) {
        if (leftType == rightType) {
            return true;
        }
        if (ColumnType.isSymbolOrStringOrVarchar(leftType) && ColumnType.isSymbolOrStringOrVarchar(rightType)) {
            return true;
        }
        return ColumnType.isTimestamp(leftType) && ColumnType.isTimestamp(rightType);
    }

    private static int findColumn(RecordMetadata metadata, String name) {
        for (int i = 0, n = metadata.getColumnCount(); i < n; i++) {
            if (Chars.equalsIgnoreCase(metadata.getColumnName(i), name)) {
                return i;
            }
        }
        return -1;
    }

    public static final class JoinKeySelection {
        public final IntList leftKeys;
        public final IntList rightKeys;

        public JoinKeySelection(IntList leftKeys, IntList rightKeys) {
            this.leftKeys = leftKeys;
            this.rightKeys = rightKeys;
        }
    }
}
