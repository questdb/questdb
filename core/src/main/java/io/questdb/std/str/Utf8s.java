package io.questdb.std.str;

public interface Utf8s {
    static boolean equals(Utf8Sequence a, Utf8Sequence b) {
        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        if (a.size() != b.size()) {
            return false;
        }

        for (int index = 0, n = a.size(); index < n; index++) {
            if (a.byteAt(index) != b.byteAt(index)) {
                return false;
            }
        }

        return true;
    }

    static boolean equals(DirectUtf8Sequence a, DirectUtf8Sequence b) {
        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        if (a.ptr() == b.ptr() && a.size() == b.size()) {
            return true;
        }

        return equals(a, (Utf8Sequence) b);
    }
}
