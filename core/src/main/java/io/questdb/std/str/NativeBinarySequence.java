package io.questdb.std.str;

import io.questdb.std.BinarySequence;

public interface NativeBinarySequence extends BinarySequence {
    long getAddress();
}
