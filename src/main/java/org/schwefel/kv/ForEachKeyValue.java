package org.schwefel.kv;

import java.util.function.BiConsumer;

public interface ForEachKeyValue {

    void forEachRemaining(BiConsumer<byte[], byte[]> action);

    boolean tryAdvance(BiConsumer<byte[], byte[]> action);
}
