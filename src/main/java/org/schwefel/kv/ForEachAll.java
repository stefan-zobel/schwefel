package org.schwefel.kv;

import java.util.Objects;
import java.util.function.BiConsumer;

import org.rocksdb.RocksIterator;

class ForEachAll implements ForEachKeyValue {

    private final RocksIterator iter;

    ForEachAll(RocksIterator iter) {
        this.iter = Objects.requireNonNull(iter);
    }

    @Override
    public synchronized void close() {
        if (iter.isOwningHandle()) {
            iter.close();
        }
    }

    @Override
    public synchronized void forEachRemaining(BiConsumer<byte[], byte[]> action) {
        Objects.requireNonNull(action, "action cannot be null");
        while (iter.isValid()) {
            action.accept(iter.key(), iter.value());
            iter.next();
        }
        close();
    }

    @Override
    public synchronized boolean tryAdvance(BiConsumer<byte[], byte[]> action) {
        Objects.requireNonNull(action, "action cannot be null");
        if (iter.isValid()) {
            action.accept(iter.key(), iter.value());
            iter.next();
            return true;
        }
        return false;
    }
}
