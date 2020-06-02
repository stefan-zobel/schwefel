package org.schwefel.kv;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.rocksdb.RocksIterator;

// TODO: not sure if this overcautious early closing approach is a good idea
class ForEachAll implements ForEachKeyValue {

    private static final Logger logger = Logger.getLogger(ForEachAll.class.getName());

    private volatile RocksIterator iter;

    ForEachAll(RocksIterator iter) {
        this.iter = Objects.requireNonNull(iter);
    }

    @Override
    public synchronized void close() {
        if (isOpen() && iter.isOwningHandle()) {
            try {
                iter.close();
            } finally {
                iter = null;
            }
        }
    }

    @Override
    public synchronized void forEachRemaining(BiConsumer<byte[], byte[]> action) {
        Objects.requireNonNull(action, "action cannot be null");
        checkOpen();
        try {
            while (iter.isValid()) {
                action.accept(iter.key(), iter.value());
                iter.next();
            }
        } catch (RuntimeException e) {
            logger.log(Level.WARNING, "closing ForEachAll", e);
            throw e;
        } finally {
            close();
        }
    }

    @Override
    public synchronized boolean tryAdvance(BiConsumer<byte[], byte[]> action) {
        Objects.requireNonNull(action, "action cannot be null");
        checkOpen();
        if (iter.isValid()) {
            try {
                action.accept(iter.key(), iter.value());
                iter.next();
            } catch (RuntimeException e) {
                close();
                logger.log(Level.WARNING, "closing ForEachAll", e);
                throw e;
            }
            return true;
        }
        close();
        return false;
    }

    private boolean isOpen() {
        return iter != null;
    }

    private void checkOpen() {
        if (!isOpen()) {
            throw new StoreException("ForEachKeyValue is exhausted or closed");
        }
    }
}
