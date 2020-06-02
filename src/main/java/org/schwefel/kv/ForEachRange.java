package org.schwefel.kv;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.rocksdb.RocksIterator;

// TODO: not sure if this overcautious early closing approach is a good idea
class ForEachRange implements ForEachKeyValue {

    private static final Logger logger = Logger.getLogger(ForEachRange.class.getName());

    private volatile RocksIterator iter;
    private final byte[] endExclusive;

    ForEachRange(RocksIterator iter, byte[] endKey) {
        this.iter = Objects.requireNonNull(iter);
        this.endExclusive = Objects.requireNonNull(endKey, "endKey cannot be null");
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
                byte[] currentKey = iter.key();
                if (compare(currentKey, endExclusive) < 0) {
                    action.accept(currentKey, iter.value());
                    iter.next();
                } else {
                    break;
                }
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
            byte[] currentKey = iter.key();
            if (compare(currentKey, endExclusive) < 0) {
                try {
                    action.accept(currentKey, iter.value());
                    iter.next();
                } catch (RuntimeException e) {
                    close();
                    logger.log(Level.WARNING, "closing ForEachRange", e);
                    throw e;
                }
                return true;
            }
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

    /**
     * Compares two {@code byte} arrays lexicographically.
     * 
     * @param a
     *            the first array to compare
     * @param b
     *            the second array to compare
     * @return the value {@code 0} if the first and second array are equal and
     *         contain the same elements in the same order; a value less than
     *         {@code 0} if the first array is lexicographically less than the
     *         second array; and a value greater than {@code 0} if the first
     *         array is lexicographically greater than the second array
     */
    private static int compare(byte[] a, byte[] b) {
        if (a == b) {
            return 0;
        }
        if (a == null || b == null) {
            return a == null ? -1 : 1;
        }
        int i = mismatch(a, b, Math.min(a.length, b.length));
        if (i >= 0) {
            return Byte.compare(a[i], b[i]);
        }
        return a.length - b.length;
    }

    private static int mismatch(byte[] a, byte[] b, int length) {
        for (int i = 0; i < length; ++i) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return -1;
    }
}
