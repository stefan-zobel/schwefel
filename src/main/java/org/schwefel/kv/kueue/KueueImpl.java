package org.schwefel.kv.kueue;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.schwefel.kv.Kind;
import org.schwefel.kv.StoreOps;

import net.volcanite.util.Byte8Key;

/**
 * A simple RocksDB-based in-process durable queue.
 */
/* package */ class KueueImpl implements Kueue {

    private Byte8Key minKey = new Byte8Key(Byte8Key.minKey());
    private Byte8Key maxKey = new Byte8Key(Byte8Key.minKey());

    private final StoreOps ops;
    private final Kind id;

    /** Lock held by put */
    private final ReentrantLock putLock = new ReentrantLock(true);
    /** Lock held by take */
    private final ReentrantLock takeLock = new ReentrantLock(true);
    /** Wait queue for waiting takes */
    private final Condition notEmpty = takeLock.newCondition();
    /** Current number of messages */
    private final AtomicLong count;

    /* package */ KueueImpl(StoreOps store, String identifier) {
        ops = Objects.requireNonNull(store, "store");
        id = store.getKindManagement().getOrCreateKind(Objects.requireNonNull(identifier, "identifier"));
        Byte8Key lastMax = maxKey;
        byte[] currentMin = null;
        byte[] currentMax = null;
        synchronized (ops) {
            currentMin = ops.findMinKey(id);
            currentMax = ops.findMaxKey(id);
        }
        if (currentMin != null) {
            minKey = new Byte8Key(currentMin);
        }
        if (currentMax != null) {
            Byte8Key nextMax = new Byte8Key(currentMax);
            lastMax = new Byte8Key(nextMax.currentValue());
            nextMax.increment();
            maxKey = nextMax;
        }
        if (lastMax.currentValue() < minKey.currentValue()) {
            throw new IllegalStateException("maxKey < minKey");
        }
        long quantity = maxKey.minus(minKey);
        count = new AtomicLong(quantity);
    }

    @Override
    public long size() {
        return count.get();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0L;
    }

    @Override
    public boolean isClosed() {
        return !ops.isOpen();
    }

    @Override
    public String identifier() {
        return id.name();
    }

    /**
     * Signals a waiting take. Called only from put.
     */
    private void signalNotEmpty() {
        ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * Locks to prevent both puts and takes.
     */
    private void fullyLock() {
        putLock.lock();
        takeLock.lock();
    }

    /**
     * Unlocks to allow both puts and takes.
     */
    private void fullyUnlock() {
        takeLock.unlock();
        putLock.unlock();
    }

    @Override
    public void put(byte[] value) {
        Objects.requireNonNull(value, "value");
        long c = -1L;
        ReentrantLock putLock = this.putLock;
        AtomicLong count = this.count;
        putLock.lock();
        try {
            ops.put(id, maxKey.next(), value);
            c = count.getAndIncrement();
        } catch (Throwable t) {
            maxKey.decrement();
            throw t;
        } finally {
            putLock.unlock();
        }
        if (c == 0L) {
            signalNotEmpty();
        }
    }

    @Override
    public byte[] take() throws InterruptedException {
        byte[] value;
        long c = -1L;
        ReentrantLock takeLock = this.takeLock;
        AtomicLong count = this.count;
        takeLock.lock();
        try {
            while (count.get() == 0L) {
                notEmpty.await();
            }
            value = ops.singleDeleteIfPresent(id, minKey.current());
            if (value != null) {
                c = count.getAndDecrement();
            }
            minKey.increment();
            if (c > 1L) {
                // signal other waiting takers
                notEmpty.signal();
            }
        } finally {
            takeLock.unlock();
        }
        return value;
    }

    @Override
    public boolean accept(KueueMsgConsumer consumer) {
        if (consumer == null) {
            return false;
        }
        if (count.get() == 0L) {
            return false;
        }
        ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            byte[] key = minKey.current();
            byte[] msg = ops.get(id, key);
            if (msg != null) {
                if (consumer.accept(msg)) {
                    ops.singleDelete(id, key);
                    if (count.getAndDecrement() > 1L) {
                        // signal other waiting takers
                        notEmpty.signal();
                    }
                    minKey.increment();
                    return true;
                }
            }
        } finally {
            takeLock.unlock();
        }
        return false;
    }

    @Override
    public void clear() {
        fullyLock();
        try {
            AtomicLong count = this.count;
            while (count.get() > 0L) {
                byte[] value = ops.singleDeleteIfPresent(id, minKey.current());
                if (value != null) {
                    count.getAndDecrement();
                }
                minKey.increment();
            }
        } finally {
            fullyUnlock();
        }
    }
}