package org.schwefel.kv;

import java.util.Objects;

import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;

class Transactional implements Tx {

    private Transaction txn = null;
    private final Stats stats;

    Transactional(Transaction txn, Stats stats) {
        this.txn = Objects.requireNonNull(txn);
        this.stats = Objects.requireNonNull(stats).incOpenTxCount();
    }

    @Override
    public synchronized void commit() {
        validateOwned();
        try {
            txn.commit();
        } catch (RocksDBException e) {
            try {
                rollback();
            } catch (Exception ignore) {
                // ignore
            }
            throw new StoreException(e);
        } finally {
            close();
        }
    }

    @Override
    public synchronized void rollback() {
        validateOwned();
        try {
            txn.rollback();
        } catch (RocksDBException e) {
            throw new StoreException(e);
        } finally {
            close();
        }
    }

    @Override
    public synchronized void close() {
        if (txn != null) {
            try {
                txn.close();
            } finally {
                txn = null;
                stats.decOpenTxCount();
            }
        }
    }

    @Override
    public synchronized void disableIndexing() {
        validateOwned();
        txn.disableIndexing();
    }

    @Override
    public synchronized void enableIndexing() {
        validateOwned();
        txn.enableIndexing();
    }

    @Override
    public synchronized void setLockTimeout(long lockTimeoutMillis) {
        validateOwned();
        txn.setLockTimeout(lockTimeoutMillis);
    }

    @Override
    public synchronized void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        // TODO Auto-generated method stub

    }

    @Override
    public synchronized byte[] get(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized byte[][] multiGet(byte[][] keys) {
        Objects.requireNonNull(keys, "keys cannot be null");
        validateOwned();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized byte[] getForUpdate(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized byte[] getForUpdate(byte[] key, boolean exclusive) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized byte[][] multiGetForUpdate(byte[][] keys) {
        Objects.requireNonNull(keys, "keys cannot be null");
        validateOwned();
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public synchronized void undoGetForUpdate(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        // TODO Auto-generated method stub

    }

    @Override
    public synchronized void delete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        // TODO Auto-generated method stub

    }

    @Override
    public synchronized void singleDelete(byte[] key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        // TODO Auto-generated method stub

    }

    @Override
    public synchronized void update(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        validateOwned();
        // TODO Auto-generated method stub

    }

    private void validateOwned() {
        if (txn == null) {
            throw new StoreException("Tx has already lost ownership");
        }
    }
}
