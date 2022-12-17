/*
 * Copyright 2021, 2022 Stefan Zobel
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schwefel.kv.kueue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.rocksdb.RocksDB;
import org.schwefel.kv.KVStore;
import org.schwefel.kv.Stats;
import org.schwefel.kv.StoreOps;

public final class KueueManager implements AutoCloseable {

    private final StoreOps ops;
    private final Path dir;
    private final ConcurrentHashMap<String, KueueImpl> kueues = new ConcurrentHashMap<>();

    public KueueManager(Path directory) {
        ops = new KVStore(Objects.requireNonNull(directory));
        dir = directory;
    }

    public Kueue get(String identifier) {
        if (!isClosed()) {
            return kueues.computeIfAbsent(Objects.requireNonNull(identifier), id -> new KueueImpl(ops, id, this));
        }
        throw new IllegalStateException(KueueManager.class.getName() + " for " + getDirectory() + " is closed");
    }

    public Path getPath() {
        return dir;
    }

    public String getDirectory() {
        String directory = "???";
        try {
            directory = getPath().toFile().getCanonicalPath();
        } catch (IOException ignore) {
        }
        return directory;
    }

    public Stats getStats() {
        return ops.getStats();
    }

    public boolean isClosed() {
        return !ops.isOpen();
    }

    @Override
    public void close() {
        ops.close();
        kueues.clear();
    }

    public void compactAll() {
        ((KVStore) ops).compactAll();
    }

    public RocksDB getRocksDB() {
        return ((KVStore) ops).getRocksDB();
    }
}
