/*
 * Copyright 2021 Stefan Zobel
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
package org.schwefel.kv;

import java.nio.charset.Charset;
import java.util.Objects;

import org.rocksdb.RocksDBException;

import net.volcanite.util.LittleEndianLong;

final class SequenceImpl implements Sequence {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final KindImpl kind;
    private final String identifier;
    private final byte[] key;
    private final KVStore kvStore;

    SequenceImpl(KindImpl kind, String key, KVStore kvStore) {
        if (Objects.requireNonNull(key).isEmpty()) {
            throw new IllegalArgumentException("key: ");
        }
        this.kind = Objects.requireNonNull(kind);
        this.identifier = key;
        this.kvStore = kvStore;
        this.key = identifier.getBytes(UTF8);
    }

    @Override
    public void increment() {
        try {
            kvStore.incrementSeq(kind.handle(), key);
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public long incrementAndGet() {
        try {
            return LittleEndianLong.getLong(kvStore.incrementAndGetSeq(kind.handle(), key));
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public long get() {
        try {
            return LittleEndianLong.getLong(kvStore.getSeq(kind.handle(), key));
        } catch (RocksDBException e) {
            throw new StoreException(e);
        }
    }

    @Override
    public String identifier() {
        return identifier;
    }

    @Override
    public Kind kind() {
        return kind;
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, kind);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof SequenceImpl) {
            SequenceImpl other = (SequenceImpl) obj;
            return identifier.equals(other.identifier) && kind.equals(other.kind);
        }
        return false;
    }

    @Override
    public String toString() {
        return kind.name() + ":" + identifier;
    }
}
