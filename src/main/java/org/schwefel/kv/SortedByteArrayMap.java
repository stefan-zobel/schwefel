/*
 * Copyright 2020, 2023 Stefan Zobel
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

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;

public class SortedByteArrayMap implements SortedMap<byte[], byte[]> {

    private final TreeMap<byte[], byte[]> map;

    public SortedByteArrayMap() {
        map = new TreeMap<>(LexicographicByteArrayComparator.COMPARATOR);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void forEach(BiConsumer<? super byte[], ? super byte[]> action) {
        map.forEach(action);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return map.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsValue(Object value) {
        return map.containsValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] get(Object key) {
        return map.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] put(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return map.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] remove(Object key) {
        return map.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void putAll(Map<? extends byte[], ? extends byte[]> m) {
        map.putAll(m);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        map.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] putIfAbsent(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return map.putIfAbsent(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean replace(byte[] key, byte[] oldValue, byte[] newValue) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(oldValue, "oldValue cannot be null");
        Objects.requireNonNull(newValue, "newValue cannot be null");
        return map.replace(key, oldValue, newValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] replace(byte[] key, byte[] value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        return map.replace(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Comparator<? super byte[]> comparator() {
        return map.comparator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMap<byte[], byte[]> subMap(byte[] fromKey, byte[] toKey) {
        Objects.requireNonNull(fromKey, "fromKey cannot be null");
        Objects.requireNonNull(toKey, "toKey cannot be null");
        return map.subMap(fromKey, toKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMap<byte[], byte[]> headMap(byte[] toKey) {
        Objects.requireNonNull(toKey, "toKey cannot be null");
        return map.headMap(toKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SortedMap<byte[], byte[]> tailMap(byte[] fromKey) {
        Objects.requireNonNull(fromKey, "fromKey cannot be null");
        return map.tailMap(fromKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] firstKey() {
        return map.firstKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] lastKey() {
        return map.lastKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<byte[]> keySet() {
        return map.keySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<byte[]> values() {
        return map.values();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<java.util.Map.Entry<byte[], byte[]>> entrySet() {
        return map.entrySet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return map.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        return map.equals(obj);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return map.toString();
    }
}
