/*
 * Copyright 2020, 2021 Stefan Zobel
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

public interface StoreOps extends BasicOps, AutoCloseable {

    void close();
    void put(byte[] key, byte[] value);
    void putIfAbsent(byte[] key, byte[] value);
    byte[] get(byte[] key);
    void delete(byte[] key);
    byte[] deleteIfPresent(byte[] key);
    void deleteRange(byte[] beginKey, byte[] endKey);
    void update(byte[] key, byte[] value);
    Batch createBatch();
    void writeBatch(Batch batch);
    ForEachKeyValue scanAll();
    ForEachKeyValue scanAll(byte[] beginKey);
    ForEachKeyValue scanRange(byte[] beginKey, byte[] endKey);
    Tx startTx();
    void syncWAL();
    boolean isOpen();
    void flush();
    void flushNoWait();
    Stats getStats();
}
