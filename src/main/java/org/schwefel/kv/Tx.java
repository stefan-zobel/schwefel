/*
 * Copyright 2020 Stefan Zobel
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

public interface Tx extends AutoCloseable {

    void commit();
    void rollback();
    void close();
    // boolean isOpen(); // ???
    void disableIndexing();
    void enableIndexing();
    void setLockTimeout(long lockTimeoutMillis);
    void put(byte[] key, byte[] value);
    byte[] get(byte[] key);
    byte[][] multiGet(byte[][] keys);
    byte[] getForUpdate(byte[] key);
    byte[] getForUpdate(byte[] key, boolean exclusive);
    byte[][] multiGetForUpdate(byte[][] keys);
    void undoGetForUpdate(byte[] key);
    void delete(byte[] key);
    void singleDelete(byte[] key);
    void update(byte[] key, byte[] value);
}
