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

import org.rocksdb.ColumnFamilyHandle;

final class KindImpl implements Kind {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final String name;
    private final ColumnFamilyHandle handle;

    KindImpl(byte[] name, ColumnFamilyHandle handle) {
        this.name = new String(Objects.requireNonNull(name), UTF8);
        this.handle = Objects.requireNonNull(handle);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public int hashCode() {
        return handle.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return handle.equals(obj);
    }

    @Override
    public int compareTo(Kind k) {
        return name.compareTo(Objects.requireNonNull(k).name());
    }

    @Override
    public String toString() {
        return name;
    }
}
