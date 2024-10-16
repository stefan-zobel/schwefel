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
package org.schwefel.kv;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.rocksdb.ColumnFamilyHandle;

final class KindImpl implements Kind {

    private final String name;
    private final ColumnFamilyHandle handle;

    KindImpl(byte[] name, ColumnFamilyHandle handle) {
        this.name = new String(Objects.requireNonNull(name), StandardCharsets.UTF_8);
        this.handle = Objects.requireNonNull(handle);
    }

    ColumnFamilyHandle handle() {
        return handle;
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
        if (obj == this) {
            return true;
        }
        if (obj instanceof KindImpl) {
            return handle.equals(((KindImpl) obj).handle);
        }
        return false;
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
