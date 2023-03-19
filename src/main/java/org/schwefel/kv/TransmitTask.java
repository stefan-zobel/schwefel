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

import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.volcanite.task.AsyncTask;

public final class TransmitTask implements AsyncTask {

    private static final Logger logger = Logger.getLogger(TransmitTask.class.getName());

    private final StoreOps store;
    private final KindImpl kind;
    private final byte[] key;
    private final byte[] value;

    public TransmitTask(StoreOps store, Kind kind, byte[] key, byte[] value) {
        this.store = Objects.requireNonNull(store);
        this.kind = Objects.requireNonNull((KindImpl) kind);
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() {
        byte[] key = this.key;
        if (key != null && key.length > 0 && value != null) {
            try {
                store.put(kind, key, value);
            } catch (Throwable t) {
                logger.log(Level.SEVERE, "", t);
            }
        }
    }
}
