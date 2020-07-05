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

import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import net.volcanite.task.AsyncTask;

public final class TransmitBatchTask implements AsyncTask {

    static final Logger logger = Logger.getLogger(TransmitBatchTask.class.getName());

    private final StoreOps store;
    private final SortedByteArrayMap keysValues;

    public TransmitBatchTask(StoreOps store, SortedByteArrayMap keysValues) {
        this.store = store;
        this.keysValues = keysValues;
    }

    @Override
    public void run() {
        try {
            Batch batch = store.createBatch();
            keysValues.forEach(new BatchWriter(batch));
            store.writeBatch(batch);
        } catch (Throwable t) {
            logger.log(Level.SEVERE, "", t);
        }
    }

    private static final class BatchWriter implements BiConsumer<byte[], byte[]> {

        private final Batch batch;

        BatchWriter(Batch batch) {
            this.batch = batch;
        }

        @Override
        public void accept(byte[] key, byte[] value) {
            if (key != null && key.length > 0) {
                try {
                    batch.put(key, value);
                } catch (Throwable t) {
                    logger.log(Level.SEVERE, "", t);
                }
            }
        }
    }
}
