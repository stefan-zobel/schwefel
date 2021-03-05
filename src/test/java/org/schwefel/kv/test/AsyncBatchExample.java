package org.schwefel.kv.test;

import net.volcanite.task.AsyncExecutor;

import java.nio.file.Paths;

import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;
import org.schwefel.kv.SortedByteArrayMap;
import org.schwefel.kv.StoreOps;
import org.schwefel.kv.TransmitBatchTask;

import static org.schwefel.kv.test.TestUtil.randomBytes;

public class AsyncBatchExample {

    public static void main(String[] args) {

        AsyncExecutor executor = new AsyncExecutor();
        executor.start();

        int RUNS = 500_000;
        long runtime = 0L;

        try (StoreOps store = new KVStore(Paths.get("D:/Temp/rocksdb_database"))) {
            SortedByteArrayMap map = new SortedByteArrayMap();
            Kind defaultKind = store.getKindManagement().getDefaultKind();

            for (int i = 0; i < RUNS; ++i) {
                long start = System.currentTimeMillis();
                byte[] key = randomBytes();
                byte[] value = randomBytes();
                map.put(key, value);

                if (i % 500 == 0) {
                    executor.execute(new TransmitBatchTask(store, defaultKind, map));
                    map = new SortedByteArrayMap();
                }
                runtime += (System.currentTimeMillis() - start);
            }

            System.out.println("runti>  avg   :  " + (runtime / (double) RUNS) + " ms");
            long shutdownMs = executor.stop(25_000L);
            System.out.println("shutdown took :  " + shutdownMs + " ms");
        }
    }
}
