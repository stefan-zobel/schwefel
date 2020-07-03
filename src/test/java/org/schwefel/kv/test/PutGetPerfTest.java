package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.Arrays;

import org.schwefel.kv.KVStore;
import org.schwefel.kv.Stats;
import org.schwefel.kv.StoreOps;

import net.volcanite.util.DoubleStatistics;

import static org.schwefel.kv.test.TestUtil.randomBytes;
import static net.volcanite.util.Precision.round;

public class PutGetPerfTest {

    public static void main(String[] args) {
        int RUNS = 500_000;
        long runtime = 0L;

        try (StoreOps store = new KVStore(Paths.get("D:/Temp/rocksdb_database"))) {
            for (int i = 0; i < RUNS; ++i) {
                long start = System.currentTimeMillis();
                byte[] key = randomBytes();
                byte[] value = randomBytes();

                store.put(key, value);
                byte[] valueRead = store.get(key);
                runtime += (System.currentTimeMillis() - start);

                if (valueRead == null) {
                    throw new RuntimeException("Unexpected: valueRead == null");
                }
                if (!Arrays.equals(value, valueRead)) {
                    throw new RuntimeException("Unexpected: value != valueRead");
                }
            }

            System.out.println("runti>  avg: " + (runtime / (double) RUNS) + " ms");
            printPutStatistics(store.getStats());
            printGetStatistics(store.getStats());
        }
    }

    private static void printPutStatistics(Stats put) { // put
        DoubleStatistics writeTimeNanos = put.getPutTimeNanos();
        DoubleStatistics fsyncTimeNanos = put.getWalTimeNanos();
        DoubleStatistics totalTimeNanos = put.getAllOpsTimeNanos();

        System.out.println(
                "write>  avg: " + round(writeTimeNanos.getAverage() / 1_000_000.0) + ", n: " + writeTimeNanos.getCount()
                        + ", std: " + round(writeTimeNanos.getStandardDeviation() / 1_000_000.0) + ", min: "
                        + writeTimeNanos.getMin() / 1_000_000.0 + ", max: " + writeTimeNanos.getMax() / 1_000_000.0);
        System.out.println(
                "fsync>  avg: " + round(fsyncTimeNanos.getAverage() / 1_000_000.0) + ", n: " + fsyncTimeNanos.getCount()
                        + ", std: " + round(fsyncTimeNanos.getStandardDeviation() / 1_000_000.0) + ", min: "
                        + fsyncTimeNanos.getMin() / 1_000_000.0 + ", max: " + fsyncTimeNanos.getMax() / 1_000_000.0);
        System.out.println(
                "total>  avg: " + round(totalTimeNanos.getAverage() / 1_000_000.0) + ", n: " + totalTimeNanos.getCount()
                        + ", std: " + round(totalTimeNanos.getStandardDeviation() / 1_000_000.0) + ", min: "
                        + totalTimeNanos.getMin() / 1_000_000.0 + ", max: " + totalTimeNanos.getMax() / 1_000_000.0);
        System.out.println("fsync every: "
                + (totalTimeNanos.getSum() - fsyncTimeNanos.getSum()) / (1_000_000.0 * fsyncTimeNanos.getCount())
                + " ms");
    }

    private static void printGetStatistics(Stats get) { // get
        DoubleStatistics readTimeNanos = get.getGetTimeNanos();
        System.out.println("get  >  avg: " + round(readTimeNanos.getAverage() / 1_000_000.0) + ", n: "
                + readTimeNanos.getCount() + ", std: " + round(readTimeNanos.getStandardDeviation() / 1_000_000.0)
                + ", min: " + readTimeNanos.getMin() / 1_000_000.0 + ", max: " + readTimeNanos.getMax() / 1_000_000.0);
    }
}