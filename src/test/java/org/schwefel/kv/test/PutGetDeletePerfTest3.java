package org.schwefel.kv.test;

import java.nio.file.Paths;
import java.util.Arrays;

import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;
import org.schwefel.kv.Stats;
import org.schwefel.kv.StoreOps;

import net.volcanite.util.Byte4Key;
import net.volcanite.util.DoubleStatistics;

import static org.schwefel.kv.test.TestUtil.randomBytes;
import static net.volcanite.util.Precision.round;

public class PutGetDeletePerfTest3 {

    public static void main(String[] args) {
        Byte4Key gen = new Byte4Key();
        Byte4Key gen2 = null;
        int RUNS = 1_000_000;
        long runtime = 0L;

        try (StoreOps store = new KVStore(Paths.get("D:/Temp/rocksdb_database"))) {
            Kind defaultKind = store.getKindManagement().getDefaultKind();
            for (int i = 0; i < RUNS; ++i) {
                long start = System.currentTimeMillis();
                byte[] key = gen.next();
                byte[] value = randomBytes();

                store.put(defaultKind, key, value);
                byte[] valueRead = store.get(defaultKind, key);
                byte[] key2 = null;
                if (i == 0) {
                    key2 = store.findMinKey(defaultKind);
                    gen2 = new Byte4Key(key2);
                    System.out.println(gen2);
                    gen2.next();
                } else {
                    key2 = gen2.next();
                }
                store.delete(defaultKind, key2);
                runtime += (System.currentTimeMillis() - start);

                if (valueRead == null) {
                    throw new RuntimeException("Unexpected: valueRead == null");
                }
                if (!Arrays.equals(value, valueRead)) {
                    throw new RuntimeException("Unexpected: value != valueRead");
                }
                if (!Arrays.equals(key, key2)) {
                    throw new RuntimeException(
                            i + " Unexpected: key != key2: " + Arrays.toString(key) + " , " + Arrays.toString(key2));
                }
            }

            System.out.println("runti>  avg: " + (runtime / (double) RUNS) + " ms");
            printPutStatistics(store.getStats());
            printGetStatistics(store.getStats());
        }
    }

    private static void printPutStatistics(Stats put) { // put
        DoubleStatistics writeTimeNanos = put.getPutTimeNanos();
        DoubleStatistics deleteTimeNanos = put.getDeleteTimeNanos();
        DoubleStatistics fsyncTimeNanos = put.getWalTimeNanos();
        DoubleStatistics totalTimeNanos = put.getAllOpsTimeNanos();

        System.out.println(
                "write>  avg: " + round(writeTimeNanos.getAverage() / 1_000_000.0) + ", n: " + writeTimeNanos.getCount()
                        + ", std: " + round(writeTimeNanos.getStandardDeviation() / 1_000_000.0) + ", min: "
                        + writeTimeNanos.getMin() / 1_000_000.0 + ", max: " + writeTimeNanos.getMax() / 1_000_000.0);
        System.out.println("delet>  avg: " + round(deleteTimeNanos.getAverage() / 1_000_000.0) + ", n: "
                + deleteTimeNanos.getCount() + ", std: " + round(deleteTimeNanos.getStandardDeviation() / 1_000_000.0)
                + ", min: " + deleteTimeNanos.getMin() / 1_000_000.0 + ", max: "
                + deleteTimeNanos.getMax() / 1_000_000.0);
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