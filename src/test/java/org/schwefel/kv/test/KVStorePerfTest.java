package org.schwefel.kv.test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Paths;

import org.schwefel.kv.KVStore;
import org.schwefel.kv.Kind;
import org.schwefel.kv.Stats;
import org.schwefel.kv.StoreOps;

import net.volcanite.util.DoubleStatistics;
import static org.schwefel.kv.test.TestUtil.randomBytes;

public class KVStorePerfTest {

    public static void main(String[] args) {
        int RUNS = 500_000;
        long runtime = 0L;

        try (StoreOps store = new KVStore(Paths.get("D:/Temp/rocksdb_database"))) {
            Kind defaultKind = store.getKindManagement().getDefaultKind();
            for (int i = 0; i < RUNS; ++i) {
                long start = System.currentTimeMillis();
                byte[] key = randomBytes();
                byte[] value = randomBytes();

                store.put(defaultKind, key, value);
                runtime += (System.currentTimeMillis() - start);
            }

            System.out.println("runti>  avg: " + (runtime / (double) RUNS) + " ms");
            printStatistics(store);
        }
    }

    private static void printStatistics(StoreOps store) {
        Stats stats = store.getStats();
        DoubleStatistics putTimeNanos = stats.getPutTimeNanos();
        DoubleStatistics walTimeNanos = stats.getWalTimeNanos();
        DoubleStatistics totalTimeNanos = stats.getAllOpsTimeNanos();

        System.out.println("write>  avg: " + round(putTimeNanos.getAverage() / 1_000_000.0) + ", n: "
                + putTimeNanos.getCount() + ", std: " + round(putTimeNanos.getStandardDeviation() / 1_000_000.0)
                + ", min: " + putTimeNanos.getMin() / 1_000_000.0 + ", max: " + putTimeNanos.getMax() / 1_000_000.0);
        System.out.println("wal  >  avg: " + round(walTimeNanos.getAverage() / 1_000_000.0) + ", n: "
                + walTimeNanos.getCount() + ", std: " + round(walTimeNanos.getStandardDeviation() / 1_000_000.0)
                + ", min: " + walTimeNanos.getMin() / 1_000_000.0 + ", max: " + walTimeNanos.getMax() / 1_000_000.0);
        System.out.println(
                "total>  avg: " + round(totalTimeNanos.getAverage() / 1_000_000.0) + ", n: " + totalTimeNanos.getCount()
                        + ", std: " + round(totalTimeNanos.getStandardDeviation() / 1_000_000.0) + ", min: "
                        + totalTimeNanos.getMin() / 1_000_000.0 + ", max: " + totalTimeNanos.getMax() / 1_000_000.0);
        System.out.println("wal every  : " + round(stats.getAverageWalIntervalMillis()) + " ms");
    }

    public static double round(double x) {
        return round(x, 5);
    }

    public static double round(double x, int scale) {
        if (isBadNum(x)) {
            return 0.0;
        }
        return BigDecimal.valueOf(x).setScale(scale, RoundingMode.HALF_EVEN).doubleValue();
    }

    public static boolean isBadNum(double x) {
        return Double.isNaN(x) || Double.isInfinite(x);
    }
}
