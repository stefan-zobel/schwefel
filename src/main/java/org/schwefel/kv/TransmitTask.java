package org.schwefel.kv;

import java.util.logging.Level;
import java.util.logging.Logger;

import net.volcanite.task.AsyncTask;

public final class TransmitTask implements AsyncTask {

    private static final Logger logger = Logger.getLogger(TransmitTask.class.getName());

    private final StoreOps store;
    private final byte[] key;
    private final byte[] value;

    public TransmitTask(StoreOps store, byte[] key, byte[] value) {
        this.store = store;
        this.key = key;
        this.value = value;
    }

    @Override
    public void run() {
        byte[] key = this.key;
        if (key != null && key.length > 0) {
            try {
                store.put(key, value);
            } catch (Throwable t) {
                logger.log(Level.SEVERE, "", t);
            }
        }
    }
}
