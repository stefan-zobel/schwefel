package org.schwefel.kv.kueue;

public interface KueueMsgConsumer {

    boolean accept(byte[] message);
}
