package net.volcanite.util;

import java.util.Objects;

/**
 * Conversion from long to byte[] array and vice versa. The array representation
 * of the underlying long is in little-endian order.
 */
public final class LittleEndianLong {

    private static final byte[] ZERO = { 0, 0, 0, 0, 0, 0, 0, 0 };
    private static final byte[] ONE = { 1, 0, 0, 0, 0, 0, 0, 0 };

    public static byte[] zero() {
        return ZERO.clone();
    }

    public static byte[] one() {
        return ONE.clone();
    }

    public static long getLong(byte[] bytes) {
        if (Objects.requireNonNull(bytes).length != 8) {
            throw new IllegalArgumentException("byte[] length is <> 8: " + bytes.length);
        }
        return getLongL(bytes);
    }

    public static byte[] getBytes(long value) {
        return putLongL(value, new byte[8]);
    }

    //@formatter:off
    private static long getLongL(byte[] bytes) {
        return makeLong(bytes[7],
                        bytes[6],
                        bytes[5],
                        bytes[4],
                        bytes[3],
                        bytes[2],
                        bytes[1],
                        bytes[0]);
    }
    //@formatter:on

    //@formatter:off
    private static long makeLong(byte b7, byte b6, byte b5, byte b4,
            byte b3, byte b2, byte b1, byte b0) {
        return ((((long) b7       ) << 56) |
                (((long) b6 & 0xff) << 48) |
                (((long) b5 & 0xff) << 40) |
                (((long) b4 & 0xff) << 32) |
                (((long) b3 & 0xff) << 24) |
                (((long) b2 & 0xff) << 16) |
                (((long) b1 & 0xff) <<  8) |
                (((long) b0 & 0xff)      ));
    }
    //@formatter:on

    private static byte[] putLongL(long x, byte[] bytes) {
        bytes[7] = (byte) (x >> 56);
        bytes[6] = (byte) (x >> 48);
        bytes[5] = (byte) (x >> 40);
        bytes[4] = (byte) (x >> 32);
        bytes[3] = (byte) (x >> 24);
        bytes[2] = (byte) (x >> 16);
        bytes[1] = (byte) (x >> 8);
        bytes[0] = (byte) x;
        return bytes;
    }

    private LittleEndianLong() {
        throw new AssertionError();
    }
}
