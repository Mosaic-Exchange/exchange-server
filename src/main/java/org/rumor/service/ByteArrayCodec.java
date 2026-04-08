package org.rumor.service;

/**
 * Identity codec for {@code byte[]} — no serialization, just passes
 * the byte array through unchanged. Used as the default when the
 * service's type parameter is {@code byte[]}.
 */
final class ByteArrayCodec implements ServiceCodec<byte[]> {

    static final ByteArrayCodec INSTANCE = new ByteArrayCodec();

    @Override
    public byte[] encode(byte[] value) { return value; }

    @Override
    public byte[] decode(byte[] data) { return data; }
}
