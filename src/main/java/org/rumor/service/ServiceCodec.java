package org.rumor.service;

/**
 * Defines how a custom type {@code T} is serialized to and deserialized from
 * bytes for wire transmission.
 *
 * <p>Implement this interface and reference it via the {@link Codec} annotation
 * on {@link DistributedService#serve} parameters to override the default serialization.
 *
 * <p>The default codec is identity passthrough for {@code byte[]} types, and
 * Java serialization for any other type. Provide a custom codec for better
 * control (e.g. JSON, Protobuf, etc.).
 *
 * @param <T> the type this codec handles
 */
public interface ServiceCodec<T> {

    /**
     * Serialize a value to bytes for wire transmission.
     *
     * @param value the value to encode
     * @return the encoded bytes
     */
    byte[] encode(T value);

    /**
     * Deserialize bytes received from the wire back into a value.
     *
     * @param data the bytes to decode
     * @return the decoded value
     */
    T decode(byte[] data);
}
