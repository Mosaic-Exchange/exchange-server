package org.rumor.service;

import java.io.*;

/**
 * Default codec that uses Java serialization for arbitrary types.
 * Objects must implement {@link Serializable}.
 *
 * <p>This codec is used automatically when a service has a non-{@code byte[]}
 * type parameter and no {@link Codec} annotation is specified.
 * For production use, consider providing a custom {@link ServiceCodec}
 * (e.g. JSON via Jackson) via the {@link Codec} annotation.
 *
 * @param <T> the type to serialize (must be {@link Serializable})
 */
final class JavaSerializationCodec<T> implements ServiceCodec<T> {

    @SuppressWarnings("rawtypes")
    static final JavaSerializationCodec INSTANCE = new JavaSerializationCodec<>();

    @Override
    public byte[] encode(T value) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(value);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Serialization failed for " + value.getClass().getName(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T decode(byte[] data) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Deserialization failed", e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Deserialization failed: class not found", e);
        }
    }
}
