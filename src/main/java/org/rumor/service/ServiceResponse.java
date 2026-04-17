package org.rumor.service;

/**
 * Abstraction for writing response data back to the requester.
 * Implementations handle either network (remote) or in-memory (local) delivery.
 *
 * For remote responses, data is encoded to bytes via the service's codec
 * before transmission. For local responses, the typed object is passed
 * directly to the caller, no serialization occurs.
 *
 * @param <T> the response data type ({@code byte[]} by default)
 */
public interface ServiceResponse<T> {

    /**
     * Write typed response data. May be called multiple times for streaming.
     */
    void write(T data);

    /**
     * Write raw bytes directly. For byte[] services this is equivalent to
     * {@link #write(Object) write(T)}. For typed services this bypasses
     * the codec and sends the bytes as-is over the wire.
     *
     * This is a convenience method so byte[] services using raw
     * (unparameterized) {@code ServiceResponse} don't need casts.
     */
    void writeRaw(byte[] data);

    /**
     * Signal successful completion. No further writes are allowed after this.
     */
    void close();

    /**
     * Signal that the service failed to process the request.
     * Sends the error details to the requester and closes the response.
     * No further writes are allowed after this.
     *
     * @param error error details (e.g. exception message encoded as UTF-8)
     */
    void fail(byte[] error);
}
