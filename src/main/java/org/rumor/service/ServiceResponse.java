package org.rumor.service;

/**
 * Abstraction for writing response bytes back to the requester.
 * Implementations handle either network (remote) or in-memory (local) delivery.
 */
public interface ServiceResponse {

    /**
     * Write response data. May be called multiple times for streaming.
     */
    void write(byte[] data);

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
