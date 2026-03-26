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
}
