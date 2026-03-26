package org.rumor.service;

import io.netty.channel.Channel;
import org.rumor.transport.ConnectionManager;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Writes response bytes back to a remote requesting peer over a Netty channel.
 * Supports streaming (multiple write calls) or single-shot transfer.
 */
class RemoteServiceResponse implements ServiceResponse {

    private static final Logger log = LoggerFactory.getLogger(RemoteServiceResponse.class);
    private static final int CHUNK_SIZE = 8192;

    private final int requestId;
    private final Channel channel;
    private final Object writeMonitor;
    private boolean closed;

    RemoteServiceResponse(int requestId, Channel channel) {
        this.requestId = requestId;
        this.channel = channel;
        this.writeMonitor = ConnectionManager.getWriteMonitor(channel);
    }

    @Override
    public void write(byte[] data) {
        if (closed) throw new IllegalStateException("Response already closed");

        int remaining = data.length;
        int offset = 0;

        while (remaining > 0) {
            int chunkLen = Math.min(remaining, CHUNK_SIZE);

            synchronized (writeMonitor) {
                while (!channel.isWritable()) {
                    if (!channel.isActive()) {
                        throw new IllegalStateException("Channel closed");
                    }
                    try {
                        writeMonitor.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Interrupted while writing response", e);
                    }
                }
            }

            byte[] framePayload = new byte[4 + chunkLen];
            ByteBuffer.wrap(framePayload).putInt(requestId);
            System.arraycopy(data, offset, framePayload, 4, chunkLen);

            channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_DATA, framePayload));

            offset += chunkLen;
            remaining -= chunkLen;
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        // Send SERVICE_END with status=0 (success)
        byte[] payload = new byte[5];
        ByteBuffer buf = ByteBuffer.wrap(payload);
        buf.putInt(requestId);
        buf.put((byte) 0);
        channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_END, payload));
        log.debug("Response closed for request {}", requestId);
    }

    void closeWithError() {
        if (closed) return;
        closed = true;

        byte[] payload = new byte[5];
        ByteBuffer buf = ByteBuffer.wrap(payload);
        buf.putInt(requestId);
        buf.put((byte) 1);
        channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_END, payload));
        log.debug("Response closed with error for request {}", requestId);
    }
}
