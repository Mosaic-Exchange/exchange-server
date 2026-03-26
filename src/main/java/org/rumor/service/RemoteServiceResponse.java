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
 *
 * <p><b>Backpressure:</b> When the caller is on a non-event-loop thread,
 * this handler blocks with {@code wait()} until the channel is writable.
 * When the caller is on the Netty event loop (e.g. during {@code serve()}),
 * blocking would deadlock — so writes proceed without waiting. Netty's
 * internal buffering and TCP flow control still provide backpressure at
 * the transport level, but memory usage may spike for very large responses.
 * Services performing large streaming writes should offload to their own
 * thread to benefit from application-level backpressure.
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

        boolean onEventLoop = channel.eventLoop().inEventLoop();

        while (remaining > 0) {
            int chunkLen = Math.min(remaining, CHUNK_SIZE);

            if (!onEventLoop) {
                // Safe to block — we won't starve the event loop
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
            } else {
                // On event loop — blocking would deadlock.
                // Just check if the channel is still alive.
                if (!channel.isActive()) {
                    throw new IllegalStateException("Channel closed");
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
