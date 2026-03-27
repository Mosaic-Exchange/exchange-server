package org.rumor.service;

import io.netty.channel.Channel;
import org.rumor.transport.ConnectionManager;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Multi-write response for {@link Streamable} service requests.
 *
 * <p>{@link #write(byte[])} may be called multiple times, each sending a
 * {@code SERVICE_STREAM_DATA} frame. {@link #close()} sends
 * {@code SERVICE_STREAM_END}. {@link #fail(byte[])} sends
 * {@code SERVICE_STREAM_ERROR}.
 *
 * <p><b>Backpressure:</b> When the caller is on a non-event-loop thread
 * (which is the normal case for streaming services), this handler blocks
 * with {@code wait()} until the channel is writable. This provides
 * application-level backpressure so memory doesn't spike for large streams.
 */
class RemoteStreamingServiceResponse implements ServiceResponse {

    private static final Logger log = LoggerFactory.getLogger(RemoteStreamingServiceResponse.class);
    private static final int CHUNK_SIZE = 8192;

    private final int requestId;
    private final Channel channel;
    private final Object writeMonitor;
    private final Runnable onComplete;
    private boolean closed;

    RemoteStreamingServiceResponse(int requestId, Channel channel, Runnable onComplete) {
        this.requestId = requestId;
        this.channel = channel;
        this.writeMonitor = ConnectionManager.getWriteMonitor(channel);
        this.onComplete = onComplete;
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
                if (!channel.isActive()) {
                    throw new IllegalStateException("Channel closed");
                }
            }

            byte[] framePayload = new byte[4 + chunkLen];
            ByteBuffer.wrap(framePayload).putInt(requestId);
            System.arraycopy(data, offset, framePayload, 4, chunkLen);

            channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_STREAM_DATA, framePayload));

            offset += chunkLen;
            remaining -= chunkLen;
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        byte[] payload = new byte[4];
        ByteBuffer.wrap(payload).putInt(requestId);
        channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_STREAM_END, payload));
        log.debug("Stream closed for request {}", requestId);
        onComplete.run();
    }

    @Override
    public void fail(byte[] error) {
        if (closed) return;
        closed = true;

        byte[] payload = new byte[4 + error.length];
        ByteBuffer.wrap(payload).putInt(requestId);
        System.arraycopy(error, 0, payload, 4, error.length);

        channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_STREAM_ERROR, payload));
        log.debug("Stream failed for request {} ({} bytes error detail)", requestId, error.length);
        onComplete.run();
    }
}
