package org.rumor.service;

import io.netty.channel.Channel;
import org.rumor.transport.ConnectionManager;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;

/**
 * Multi-write response for {@link Streamable} service requests over the network.
 *
 * <p>{@link #write(Object) write(T)} encodes the typed response via the service's codec,
 * then sends each encoded chunk as a {@code SERVICE_STREAM_DATA} frame.
 * {@link #write(byte[])} sends raw bytes directly without encoding.
 * {@link #close()} sends {@code SERVICE_STREAM_END}.
 * {@link #fail(byte[])} sends {@code SERVICE_STREAM_ERROR}.
 *
 * <p><b>Backpressure:</b> When the caller is on a non-event-loop thread
 * (which is the normal case for streaming services), this handler blocks
 * with {@code wait()} until the channel is writable. This provides
 * application-level backpressure so memory doesn't spike for large streams.
 *
 * @param <T> the response data type
 */
class RemoteStreamingServiceResponse<T> implements ServiceResponse<T> {

    private static final Logger log = LoggerFactory.getLogger(RemoteStreamingServiceResponse.class);
    private static final int CHUNK_SIZE = 8192;

    private final int requestId;
    private final Channel channel;
    private final ServiceHandle handle;
    private final ServiceCodec<T> codec;
    private final Object writeMonitor;
    private boolean closed;

    RemoteStreamingServiceResponse(int requestId, Channel channel, ServiceHandle handle, ServiceCodec<T> codec) {
        this.requestId = requestId;
        this.channel = channel;
        this.handle = handle;
        this.codec = codec;
        this.writeMonitor = ConnectionManager.getWriteMonitor(channel);
    }

    @Override
    public void write(T data) {
        doWrite(codec.encode(data));
    }

    @Override
    public void writeRaw(byte[] data) {
        doWrite(data);
    }

    private void doWrite(byte[] encoded) {
        if (closed) throw new IllegalStateException("Response already closed");
        if (handle.isCancelled()) throw new CancellationException("Service request cancelled");

        int remaining = encoded.length;
        int offset = 0;

        boolean onEventLoop = channel.eventLoop().inEventLoop();

        while (remaining > 0) {
            int chunkLen = Math.min(remaining, CHUNK_SIZE);

            if (!onEventLoop) {
                synchronized (writeMonitor) {
                    while (!channel.isWritable()) {
                        if (!channel.isActive() || handle.isCancelled()) {
                            throw new CancellationException("Service request cancelled");
                        }
                        try {
                            writeMonitor.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new CancellationException("Service request interrupted");
                        }
                    }
                }
            } else {
                if (!channel.isActive()) {
                    throw new IllegalStateException("Channel closed");
                }
            }

            if (handle.isCancelled()) throw new CancellationException("Service request cancelled");

            byte[] framePayload = new byte[4 + chunkLen];
            ByteBuffer.wrap(framePayload).putInt(requestId);
            System.arraycopy(encoded, offset, framePayload, 4, chunkLen);

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
    }
}
