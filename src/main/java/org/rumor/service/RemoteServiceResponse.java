package org.rumor.service;

import io.netty.channel.Channel;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Single-write response for {@link RService} requests.
 *
 * <p>{@link #write(byte[])} may be called at most once and buffers the data.
 * {@link #close()} sends a single {@code SERVICE_RESPONSE} frame with the
 * buffered payload (or an empty payload if {@code write} was never called).
 * {@link #fail(byte[])} sends a {@code SERVICE_ERROR} frame instead.
 *
 * <p>Calling {@code write()} more than once throws {@link IllegalStateException}.
 */
class RemoteServiceResponse implements ServiceResponse {

    private static final Logger log = LoggerFactory.getLogger(RemoteServiceResponse.class);

    private final int requestId;
    private final Channel channel;
    private boolean closed;
    private boolean written;
    private byte[] bufferedData;

    RemoteServiceResponse(int requestId, Channel channel) {
        this.requestId = requestId;
        this.channel = channel;
    }

    @Override
    public void write(byte[] data) {
        if (closed) throw new IllegalStateException("Response already closed");
        if (written) throw new IllegalStateException(
                "write() can only be called once for non-streaming services. " +
                "Use @Streamable for multi-write responses.");
        written = true;
        bufferedData = data;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;

        byte[] data = bufferedData != null ? bufferedData : new byte[0];
        byte[] payload = new byte[4 + data.length];
        ByteBuffer.wrap(payload).putInt(requestId);
        System.arraycopy(data, 0, payload, 4, data.length);

        channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_RESPONSE, payload));
        log.debug("Response sent for request {} ({} bytes)", requestId, data.length);
    }

    @Override
    public void fail(byte[] error) {
        if (closed) return;
        closed = true;

        byte[] payload = new byte[4 + error.length];
        ByteBuffer.wrap(payload).putInt(requestId);
        System.arraycopy(error, 0, payload, 4, error.length);

        channel.writeAndFlush(new RumorFrame(MessageType.SERVICE_ERROR, payload));
        log.debug("Response failed for request {} ({} bytes error detail)", requestId, error.length);
    }
}
