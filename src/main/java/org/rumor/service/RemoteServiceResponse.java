package org.rumor.service;

import io.netty.channel.Channel;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CancellationException;

/**
 * Single-write response for {@link DistributedService} requests over the network.
 *
 * {@link #write(Object) write(T)} encodes the typed response via the service's codec,
 * then buffers the bytes. {@link #write(byte[])} stores raw bytes directly without
 * encoding. {@link #close()} sends a single {@code SERVICE_RESPONSE} frame with the
 * buffered payload. {@link #fail(byte[])} sends a {@code SERVICE_ERROR} frame instead.
 *
 * Calling {@link #write(Object)} more than once throws {@link IllegalStateException}.
 *
 * @param <T> the response data type
 */
class RemoteServiceResponse<T> implements ServiceResponse<T> {

    private static final Logger log = LoggerFactory.getLogger(RemoteServiceResponse.class);

    private final int requestId;
    private final Channel channel;
    private final ServiceHandle handle;
    private final ServiceCodec<T> codec;
    private boolean closed;
    private boolean written;
    private byte[] bufferedData;

    RemoteServiceResponse(int requestId, Channel channel, ServiceHandle handle, ServiceCodec<T> codec) {
        this.requestId = requestId;
        this.channel = channel;
        this.handle = handle;
        this.codec = codec;
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
        if (written) throw new IllegalStateException(
                "write() can only be called once for non-streaming services. " +
                "Use @Streamable for multi-write responses.");
        written = true;
        bufferedData = encoded;
    }

    @Override
    public void close() {
        if (closed) return;
        if (handle.isCancelled()) { closed = true; return; }
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
