package org.rumor.exchange;

import io.netty.channel.Channel;
import org.rumor.transport.ConnectionManager;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * An OutputStream that writes EXCHANGE_DATA frames to the network.
 * Implements backpressure: blocks the application thread when the Netty
 * channel is not writable, and resumes when it drains.
 */
public class ExchangeOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(ExchangeOutputStream.class);
    private static final int CHUNK_SIZE = 8192; // 8 KB chunks

    private final int exchangeId;
    private final Channel channel;
    private final Object writeMonitor;
    private boolean closed;

    public ExchangeOutputStream(int exchangeId, Channel channel) {
        this.exchangeId = exchangeId;
        this.channel = channel;
        this.writeMonitor = ConnectionManager.getWriteMonitor(channel);
    }

    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed) throw new IOException("Stream closed");

        int remaining = len;
        int offset = off;

        while (remaining > 0) {
            int chunkLen = Math.min(remaining, CHUNK_SIZE);

            // Backpressure: block if the channel is full
            synchronized (writeMonitor) {
                while (!channel.isWritable()) {
                    if (!channel.isActive()) {
                        throw new IOException("Channel closed");
                    }
                    try {
                        log.trace("Exchange {} backpressure: waiting for channel to drain", exchangeId);
                        writeMonitor.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting for backpressure", e);
                    }
                }
            }

            // Build frame payload: [exchangeId: 4 bytes][data: N bytes]
            byte[] framePayload = new byte[4 + chunkLen];
            ByteBuffer.wrap(framePayload).putInt(exchangeId);
            System.arraycopy(b, offset, framePayload, 4, chunkLen);

            channel.writeAndFlush(new RumorFrame(MessageType.EXCHANGE_DATA, framePayload));

            offset += chunkLen;
            remaining -= chunkLen;
        }
    }

    @Override
    public void flush() {
        channel.flush();
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;

        // Send EXCHANGE_END
        byte[] payload = new byte[4];
        ByteBuffer.wrap(payload).putInt(exchangeId);
        channel.writeAndFlush(new RumorFrame(MessageType.EXCHANGE_END, payload));
        log.debug("Exchange {} output stream closed", exchangeId);
    }
}
