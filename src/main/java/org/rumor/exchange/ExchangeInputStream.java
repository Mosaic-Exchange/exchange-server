package org.rumor.exchange;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An InputStream backed by incoming EXCHANGE_DATA frames from the network.
 * Netty feeds chunks into the queue; the application thread reads from it.
 */
public class ExchangeInputStream extends InputStream {

    private static final byte[] POISON_PILL = new byte[0];

    private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    private byte[] currentChunk;
    private int currentOffset;
    private boolean closed;

    /**
     * Called by the Netty pipeline to feed data from EXCHANGE_DATA frames.
     */
    void feed(byte[] data) {
        if (!closed) {
            queue.offer(data);
        }
    }

    /**
     * Called when EXCHANGE_END is received — signals EOF.
     */
    void complete() {
        queue.offer(POISON_PILL);
    }

    @Override
    public int read() throws IOException {
        byte[] buf = new byte[1];
        int n = read(buf, 0, 1);
        return n == -1 ? -1 : buf[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (closed) return -1;

        while (currentChunk == null || currentOffset >= currentChunk.length) {
            try {
                currentChunk = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while reading", e);
            }
            if (currentChunk == POISON_PILL) {
                closed = true;
                return -1;
            }
            currentOffset = 0;
        }

        int available = currentChunk.length - currentOffset;
        int toRead = Math.min(len, available);
        System.arraycopy(currentChunk, currentOffset, b, off, toRead);
        currentOffset += toRead;
        return toRead;
    }

    @Override
    public int available() {
        if (currentChunk != null && currentOffset < currentChunk.length) {
            return currentChunk.length - currentOffset;
        }
        byte[] peeked = queue.peek();
        return (peeked != null && peeked != POISON_PILL) ? peeked.length : 0;
    }

    @Override
    public void close() {
        closed = true;
        queue.clear();
    }
}
