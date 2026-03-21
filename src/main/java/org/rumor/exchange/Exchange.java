package org.rumor.exchange;

import io.netty.channel.Channel;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Represents a multiplexed bidirectional exchange over a TCP connection.
 * The application interacts with the framework through standard Java streams.
 */
public class Exchange {

    private final int exchangeId;
    private final ExchangeInputStream inputStream;
    private final ExchangeOutputStream outputStream;

    public Exchange(int exchangeId, Channel channel) {
        this.exchangeId = exchangeId;
        this.inputStream = new ExchangeInputStream();
        this.outputStream = new ExchangeOutputStream(exchangeId, channel);
    }

    /**
     * Constructor for receiver side where we might not need an output stream initially.
     */
    Exchange(int exchangeId, ExchangeInputStream inputStream, ExchangeOutputStream outputStream) {
        this.exchangeId = exchangeId;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
    }

    public int exchangeId() {
        return exchangeId;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }

    /**
     * Package-private: used by ExchangeManager to feed incoming data.
     */
    ExchangeInputStream rawInputStream() {
        return inputStream;
    }
}
