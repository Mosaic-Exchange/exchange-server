package org.rumor.exchange;

/**
 * Application callback for handling incoming exchanges.
 * Implementations define how to respond to exchange requests from remote peers.
 */
@FunctionalInterface
public interface ExchangeHandler {

    /**
     * Called when a remote peer initiates an exchange.
     *
     * @param metadata the application-defined metadata sent by the initiator
     *                 (e.g., "FILE_REQ:video.mp4" or "HELLO")
     * @param exchange the exchange object with input/output streams
     */
    void onExchange(byte[] metadata, Exchange exchange);
}
