package org.rumor.exchange;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.rumor.gossip.NodeId;
import org.rumor.transport.ConnectionManager;
import org.rumor.transport.MessageType;
import org.rumor.transport.RumorFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Manages the lifecycle of exchanges. Creates new exchanges, routes incoming
 * EXCHANGE_START/DATA/END frames to the correct Exchange, and dispatches
 * application callbacks on virtual threads.
 *
 * Each exchange type is identified by a global string ID (e.g., "hello", "file_transfer")
 * that is consistent across all nodes. Multiple exchange types can be registered,
 * and their availability is propagated via gossip for service discovery.
 */
public class ExchangeManager {

    private static final Logger log = LoggerFactory.getLogger(ExchangeManager.class);

    private final ConnectionManager connectionManager;
    private final Map<Integer, Exchange> activeExchanges = new ConcurrentHashMap<>();
    private final AtomicInteger exchangeIdGenerator = new AtomicInteger(1);
    private final Map<String, ExchangeHandler> handlers = new ConcurrentHashMap<>();
    private volatile Consumer<Set<String>> onRegistrationChanged;

    // Virtual threads for application I/O — never block Netty's event loop
    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public ExchangeManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    /**
     * Set a callback that fires whenever the set of registered exchange types changes.
     * Used by RumorNode to publish updates via gossip.
     */
    public void setOnRegistrationChanged(Consumer<Set<String>> callback) {
        this.onRegistrationChanged = callback;
    }

    /**
     * Register a handler for a specific exchange type.
     *
     * @param exchangeType global identifier for this exchange (e.g., "hello", "file_transfer")
     * @param handler      callback invoked when a remote peer starts this exchange type
     */
    public void registerHandler(String exchangeType, ExchangeHandler handler) {
        handlers.put(exchangeType, handler);
        log.info("Registered exchange type: {}", exchangeType);
        if (onRegistrationChanged != null) {
            onRegistrationChanged.accept(getRegisteredTypes());
        }
    }

    /**
     * Unregister a handler for a specific exchange type.
     */
    public void unregisterHandler(String exchangeType) {
        handlers.remove(exchangeType);
        log.info("Unregistered exchange type: {}", exchangeType);
        if (onRegistrationChanged != null) {
            onRegistrationChanged.accept(getRegisteredTypes());
        }
    }

    /**
     * Returns the set of exchange types this node can handle.
     */
    public Set<String> getRegisteredTypes() {
        return Collections.unmodifiableSet(handlers.keySet());
    }

    /**
     * Initiate an exchange with a remote peer.
     * Returns an Exchange whose streams the application can use.
     *
     * @param peer         the target node
     * @param exchangeType the global exchange type identifier
     * @param metadata     application-defined metadata
     */
    public Exchange startExchange(NodeId peer, String exchangeType, byte[] metadata)
            throws InterruptedException {
        Channel channel = connectionManager.getChannel(peer);
        if (channel == null) {
            channel = connectionManager.connect(peer);
        }

        int exchangeId = exchangeIdGenerator.getAndIncrement();
        Exchange exchange = new Exchange(exchangeId, channel);
        activeExchanges.put(exchangeId, exchange);

        // Build EXCHANGE_START payload:
        //   [exchangeId: 4B][typeLen: 2B][exchangeType: NB][metadata: NB]
        byte[] typeBytes = exchangeType.getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[4 + 2 + typeBytes.length + metadata.length];
        ByteBuffer buf = ByteBuffer.wrap(payload);
        buf.putInt(exchangeId);
        buf.putShort((short) typeBytes.length);
        buf.put(typeBytes);
        buf.put(metadata);

        channel.writeAndFlush(new RumorFrame(MessageType.EXCHANGE_START, payload));
        log.debug("Started exchange {} (type={}) with peer {}", exchangeId, exchangeType, peer);

        return exchange;
    }

    // Handle incoming exchange frames 

    public void handleExchangeStart(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int exchangeId = buf.getInt();

        // Read exchange type
        short typeLen = buf.getShort();
        byte[] typeBytes = new byte[typeLen];
        buf.get(typeBytes);
        String exchangeType = new String(typeBytes, StandardCharsets.UTF_8);

        // Remaining bytes are metadata
        byte[] metadata = new byte[buf.remaining()];
        buf.get(metadata);

        Exchange exchange = new Exchange(exchangeId, ctx.channel());
        activeExchanges.put(exchangeId, exchange);

        log.debug("Received EXCHANGE_START id={} type={} ({} bytes metadata)",
                exchangeId, exchangeType, metadata.length);

        ExchangeHandler handler = handlers.get(exchangeType);
        if (handler != null) {
            // Dispatch to application on a virtual thread
            ioExecutor.submit(() -> {
                try {
                    handler.onExchange(metadata, exchange);
                } catch (Exception e) {
                    log.error("Error in exchange handler for exchange {} (type={})",
                            exchangeId, exchangeType, e);
                }
            });
        } else {
            log.warn("No handler registered for exchange type '{}', ignoring exchange {}",
                    exchangeType, exchangeId);
        }
    }

    public void handleExchangeData(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int exchangeId = buf.getInt();
        byte[] data = new byte[payload.length - 4];
        buf.get(data);

        Exchange exchange = activeExchanges.get(exchangeId);
        if (exchange != null) {
            exchange.rawInputStream().feed(data);
        } else {
            log.warn("Received data for unknown exchange {}", exchangeId);
        }
    }

    public void handleExchangeEnd(ChannelHandlerContext ctx, byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int exchangeId = buf.getInt();

        Exchange exchange = activeExchanges.remove(exchangeId);
        if (exchange != null) {
            exchange.rawInputStream().complete();
            log.debug("Exchange {} completed", exchangeId);
        } else {
            log.warn("Received end for unknown exchange {}", exchangeId);
        }
    }

    public void shutdown() {
        ioExecutor.shutdown();
        for (Exchange exchange : activeExchanges.values()) {
            exchange.rawInputStream().complete();
        }
        activeExchanges.clear();
    }
}
