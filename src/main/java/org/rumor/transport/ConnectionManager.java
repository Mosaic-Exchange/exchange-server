package org.rumor.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.AttributeKey;
import org.rumor.gossip.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Manages TCP connections to peer nodes.
 * Maps NodeId -> Channel for outbound communication.
 */
public class ConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    public static final AttributeKey<Object> WRITE_MONITOR_KEY =
            AttributeKey.valueOf("writeMonitor");
    public static final AttributeKey<NodeId> NODE_ID_KEY =
            AttributeKey.valueOf("nodeId");

    private final Map<NodeId, Channel> connections = new ConcurrentHashMap<>();
    private final EventLoopGroup workerGroup;
    private final BiConsumer<ChannelHandlerContext, RumorFrame> frameHandler;

    public ConnectionManager(EventLoopGroup workerGroup,
                             BiConsumer<ChannelHandlerContext, RumorFrame> frameHandler) {
        this.workerGroup = workerGroup;
        this.frameHandler = frameHandler;
    }

    /**
     * Connect to a peer. If already connected, returns the existing channel.
     */
    public Channel connect(NodeId peer) throws InterruptedException {
        Channel existing = connections.get(peer);
        if (existing != null && existing.isActive()) {
            return existing;
        }

        Object writeMonitor = new Object();
        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
         .channel(NioSocketChannel.class)
         .option(ChannelOption.WRITE_BUFFER_WATER_MARK,
                 new WriteBufferWaterMark(32 * 1024, 64 * 1024))
         .handler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 ch.pipeline().addLast("codec", new FrameCodec());
                 ch.pipeline().addLast("backpressure", new BackpressureHandler(writeMonitor));
                 ch.pipeline().addLast("dispatcher", new FrameDispatcher(frameHandler));
                 ch.attr(WRITE_MONITOR_KEY).set(writeMonitor);
                 ch.attr(NODE_ID_KEY).set(peer);
             }
         });

        Channel channel = b.connect(peer.host(), peer.port()).sync().channel();
        channel.attr(NODE_ID_KEY).set(peer);
        connections.put(peer, channel);
        log.info("Connected to peer {}", peer);

        channel.closeFuture().addListener(f -> {
            connections.remove(peer);
            log.info("Disconnected from peer {}", peer);
        });

        return channel;
    }

    /**
     * Register an inbound connection (accepted by the server).
     */
    public void registerInbound(Channel channel, NodeId peer) {
        Channel existing = connections.get(peer);
        if (existing == channel) {
            return; // already registered
        }

        channel.attr(NODE_ID_KEY).set(peer);
        connections.put(peer, channel);
        log.info("Registered inbound connection from {}", peer);

        channel.closeFuture().addListener(f -> {
            connections.remove(peer, channel); // only remove if still this channel
            log.info("Connection closed from {}", peer);
        });
    }

    public Channel getChannel(NodeId peer) {
        return connections.get(peer);
    }

    public boolean isConnected(NodeId peer) {
        Channel ch = connections.get(peer);
        return ch != null && ch.isActive();
    }

    public Collection<NodeId> connectedPeers() {
        return connections.keySet();
    }

    /**
     * Send a frame to a specific peer.
     */
    public void send(NodeId peer, RumorFrame frame) {
        Channel ch = connections.get(peer);
        if (ch != null && ch.isActive()) {
            ch.writeAndFlush(frame);
        } else {
            log.warn("Cannot send to {}: not connected", peer);
        }
    }

    /**
     * Get the write monitor for a channel (used for backpressure).
     */
    public static Object getWriteMonitor(Channel channel) {
        return channel.attr(WRITE_MONITOR_KEY).get();
    }

    public void closeAll() {
        for (Channel ch : connections.values()) {
            ch.close();
        }
        connections.clear();
    }
}
