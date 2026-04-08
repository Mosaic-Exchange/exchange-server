package org.rumor.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Netty TCP server that listens for incoming peer connections.
 */
public class TransportServer {

    private static final Logger log = LoggerFactory.getLogger(TransportServer.class);

    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final BiConsumer<ChannelHandlerContext, RumorFrame> frameHandler;
    private final Consumer<Channel> onConnect;
    private Channel serverChannel;

    public TransportServer(int port,
                           BiConsumer<ChannelHandlerContext, RumorFrame> frameHandler,
                           Consumer<Channel> onConnect) {
        this.port = port;
        this.frameHandler = frameHandler;
        this.onConnect = onConnect;
        this.bossGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup(3);
    }

    public void start() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
         .channel(NioServerSocketChannel.class)
         .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                 new WriteBufferWaterMark(32 * 1024, 64 * 1024))
         .childHandler(new ChannelInitializer<SocketChannel>() {
             @Override
             protected void initChannel(SocketChannel ch) {
                 Object writeMonitor = new Object();
                 ch.pipeline().addLast("codec", new FrameCodec());
                 ch.pipeline().addLast("backpressure", new BackpressureHandler(writeMonitor));
                 ch.pipeline().addLast("dispatcher", new FrameDispatcher(frameHandler));
                 ch.attr(ConnectionManager.WRITE_MONITOR_KEY).set(writeMonitor);
                 onConnect.accept(ch);
             }
         });

        serverChannel = b.bind(port).sync().channel();
        log.info("Transport server listening on port {}", port);
    }

    public void stop() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        log.info("Transport server stopped");
    }

    public EventLoopGroup workerGroup() {
        return workerGroup;
    }
}
