package org.rumor.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors Netty channel writability and wakes up blocked application threads
 * when the channel drains below the low water mark.
 *
 * Each channel gets its own BackpressureHandler instance (not @Sharable).
 */
public class BackpressureHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(BackpressureHandler.class);

    private final Object writeMonitor;

    public BackpressureHandler(Object writeMonitor) {
        this.writeMonitor = writeMonitor;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (ctx.channel().isWritable()) {
            log.trace("Channel {} became writable, notifying blocked writers", ctx.channel().remoteAddress());
            synchronized (writeMonitor) {
                writeMonitor.notifyAll();
            }
        }
        ctx.fireChannelWritabilityChanged();
    }
}
