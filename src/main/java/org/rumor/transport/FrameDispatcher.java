package org.rumor.transport;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

/**
 * Routes decoded RumorFrames to the appropriate handler (gossip or exchange).
 * This sits at the top of the Netty pipeline, after FrameCodec.
 */
public class FrameDispatcher extends SimpleChannelInboundHandler<RumorFrame> {

    private static final Logger log = LoggerFactory.getLogger(FrameDispatcher.class);

    private final BiConsumer<ChannelHandlerContext, RumorFrame> frameHandler;

    public FrameDispatcher(BiConsumer<ChannelHandlerContext, RumorFrame> frameHandler) {
        this.frameHandler = frameHandler;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RumorFrame frame) {
        log.debug("Received {} frame ({} bytes) from {}",
                frame.type(), frame.payload().length, ctx.channel().remoteAddress());
        frameHandler.accept(ctx, frame);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in frame dispatcher for {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
