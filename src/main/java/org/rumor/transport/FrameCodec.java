package org.rumor.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Encodes/decodes RumorFrames on the wire.
 *
 * Wire format:
 *   [Magic: 1B] [MessageType: 1B] [PayloadLength: 4B] [Payload: NB]
 */
public class FrameCodec extends ByteToMessageCodec<RumorFrame> {

    private static final Logger log = LoggerFactory.getLogger(FrameCodec.class);

    private static final int MAX_PAYLOAD_SIZE = 16 * 1024 * 1024; // 16 MB guard

    @Override
    protected void encode(ChannelHandlerContext ctx, RumorFrame frame, ByteBuf out) {
        out.writeByte(RumorFrame.MAGIC);
        out.writeByte(frame.type().code());
        out.writeInt(frame.payload().length);
        out.writeBytes(frame.payload());
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        while (in.readableBytes() >= RumorFrame.HEADER_SIZE) {
            in.markReaderIndex();

            int magic = in.readByte() & 0xFF;
            if (magic != RumorFrame.MAGIC) {
                log.error("Invalid magic byte: 0x{}, closing connection", Integer.toHexString(magic));
                ctx.close();
                return;
            }

            int typeCode = in.readByte() & 0xFF;
            int payloadLen = in.readInt();

            if (payloadLen < 0 || payloadLen > MAX_PAYLOAD_SIZE) {
                log.error("Invalid payload length: {}, closing connection", payloadLen);
                ctx.close();
                return;
            }

            if (in.readableBytes() < payloadLen) {
                in.resetReaderIndex();
                return; // wait for more data
            }

            byte[] payload = new byte[payloadLen];
            in.readBytes(payload);

            MessageType type = MessageType.fromCode(typeCode);
            out.add(new RumorFrame(type, payload));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("FrameCodec error on {}", ctx.channel().remoteAddress(), cause);
        ctx.close();
    }
}
