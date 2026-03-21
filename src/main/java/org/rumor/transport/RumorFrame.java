package org.rumor.transport;

/**
 * A decoded frame from the wire protocol.
 *
 * Wire format:
 *   [Magic: 1 byte] [MessageType: 1 byte] [PayloadLength: 4 bytes] [Payload: N bytes]
 */
public record RumorFrame(MessageType type, byte[] payload) {

    public static final int MAGIC = 0x52; // 'R' for Rumor
    public static final int HEADER_SIZE = 6;  // 1 + 1 + 4

    public RumorFrame {
        if (payload == null) {
            payload = new byte[0];
        }
    }
}
