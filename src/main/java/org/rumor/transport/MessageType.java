package org.rumor.transport;

/**
 * Wire protocol message types. Each type is a single byte on the wire.
 */
public enum MessageType {
    GOSSIP_DIGEST  (0x01),
    GOSSIP_ACK     (0x02),
    GOSSIP_ACK2    (0x04),

    // --- RService (request / single-response) ---
    SERVICE_REQUEST  (0x10),
    SERVICE_RESPONSE (0x14),
    SERVICE_ERROR    (0x13),

    // --- RStreamingService (handshake + streamed data) ---
    SERVICE_INIT_STREAM   (0x20),
    SERVICE_STREAM_START  (0x21),
    SERVICE_STREAM_DATA   (0x22),
    SERVICE_STREAM_END    (0x23),
    SERVICE_STREAM_ERROR  (0x24);

    private final int code;

    MessageType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    public static MessageType fromCode(int code) {
        for (MessageType t : values()) {
            if (t.code == code) return t;
        }
        throw new IllegalArgumentException("Unknown message type: 0x" + Integer.toHexString(code));
    }
}
