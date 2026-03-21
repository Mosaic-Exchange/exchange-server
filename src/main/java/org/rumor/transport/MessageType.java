package org.rumor.transport;

/**
 * Wire protocol message types. Each type is a single byte on the wire.
 */
public enum MessageType {
    GOSSIP_DIGEST  (0x01),
    GOSSIP_ACK     (0x02),
    GOSSIP_ACK2    (0x04),

    EXCHANGE_START (0x10),
    EXCHANGE_DATA  (0x11),
    EXCHANGE_END   (0x12);

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
