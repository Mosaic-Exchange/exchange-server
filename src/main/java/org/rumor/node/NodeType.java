package org.rumor.node;

public enum NodeType {
    SEED,
    BASIC,
    EVICTION,
    MASTER;  // MASTER = SEED + EVICTION

    public boolean isSeed() {
        return this == SEED || this == MASTER;
    }

    public boolean isEvictor() {
        return this == EVICTION || this == MASTER;
    }

    public static NodeType fromString(String s) {
        return switch (s.toLowerCase()) {
            case "seed" -> SEED;
            case "basic" -> BASIC;
            case "eviction" -> EVICTION;
            case "master" -> MASTER;
            default -> throw new IllegalArgumentException(
                    "Unknown node type: " + s + " (expected: seed, basic, eviction, master)");
        };
    }
}
