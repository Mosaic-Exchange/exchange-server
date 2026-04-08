package org.rumor.service;

import org.rumor.gossip.NodeId;

import java.util.Map;
import java.util.List;
import java.util.function.Predicate;

/**
 * Read-only view of the cluster's application state.
 *
 * <p>Injected into {@link DistributedService} instances by the framework so that
 * services can query peer state without touching gossip internals.
 */
public interface ClusterView {

    /**
     * Returns every live peer's value for the given state key.
     *
     * @param key the application state key (e.g. "SHARED_FILES")
     * @return map of node ID → value (only peers that have the key)
     */
    Map<NodeId, String> stateForKey(String key);

    /**
     * Finds live peers whose value for {@code key} satisfies the predicate.
     *
     * @param key   the application state key
     * @param match predicate applied to the value
     * @return list of matching node IDs (excludes the local node)
     */
    List<NodeId> findPeers(String key, Predicate<String> match);

    /**
     * The local node's identifier.
     */
    NodeId localId();
}
