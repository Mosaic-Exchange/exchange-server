package org.rumor.service;

/**
 * Implement this interface on an {@link RService} to automatically publish
 * application state into the gossip protocol.
 *
 * <p>The framework periodically calls {@link #computeState()} and gossips
 * the result under the key returned by {@link #stateKey()}.
 * Other nodes can then query this state via {@link ClusterView}.
 */
public interface StatePublisher {

    /**
     * The gossip key under which this state is published
     * (e.g. "SHARED_FILES", "LLM_MODELS").
     */
    String stateKey();

    /**
     * Compute the current value for this state.
     * Called periodically by the framework on a background thread.
     *
     * @return the state value to gossip, or empty string if nothing to publish
     */
    String computeState();
}
