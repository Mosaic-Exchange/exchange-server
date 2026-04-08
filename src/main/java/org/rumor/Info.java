package org.rumor;

import org.rumor.gossip.EndpointState;
import org.rumor.gossip.NodeId;
import org.rumor.node.Rumor;
import org.rumor.service.OnStateChange;
import org.rumor.service.RequestEvent;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * Utility class providing network and progressive telemetry access.
 */
public class Info {

    private final Rumor rumor;

    public Info(Rumor rumor) {
        this.rumor = rumor;
    }

    /**
     * Retrieves the set of currently live nodes in the cluster.
     */
    public Set<NodeId> getLiveNodes() {
        return rumor.getLiveNodes();
    }

    /**
     * Gets the total number of alive nodes in the network topology.
     */
    public int getLiveNodeCount() {
        return rumor.getLiveNodes().size();
    }

    /**
     * Retrieves the complete cluster state including node statuses, heartbeats, and published application states.
     */
    public Map<NodeId, EndpointState> getNetworkTopology() {
        return rumor.getClusterState();
    }

    /**
     * Wraps an existing OnStateChange callback to track streaming download progress and intercept it
     * for a progress bar or progress logging event.
     *
     * @param totalExpectedBytes the size of the total payload (used to calculate percentages)
     * @param progress           a consumer taking (bytesDownloaded, totalExpectedBytes)
     * @param delegate           the caller's original callback
     * @return a wrapped OnStateChange callback proxy
     */
    public static OnStateChange<byte[]> withProgress(
            long totalExpectedBytes,
            BiConsumer<Long, Long> progress,
            OnStateChange<byte[]> delegate) {

        return new OnStateChange<byte[]>() {
            private long bytesProcessed = 0;

            @Override
            public void accept(RequestEvent<byte[]> event) {
                // Intercept StreamData chunks
                if (event instanceof RequestEvent.StreamData<byte[]> d) {
                    byte[] data = d.raw();
                    if (data != null) {
                        bytesProcessed += data.length;
                        if (progress != null) {
                            progress.accept(bytesProcessed, totalExpectedBytes);
                        }
                    }
                } 
                // Intercept final success payloads (if any)
                else if (event instanceof RequestEvent.Succeeded<byte[]> s) {
                    byte[] data = s.raw();
                    if (data != null) {
                        bytesProcessed += data.length;
                        if (progress != null) {
                            progress.accept(bytesProcessed, totalExpectedBytes);
                        }
                    }
                }

                // Forward event to original handler
                if (delegate != null) {
                    delegate.accept(event);
                }
            }
        };
    }
}
