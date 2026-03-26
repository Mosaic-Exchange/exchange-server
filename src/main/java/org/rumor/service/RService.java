package org.rumor.service;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Base class for all services in the Rumor framework.
 *
 * <p>Extend this class and implement {@link #serve(byte[], ServiceResponse)} to handle
 * incoming requests from remote peers. Use {@link #dispatch(byte[], OnStateChange)}
 * to invoke this service on a remote peer that offers it.
 *
 * <p><b>Threading:</b> {@code serve()} is called on the framework's Netty I/O
 * thread. <em>Do not perform blocking operations</em> (e.g. Thread.sleep,
 * blocking I/O, locks held for extended periods) inside {@code serve()} —
 * doing so will stall the event loop and may deadlock backpressure handling.
 * For long-running or blocking work, delegate to your own executor and
 * write results back through the {@link ServiceResponse}.
 *
 * <p>The service is identified by the simple class name of the subclass.
 *
 * <p>Optionally implement {@link StatePublisher} to automatically publish
 * application state into the gossip protocol.
 */
public abstract class RService {

    private ServiceManager manager;

    /**
     * Handle an incoming request from a remote peer.
     *
     * @param request  the full request bytes from the remote peer
     * @param response write response bytes here; call close() when done
     */
    public abstract void serve(byte[] request, ServiceResponse response);

    /**
     * Dispatches a request to a remote peer offering this service.
     * The framework picks a random healthy node that offers this service.
     *
     * @param request       request bytes sent to the remote peer
     * @param onStateChange called when request events occur:
     *                      {@link RequestEvent.Processing} – request sent,
     *                      {@link RequestEvent.StreamData} – a chunk of response data arrived,
     *                      {@link RequestEvent.Succeeded} – completed successfully,
     *                      {@link RequestEvent.Failed} – failed (includes reason)
     */
    public void dispatch(byte[] request, OnStateChange onStateChange) {
        dispatch(request, onStateChange, null);
    }

    /**
     * Dispatches a request to a remote peer offering this service,
     * filtered by application state criteria.
     *
     * <p>The framework selects a random healthy node that:
     * <ol>
     *   <li>is alive and offers this service</li>
     *   <li>satisfies the given {@code peerFilter} (if non-null)</li>
     * </ol>
     *
     * <p>The predicate receives a read-only {@code Map<String, String>} of the
     * candidate peer's application state (e.g. {@code {"SHARED_FILES": "a.txt,b.bin", ...}}).
     *
     * @param request       request bytes sent to the remote peer
     * @param onStateChange called when request events occur
     * @param peerFilter    optional predicate over the peer's app state;
     *                      {@code null} means no extra filtering
     */
    public void dispatch(byte[] request, OnStateChange onStateChange,
                         Predicate<Map<String, String>> peerFilter) {
        if (manager == null) {
            throw new IllegalStateException("Service not registered. Call rumor.register() first.");
        }
        manager.sendRequest(serviceName(), request, onStateChange, peerFilter);
    }

    /**
     * Invokes this service locally with the same event-driven interface as
     * {@link #dispatch(byte[], OnStateChange)}.
     *
     * <p>The {@link #serve(byte[], ServiceResponse)} method is called on the
     * caller's thread. If that is undesirable, wrap this call in your own executor.
     *
     * @param request       request bytes passed directly to {@link #serve}
     * @param onStateChange called when request events occur (same contract as dispatch)
     */
    public void request(byte[] request, OnStateChange onStateChange) {
        onStateChange.accept(new RequestEvent.Processing());
        LocalServiceResponse response = new LocalServiceResponse(onStateChange);
        try {
            serve(request, response);
            response.close();
        } catch (Exception e) {
            onStateChange.accept(new RequestEvent.Failed(e.getMessage()));
        }
    }

    /**
     * Returns a read-only view of the cluster's application state.
     * Use this to query what state other peers have published.
     *
     * @throws IllegalStateException if the service has not been registered
     */
    protected ClusterView clusterView() {
        if (manager == null) {
            throw new IllegalStateException("Service not registered. Call rumor.register() first.");
        }
        return manager;
    }

    /**
     * Returns the service name, derived from the concrete class name.
     */
    public final String serviceName() {
        return this.getClass().getSimpleName();
    }

    void setManager(ServiceManager manager) {
        this.manager = manager;
    }
}
