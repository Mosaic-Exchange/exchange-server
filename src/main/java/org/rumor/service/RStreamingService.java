package org.rumor.service;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Base class for streaming services in the Rumor framework.
 *
 * <p>Extend this class and implement {@link #serve(byte[], ServiceResponse)} to handle
 * incoming requests that require streaming a potentially large amount of data.
 * Use {@link #dispatch(byte[], OnStateChange)} to invoke this service on a remote
 * peer that offers it.
 *
 * <p><b>Key differences from {@link RService}:</b>
 * <ul>
 *   <li>{@code serve()} runs on a <em>dedicated thread</em>, not the Netty I/O
 *       thread — blocking operations are safe.</li>
 *   <li>The {@link ServiceResponse#write(byte[])} method may be called
 *       multiple times to stream data in chunks.</li>
 *   <li>Only one streaming service may be active on a node at a time.</li>
 *   <li>Backpressure is enforced: {@code write()} blocks when the channel
 *       is not writable.</li>
 * </ul>
 *
 * <p><b>Wire protocol (transparent to the service):</b>
 * <ol>
 *   <li>Client sends {@code SERVICE_REQUEST}</li>
 *   <li>Server responds with {@code SERVICE_INIT_STREAM}</li>
 *   <li>Client acknowledges with {@code SERVICE_STREAM_START}</li>
 *   <li>Server sends {@code SERVICE_STREAM_DATA} chunks until done</li>
 *   <li>Server sends {@code SERVICE_STREAM_END} (success) or
 *       {@code SERVICE_STREAM_ERROR} (failure)</li>
 * </ol>
 *
 * <p>The service is identified by the simple class name of the subclass.
 *
 * <p>Optionally implement {@link StatePublisher} to automatically publish
 * application state into the gossip protocol.
 */
public abstract class RStreamingService {

    private ServiceManager manager;

    /**
     * Handle an incoming request from a remote peer by streaming a response.
     *
     * <p>This method is called on a dedicated thread (not the Netty I/O thread),
     * so blocking operations are safe. Call {@link ServiceResponse#write(byte[])}
     * as many times as needed to stream data. The framework calls
     * {@link ServiceResponse#close()} when this method returns normally,
     * and {@link ServiceResponse#fail(byte[])} if it throws.
     *
     * @param request  the full request bytes from the remote peer
     * @param response write response bytes here; may be called multiple times
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
        LocalServiceResponse response = new LocalServiceResponse(onStateChange, false);
        try {
            serve(request, response);
            response.close();
        } catch (Exception e) {
            response.fail(e.getMessage().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    /**
     * Returns a read-only view of the cluster's application state.
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
