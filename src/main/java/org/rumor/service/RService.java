package org.rumor.service;

import java.util.Map;
import java.util.function.Predicate;

/**
 * Base class for services in the Rumor framework.
 *
 * <p>Extend this class and implement {@link #serve(byte[], ServiceResponse)} to handle
 * incoming requests from remote peers. Use {@link #dispatch(byte[], OnStateChange)}
 * to invoke this service on a remote peer that offers it.
 *
 * <p><b>Default (request/response) mode:</b>
 * <ul>
 *   <li>{@code serve()} runs on the Netty I/O thread — do not block.</li>
 *   <li>{@link ServiceResponse#write(byte[])} may be called at most once.</li>
 * </ul>
 *
 * <p><b>Streaming mode</b> (annotate the subclass with {@link Streamable}):
 * <ul>
 *   <li>{@code serve()} runs on a dedicated thread — blocking is safe.</li>
 *   <li>{@link ServiceResponse#write(byte[])} may be called multiple times.</li>
 *   <li>Backpressure is enforced: {@code write()} blocks when the channel
 *       is not writable.</li>
 *   <li>Only one streaming service may be active on a node at a time.</li>
 * </ul>
 *
 * <p>The service is identified by the simple class name of the subclass.
 *
 * <p>Optionally annotate the subclass with {@link MaintainState} and mark
 * methods with {@link StateKey} to automatically publish application state
 * into the gossip protocol.
 */
public abstract class RService {

    private ServiceManager manager;

    /**
     * Handle an incoming request from a remote peer.
     *
     * <p>For default (non-streaming) services, {@link ServiceResponse#write(byte[])}
     * may be called at most once. For {@link Streamable} services, it may be called
     * multiple times to stream data.
     *
     * @param request  the full request bytes from the remote peer
     * @param response write response bytes here; call fail() on error
     */
    public abstract void serve(byte[] request, ServiceResponse response);

    /**
     * Dispatches a request to a remote peer offering this service.
     * The framework picks a random healthy node that offers this service.
     *
     * @param request       request bytes sent to the remote peer
     * @param onStateChange called when request events occur:
     *                      {@link RequestEvent.Processing} – request sent,
     *                      {@link RequestEvent.StreamData} – response data arrived,
     *                      {@link RequestEvent.Succeeded} – completed successfully,
     *                      {@link RequestEvent.Failed} – failed (includes reason)
     * @return a handle that can be used to cancel the request
     */
    public ServiceHandle dispatch(byte[] request, OnStateChange onStateChange) {
        return dispatch(request, onStateChange, null);
    }

    /**
     * Dispatches a request to a remote peer offering this service,
     * filtered by application state criteria.
     *
     * @param request       request bytes sent to the remote peer
     * @param onStateChange called when request events occur
     * @param peerFilter    optional predicate over the peer's app state;
     *                      {@code null} means no extra filtering
     * @return a handle that can be used to cancel the request
     */
    public ServiceHandle dispatch(byte[] request, OnStateChange onStateChange,
                         Predicate<Map<String, String>> peerFilter) {
        if (manager == null) {
            throw new IllegalStateException("Service not registered. Call rumor.register() first.");
        }
        ServiceHandle handle = new ServiceHandle();
        manager.sendRequest(serviceName(), request, onStateChange, peerFilter, handle);
        return handle;
    }

    /**
     * Invokes this service locally with the same event-driven interface as
     * {@link #dispatch(byte[], OnStateChange)}.
     *
     * <p>The {@link #serve(byte[], ServiceResponse)} method is called on the
     * caller's thread. If that is undesirable for a {@link Streamable} service,
     * wrap this call in your own executor.
     *
     * @param request       request bytes passed directly to {@link #serve}
     * @param onStateChange called when request events occur (same contract as dispatch)
     * @return a handle that can be used to cancel the request
     */
    public ServiceHandle request(byte[] request, OnStateChange onStateChange) {
        ServiceHandle handle = new ServiceHandle();
        onStateChange.accept(new RequestEvent.Processing());
        boolean singleWrite = !this.getClass().isAnnotationPresent(Streamable.class);
        LocalServiceResponse response = new LocalServiceResponse(onStateChange, singleWrite, handle);
        try {
            serve(request, response);
            response.close();
        } catch (Exception e) {
            response.fail(e.getMessage().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return handle;
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

    /**
     * Returns {@code true} if this service is annotated with {@link Streamable}.
     */
    public final boolean isStreamable() {
        return this.getClass().isAnnotationPresent(Streamable.class);
    }

    /**
     * Returns the gossip-qualified form of a state key:
     * {@code ServiceClassName.rawKey}.
     *
     * <p>Use this when querying {@link ClusterView#stateForKey(String)} or
     * when building a peer filter, so the key matches what the framework
     * actually publishes.
     *
     * @param rawKey the unqualified key (same value passed to {@link StateKey})
     * @return the qualified key, e.g. {@code "FileDownloadService.SHARED_FILES"}
     */
    protected final String qualifiedKey(String rawKey) {
        return serviceName() + "." + rawKey;
    }

    void setManager(ServiceManager manager) {
        this.manager = manager;
    }
}
